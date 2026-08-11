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
  /**
   * The node holding this flow's state (#359), or `null` when the fleet did not say.
   *
   * A **decimal string**, not a number — issue #374 corrected this alongside publishing the same
   * field on the listing, so one field would not carry two types on adjacent routes. A raft id is a
   * `u64`; JavaScript reads a JSON number back as an IEEE-754 double, so every id above 2^53-1 would
   * round silently and name a neighbouring node. Render it as-is, never through `Number(...)`.
   *
   * `null` rather than a sentinel because the contract makes this field genuinely optional: the
   * server omits it when no membership is applied or the imposter's context scope could not be
   * read, which is a different situation from `numberOfRequests`, whose absence is a contract
   * violation. Absent means "not known" and must render as such — a guessed owner sends an
   * operator to the wrong node.
   */
  owner: string | null;
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
      // Optional by contract, so absence is ordinary rather than a violation — and stays absent. A
      // decimal string (#374); a number here would mean an older node or a body this console does
      // not recognise, so it is treated the same as an absent owner rather than coerced.
      owner: typeof payload.owner === "string" ? payload.owner : null,
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

/** One row of `listSpaces` (#374): a flow id this imposter holds live flow-KV entries under. */
export type SpaceListEntry = {
  space: string;
  entryCount: number;
  /** A decimal string, for the same reason as `Space.owner` — never `null` here: a row is only
   * ever produced by the node that actually owns it, so a listed row always names one. */
  owner: string;
};

/**
 * The `flowState.durability` knob this imposter resolved, or `null` when it could not be read.
 *
 * `null` is not a default. The server omits the field entirely rather than guessing, so a caller
 * that substituted `"async"` (the compiled-in default) for an absent knob would assert a config the
 * node never confirmed.
 */
export type SpaceDurability = { value: "none" | "async" | "sync"; source: "default" | "set" };

/**
 * Why a listing was refused outright rather than merely incomplete (#374).
 *
 * Both reasons imply `spaces: []` and `partial: true`, but they are not one fact wearing two
 * names — a caller that only checked `partial` could not tell "some node was slow, try again"
 * from "this scope cannot ever be listed", and the screen owes an operator a different sentence
 * for each:
 *
 * - `"fleet-scope"`: the imposter's `contextScope` is `"fleet"`, whose `f:` namespace carries no
 *   tenant component — listing it would either scan nothing (wrong prefix) or leak every other
 *   tenant's fleet-scoped flows (right prefix, no filter). A policy refusal, not a failure: it
 *   will not improve on a retry.
 * - `"scope-unresolved"`: the imposter's own flow-state config could not be read or parsed, so
 *   which scope it holds is unknown and guessing would enumerate the wrong namespace. Transient —
 *   a retry may succeed once the node catches up or the config is fixed.
 */
export type SpaceListUnavailable = "fleet-scope" | "scope-unresolved";

/**
 * `listSpaces`'s body: every space this imposter holds, fleet-wide.
 *
 * `partial` is the field this type exists to keep next to `spaces` rather than beside it as a
 * sibling a caller could forget to check: `spaces: []` with `partial: false` is a real answer
 * ("this imposter holds none"), and `spaces: []` with `partial: true` is "the fleet could not be
 * asked in time" — the same shape as an empty read, and not the same fact.
 *
 * `unavailable`, present, narrows that "could not be asked" into "will not be asked, and here is
 * why" (#374) — see `SpaceListUnavailable`. `null` when the node did attempt the listing, whether
 * or not it fully succeeded.
 */
export type SpaceList = {
  durability: SpaceDurability | null;
  spaces: SpaceListEntry[];
  partial: boolean;
  unavailable: SpaceListUnavailable | null;
};

/** Same discipline as `SpaceState`: a listing that could not be read is not an empty listing. */
export type SpaceListState = { kind: "list"; list: SpaceList } | { kind: "unknown"; reason: string };

/** The node's `listSpaces` body. Absent or malformed fields read as unknown, never as an empty list. */
export function readSpaceList(body: unknown): SpaceListState {
  const payload = body as
    | { durability?: unknown; spaces?: unknown; partial?: unknown; unavailable?: unknown }
    | null;
  if (payload === null || typeof payload !== "object") {
    return { kind: "unknown", reason: "this node answered with a body that is not a space list" };
  }
  if (!Array.isArray(payload.spaces)) {
    return { kind: "unknown", reason: "this node answered with a body carrying no space list" };
  }
  if (typeof payload.partial !== "boolean") {
    return {
      kind: "unknown",
      reason: "this node answered without saying whether the listing is complete",
    };
  }
  const spaces: SpaceListEntry[] = [];
  for (const row of payload.spaces) {
    const entry = row as Partial<SpaceListEntry> | null;
    if (
      entry === null ||
      typeof entry !== "object" ||
      typeof entry.space !== "string" ||
      typeof entry.entryCount !== "number" ||
      typeof entry.owner !== "string"
    ) {
      return { kind: "unknown", reason: "this node answered with a space row missing a required field" };
    }
    spaces.push({ space: entry.space, entryCount: entry.entryCount, owner: entry.owner });
  }
  const knob = payload.durability as Partial<SpaceDurability> | null | undefined;
  const durability: SpaceDurability | null =
    knob !== null &&
    knob !== undefined &&
    typeof knob === "object" &&
    (knob.value === "none" || knob.value === "async" || knob.value === "sync") &&
    (knob.source === "default" || knob.source === "set")
      ? { value: knob.value, source: knob.source }
      : null;
  // Strict on the enum, same as `durability`'s `value`/`source` above: a stray third string here
  // is not this console's business to guess at, and reading it as `null` ("no reason given") would
  // let a future server-side reason silently fall back to the generic partial banner instead of
  // surfacing as the unrecognised body it actually is.
  if (
    payload.unavailable !== undefined &&
    payload.unavailable !== "fleet-scope" &&
    payload.unavailable !== "scope-unresolved"
  ) {
    return {
      kind: "unknown",
      reason: "this node answered with an unrecognised reason the listing was unavailable",
    };
  }
  const unavailable: SpaceListUnavailable | null = payload.unavailable ?? null;
  return { kind: "list", list: { durability, spaces, partial: payload.partial, unavailable } };
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
