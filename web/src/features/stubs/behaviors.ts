/**
 * The `_behaviors` and fault projection for one response (issue #249), consumed by `responses.ts`.
 *
 * Split out of `responses.ts` rather than made a third top-level projection, because unlike
 * `predicates` and `responses` this is not a subtree of the stub — it hangs off an individual
 * response. `responses.ts` owns the composition; this file owns the two shapes.
 *
 * **Everything here is parse-fidelity work.** The engine accepts each of these in more than one
 * spelling and re-emits whichever it was given, because its SDK parse-fidelity gate requires a
 * `GET /imposters` to round-trip byte for byte. The form has to do the same or it produces a diff
 * on every export (#251) of a stub it merely opened. Three separate places this bites:
 *
 * - **`_behaviors` has three accepted spellings**: `_behaviors` as an object, `behaviors` as an
 *   object, and `behaviors` as an ARRAY of single-key objects. All three mean the same thing.
 * - **Key order inside them is preserved**, so the array spelling in particular re-emits unchanged
 *   rather than being reordered into whatever order this module happens to write fields in.
 * - **`_rift.fault.tcp` is a string OR an object** (`RiftTcpFault`), and the engine's own
 *   deserializer is hand-written specifically to keep the two apart and re-emit each as it came.
 *   The object form exists solely to carry `probability`, so `{type: X}` without one is not a
 *   second spelling of the bare form — it is an error, and the engine says so in a message worth
 *   showing verbatim.
 *
 * Pure and free of React, like its siblings.
 */

/** Which of the three accepted spellings the source used. */
export type BehaviorSpelling = "_behaviors" | "behaviorsObject" | "behaviorsArray";

/** The behaviour keys this form models. Anything else sends the response to raw-only. */
export const MODELLED_BEHAVIORS = ["wait", "repeat"] as const;
export type ModelledBehavior = (typeof MODELLED_BEHAVIORS)[number];

/**
 * The behaviours the engine supports that this form deliberately does NOT edit.
 *
 * `decorate` and `shellTransform` run JavaScript and a shell command respectively; building form
 * editors for those is a separate decision with its own security surface. `copy` and `lookup` are
 * substantial sub-languages of their own. All four are recognised so the card can say the response
 * runs one, and all four force raw-only editing — silently hiding them is exactly what this
 * codebase's raw-only rule exists to prevent.
 */
export const FOREIGN_BEHAVIORS = ["copy", "lookup", "decorate", "shellTransform"] as const;

export type WaitModel =
  | { kind: "none" }
  /** `wait: 50` — a fixed pause in milliseconds. */
  | { kind: "fixed"; ms: number }
  /** `wait: {min, max}` — a pause drawn from a range. */
  | { kind: "range"; min: number; max: number };

export type BehaviorModel = {
  spelling: BehaviorSpelling;
  /**
   * The modelled keys in the order the source listed them.
   *
   * Carried so the array spelling re-emits in its original order instead of this module's. An
   * operator reviewing an exported mock beside a file should see no reordering they did not ask for.
   */
  order: ModelledBehavior[];
  wait: WaitModel;
  /** `null` means the key is ABSENT, not that the response repeats zero times. */
  repeat: number | null;
};

/** The four TCP fault kinds, in the engine's canonical spelling — what this form writes. */
export const FAULT_KINDS = [
  "CONNECTION_RESET_BY_PEER",
  "EMPTY_RESPONSE",
  "RANDOM_DATA_THEN_CLOSE",
  "MALFORMED_RESPONSE_CHUNK",
] as const;
export type FaultKind = (typeof FAULT_KINDS)[number];

/**
 * The engine's short aliases, accepted on the way in and never written back.
 *
 * `TcpFaultKind::parse` takes both; the canonical `SCREAMING_CASE` names are the WireMock-compatible
 * ones, so those are what the picker writes. An alias read from a document is still shown correctly
 * — it is carried verbatim, not normalised, since rewriting it would be a diff the operator did not
 * ask for.
 */
const FAULT_ALIASES: Record<string, FaultKind> = {
  reset: "CONNECTION_RESET_BY_PEER",
  empty: "EMPTY_RESPONSE",
  garbage: "RANDOM_DATA_THEN_CLOSE",
  random: "RANDOM_DATA_THEN_CLOSE",
  malformed: "MALFORMED_RESPONSE_CHUNK",
};

/**
 * A fault, in the exact form the document spelled it.
 *
 * Three forms rather than one shape with optional fields, so an unrepresentable combination cannot
 * be constructed: only `_rift.fault.tcp`'s object form can carry a probability, so a probability on
 * the top-level `fault` key is not a state this type admits.
 */
export type FaultModel =
  /** Top-level `fault: "KIND"` — `StubResponse::Fault`, which REPLACES the response. */
  | { form: "responseKey"; kind: string }
  /** `_rift.fault.tcp: "KIND"` — the bare Rift form; always fires. */
  | { form: "riftString"; kind: string }
  /** `_rift.fault.tcp: {probability, type}` — fires probabilistically. */
  | { form: "riftObject"; kind: string; probability: number };

/** Is this a fault kind the engine recognises, in either spelling? */
export function isKnownFaultKind(value: string): boolean {
  return (FAULT_KINDS as readonly string[]).includes(value) || value in FAULT_ALIASES;
}

/** The canonical kind a (possibly aliased) spelling denotes, for display and for the picker. */
export function canonicalFaultKind(value: string): FaultKind | null {
  if ((FAULT_KINDS as readonly string[]).includes(value)) return value as FaultKind;
  return FAULT_ALIASES[value] ?? null;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isModelledBehavior(key: string): key is ModelledBehavior {
  return (MODELLED_BEHAVIORS as readonly string[]).includes(key);
}

// ---------------------------------------------------------------------------------------------
// behaviors
// ---------------------------------------------------------------------------------------------

type ParseResult<T> = { ok: true; value: T } | { ok: false; issues: string[] };

/** One behaviour key's JSON, e.g. `{wait: 50}` or `{repeat: 2}`. */
function renderBehaviorEntry(key: ModelledBehavior, model: BehaviorModel): Record<string, unknown> {
  if (key === "repeat") return model.repeat === null ? {} : { repeat: model.repeat };
  if (model.wait.kind === "fixed") return { wait: model.wait.ms };
  if (model.wait.kind === "range") return { wait: { min: model.wait.min, max: model.wait.max } };
  return {};
}

/**
 * Render a behaviour model back to the response-level key it came from.
 *
 * Returns the key name and its value, or `null` when the model carries nothing — an empty
 * `_behaviors: {}` is a key the form would otherwise invent out of a response that had none.
 */
export function renderBehaviors(
  model: BehaviorModel,
): { key: "_behaviors" | "behaviors"; value: unknown } | null {
  const entries = model.order.map((key) => renderBehaviorEntry(key, model)).filter((entry) => Object.keys(entry).length > 0);
  if (entries.length === 0) return null;

  if (model.spelling === "behaviorsArray") return { key: "behaviors", value: entries };
  const merged = Object.assign({}, ...entries) as Record<string, unknown>;
  return { key: model.spelling === "_behaviors" ? "_behaviors" : "behaviors", value: merged };
}

/** Parse a single `wait` value: a number, a `{min,max}` range, or something this form refuses. */
function parseWait(raw: unknown, path: string): ParseResult<WaitModel> {
  if (typeof raw === "number") return { ok: true, value: { kind: "fixed", ms: raw } };
  if (isPlainObject(raw)) {
    const keys = Object.keys(raw);
    const extra = keys.filter((key) => key !== "min" && key !== "max");
    if (extra.length > 0) return { ok: false, issues: extra.map((key) => `${path}.${key}`) };
    const { min, max } = raw;
    if (typeof min !== "number" || typeof max !== "number") return { ok: false, issues: [path] };
    return { ok: true, value: { kind: "range", min, max } };
  }
  // A string `wait` is a JS function the engine evaluates. Recognised — the card says so — but not
  // something this form will pretend to edit.
  return { ok: false, issues: [path] };
}

/**
 * Parse whichever behaviours key a response carries into the model, naming anything it cannot hold.
 *
 * `raw` is the key's value; `spelling` says which of the three forms it was found as. The array
 * spelling is flattened here: each element must be a single-key object, which is the shape the
 * engine's own examples use.
 */
export function parseBehaviors(
  raw: unknown,
  spelling: BehaviorSpelling,
  path: string,
): ParseResult<BehaviorModel> {
  const flat: [string, unknown][] = [];
  if (spelling === "behaviorsArray") {
    if (!Array.isArray(raw)) return { ok: false, issues: [path] };
    for (const [index, element] of raw.entries()) {
      if (!isPlainObject(element)) return { ok: false, issues: [`${path}[${index}]`] };
      const entries = Object.entries(element);
      // More than one key in an array element is a shape the engine tolerates but that this form
      // cannot re-emit without deciding how to regroup it — so it is refused, not guessed at.
      if (entries.length !== 1) return { ok: false, issues: [`${path}[${index}]`] };
      for (const entry of entries) flat.push([entry[0], entry[1]]);
    }
  } else {
    if (!isPlainObject(raw)) return { ok: false, issues: [path] };
    flat.push(...Object.entries(raw));
  }

  if (flat.length === 0) {
    /*
     * An empty `_behaviors: {}` / `behaviors: {}` / `behaviors: []`. `BehaviorModel` has no way to
     * record "the source had a container and it was empty", so it would render back to nothing and
     * the key would vanish unnamed — the diff-on-export this module exists to prevent. Refusing is
     * safe: the engine never EMITS an empty container (`behaviors_to_array` maps empty to `None`,
     * and `StubResponseOut` skips a `None`), so only hand-written JSON can carry one, and for that
     * the cost is a trip through the raw editor.
     */
    return { ok: false, issues: [path] };
  }

  const order: ModelledBehavior[] = [];
  const issues: string[] = [];
  let wait: WaitModel = { kind: "none" };
  let repeat: number | null = null;

  for (const [key, value] of flat) {
    if (!isModelledBehavior(key)) {
      // Includes every FOREIGN_BEHAVIORS key and anything the engine grows later.
      issues.push(`${path}.${key}`);
      continue;
    }
    if (order.includes(key)) {
      // The same behaviour twice — only reachable through the array spelling, and there is no
      // honest single-valued model of it.
      issues.push(`${path}.${key}`);
      continue;
    }
    if (key === "repeat") {
      if (typeof value !== "number") {
        issues.push(`${path}.repeat`);
        continue;
      }
      repeat = value;
      order.push(key);
      continue;
    }
    const parsed = parseWait(value, `${path}.wait`);
    if (!parsed.ok) {
      issues.push(...parsed.issues);
      continue;
    }
    wait = parsed.value;
    order.push(key);
  }

  if (issues.length > 0) return { ok: false, issues };
  return { ok: true, value: { spelling, order, wait, repeat } };
}

// ---------------------------------------------------------------------------------------------
// faults
// ---------------------------------------------------------------------------------------------

/** Parse a top-level `fault: "KIND"` — the `StubResponse::Fault` variant. */
export function parseResponseFault(raw: unknown, path: string): ParseResult<FaultModel> {
  if (typeof raw !== "string" || !isKnownFaultKind(raw)) return { ok: false, issues: [path] };
  return { ok: true, value: { form: "responseKey", kind: raw } };
}

/**
 * Parse `_rift.fault.tcp` — a bare kind string, or the object form carrying a probability.
 *
 * Mirrors `RiftTcpFault`'s hand-written deserializer, including its refusal of an object with no
 * `probability`: that object exists solely to carry one, so `{type: X}` alone would be a second
 * spelling of the bare string and would break the round-trip the engine's own gate enforces.
 */
export function parseRiftTcpFault(raw: unknown, path: string): ParseResult<FaultModel> {
  if (typeof raw === "string") {
    if (!isKnownFaultKind(raw)) return { ok: false, issues: [path] };
    return { ok: true, value: { form: "riftString", kind: raw } };
  }
  if (!isPlainObject(raw)) return { ok: false, issues: [path] };

  const extra = Object.keys(raw).filter((key) => key !== "probability" && key !== "type");
  if (extra.length > 0) return { ok: false, issues: extra.map((key) => `${path}.${key}`) };

  const { probability, type } = raw;
  if (typeof probability !== "number" || probability < 0 || probability > 1) {
    return { ok: false, issues: [`${path}.probability`] };
  }
  if (typeof type !== "string" || !isKnownFaultKind(type)) return { ok: false, issues: [`${path}.type`] };
  return { ok: true, value: { form: "riftObject", kind: type, probability } };
}

/** The JSON for a fault, at the key its form belongs to. */
export function renderFault(fault: FaultModel): { key: "fault" | "_rift"; value: unknown } {
  if (fault.form === "responseKey") return { key: "fault", value: fault.kind };
  if (fault.form === "riftString") return { key: "_rift", value: { fault: { tcp: fault.kind } } };
  return {
    key: "_rift",
    value: { fault: { tcp: { probability: fault.probability, type: fault.kind } } },
  };
}
