import type { components } from "../../api/schema.ts";

/**
 * Why a recorded request was served by the stub it was — or by nothing (issue #208).
 *
 * The request log exists to answer "why did this 404", and until the engine started recording a
 * match outcome per journal entry the screen could not: the per-stub detail lived only on the
 * `X-Rift-Debug` response path, which is a *different* request, judged against whatever the stubs
 * have since become. This module turns the recorded outcome into sentences; `RequestLog.tsx` only
 * places them.
 *
 * Two distinctions carry the whole design, and both are distinctions the screen must not collapse:
 *
 *  - **absent is not "did not match".** The schema says so in bold. An entry from an engine that
 *    predates the field, an `X-Rift-Debug` request and a matcher error all arrive with no outcome,
 *    and rendering any of them as a miss would tell an operator their stub was rejected when
 *    nothing ever judged it.
 *  - **unreadable is not absent.** A shape this console cannot parse means the node answered with
 *    something wrong; saying "nothing was recorded" would file a broken engine under routine.
 */
export type MatchOutcome = components["schemas"]["MatchOutcome"];

/** One candidate the matcher visited and did not serve, as the two strings the screen renders. */
export type TriedView = { label: string; why: string };

/**
 * The outcome as a renderable value — no React, so the sentences are testable without a DOM.
 *
 * `tried` and `omitted` ride on both verdicts because a hit has them too: `tried` on a match holds
 * the candidates visited *before* the winner, which is how an operator sees that the stub they
 * expected to serve the request was passed over, and why.
 */
export type OutcomeView =
  | { kind: "none" }
  | { kind: "unreadable" }
  | { kind: "matched"; label: string; tried: TriedView[]; omitted: number }
  | { kind: "unmatched"; tried: TriedView[]; omitted: number };

/**
 * The stub an outcome names but does not identify.
 *
 * `stubIndex` and `stubId` are both optional on the wire, so `{"matched":true}` is contract-legal.
 * It is still a hit, and saying which stub won is not something this console may invent.
 */
const UNNAMED_STUB = "a stub this outcome does not name";

/**
 * Read one recorded outcome.
 *
 * Typed from the contract, validated anyway. `apiGet` asserts response shapes rather than checking
 * them (`source.ts` documents why), this is attacker-adjacent data recorded from whatever called
 * the mock, and a node on another engine version can answer with a shape that is not this one. A
 * throw here would unmount the screen an operator opened to diagnose something else — so every
 * unexpected shape lands on `unreadable`, which is a sentence, not a crash.
 */
export function describeOutcome(outcome: MatchOutcome | undefined): OutcomeView {
  const raw: unknown = outcome;
  // `null` is folded into absence rather than reported as broken. The engine omits the field
  // (`skip_serializing_if`) so a null never comes from it, but a null carries the same meaning any
  // serializer that emits one intends, and crying "unreadable" at it teaches operators to ignore
  // the word on the entries where it means something.
  if (raw === undefined || raw === null) return { kind: "none" };
  if (typeof raw !== "object" || Array.isArray(raw)) return { kind: "unreadable" };

  const record = raw as Record<string, unknown>;
  if (typeof record["matched"] !== "boolean") return { kind: "unreadable" };

  const omitted = readCount(record["triedOmitted"]);
  const tried = readTried(record["tried"]);
  if (omitted === null || tried === null) return { kind: "unreadable" };

  if (!record["matched"]) return { kind: "unmatched", tried, omitted };

  const label = stubLabel(record["stubId"], record["stubIndex"]);
  if (label === null) return { kind: "unreadable" };
  return { kind: "matched", label, tried, omitted };
}

/** `null` when a declared field is present with a shape the contract does not allow. */
function stubLabel(id: unknown, index: unknown): string | null {
  if (id !== undefined && typeof id !== "string") return null;
  if (index !== undefined && !isPosition(index)) return null;
  // The id wins when both are present: an index is a position, and a position shifts under any
  // concurrent edit that inserts or removes a stub, so the id is the identity worth showing.
  if (typeof id === "string") return `stub "${id}"`;
  if (typeof index === "number") return `stub #${index}`;
  return UNNAMED_STUB;
}

/** `null` when the list itself, or any candidate in it, is not the shape the contract declares. */
function readTried(value: unknown): TriedView[] | null {
  // Absent, not empty: the engine drops an empty `tried` rather than serializing `[]`.
  if (value === undefined) return [];
  if (!Array.isArray(value)) return null;

  const views: TriedView[] = [];
  for (const entry of value as unknown[]) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) return null;
    const candidate = entry as Record<string, unknown>;
    // `stubIndex` is required on `TriedStub`, so a candidate without one is a broken shape rather
    // than an unnamed stub.
    if (!isPosition(candidate["stubIndex"])) return null;
    const label = stubLabel(candidate["stubId"], candidate["stubIndex"]);
    const why = readWhy(candidate["why"]);
    if (label === null || why === null) return null;
    views.push({ label, why });
  }
  return views;
}

/** `null` when the reason is missing or not a reason. */
function readWhy(value: unknown): string | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const why = value as Record<string, unknown>;
  const reason = why["reason"];
  if (typeof reason !== "string" || reason.length === 0) return null;

  switch (reason) {
    case "skippedSpace":
      return "space did not match";
    case "skippedScenarioState":
      return "scenario state did not match";
    case "failedPredicate": {
      const at = why["predicateIndex"];
      // The position in the stub's own `predicates` array, as the engine numbers it — the console
      // does not renumber it, because an operator uses it to index the stub config it is beside.
      if (isPosition(at)) return `predicate ${at} did not match`;
      // `predicateIndex` is documented as present for this reason, but a miss that names no
      // position is still a miss: reporting the candidate is worth more than the position.
      if (at === undefined) return "a predicate did not match";
      return null;
    }
    default:
      // Forward compatibility. A newer engine can add a gate, and dropping the candidate would
      // under-report what was tried — the one claim this panel makes. Shown as the engine spelled
      // it, and rendered as text like every other value on this screen.
      return reason;
  }
}

/** Absent means zero — the engine omits a zero count rather than serializing it. */
function readCount(value: unknown): number | null {
  if (value === undefined) return 0;
  return isPosition(value) ? value : null;
}

function isPosition(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}
