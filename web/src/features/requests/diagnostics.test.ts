import { describe, expect, it } from "vitest";

import type { MatchOutcome } from "./diagnostics.ts";
import { describeOutcome } from "./diagnostics.ts";

/**
 * The wire, as the wire actually is.
 *
 * `apiGet` asserts the response shape rather than validating it, so a node running a different
 * engine — or an entry a proxy rewrote — can put anything here. A test that could only express
 * well-typed outcomes would leave the defensive half of `describeOutcome` unexercised.
 */
function wire(value: unknown): MatchOutcome | undefined {
  return value as MatchOutcome | undefined;
}

describe("describeOutcome on a request that matched", () => {
  it("names the winning stub by its id when the outcome carries one", () => {
    expect(describeOutcome(wire({ matched: true, stubIndex: 2, stubId: "payments" }))).toEqual({
      kind: "matched",
      label: 'stub "payments"',
      tried: [],
      omitted: 0,
    });
  });

  it("names the winning stub by its position when it declares no id", () => {
    const view = describeOutcome(wire({ matched: true, stubIndex: 2 }));
    expect(view).toEqual({ kind: "matched", label: "stub #2", tried: [], omitted: 0 });
  });

  // `tried` on a hit holds the candidates visited *before* the winner, which is how an operator
  // sees that a stub they expected to serve the request was passed over and why.
  it("lists the candidates that were passed over before the winner", () => {
    const view = describeOutcome(
      wire({
        matched: true,
        stubIndex: 1,
        tried: [{ stubIndex: 0, stubId: "users", why: { reason: "failedPredicate", predicateIndex: 1 } }],
      }),
    );
    expect(view).toEqual({
      kind: "matched",
      label: "stub #1",
      tried: [{ label: 'stub "users"', why: "predicate 1 did not match" }],
      omitted: 0,
    });
  });
});

describe("describeOutcome on a request that matched nothing", () => {
  it("says which gate rejected each candidate the matcher visited", () => {
    const view = describeOutcome(
      wire({
        matched: false,
        tried: [
          { stubIndex: 0, stubId: "scoped", why: { reason: "skippedSpace" } },
          { stubIndex: 1, why: { reason: "skippedScenarioState" } },
          { stubIndex: 2, stubId: "users", why: { reason: "failedPredicate", predicateIndex: 0 } },
        ],
      }),
    );
    expect(view).toEqual({
      kind: "unmatched",
      tried: [
        { label: 'stub "scoped"', why: "space did not match" },
        { label: "stub #1", why: "scenario state did not match" },
        { label: 'stub "users"', why: "predicate 0 did not match" },
      ],
      omitted: 0,
    });
  });

  // Forward compatibility, and the reason this is a presenter rather than a lookup table: a newer
  // engine can add a gate. Dropping the candidate would under-report what was tried — the one claim
  // this panel makes — so the unrecognised reason is shown exactly as the engine spelled it.
  it("shows an unrecognised reason as the engine spelled it rather than dropping the candidate", () => {
    const view = describeOutcome(
      wire({ matched: false, tried: [{ stubIndex: 0, why: { reason: "skippedTenant" } }] }),
    );
    expect(view).toEqual({
      kind: "unmatched",
      tried: [{ label: "stub #0", why: "skippedTenant" }],
      omitted: 0,
    });
  });

  // The engine caps `tried` at 25 and counts the rest. Losing the count would make "these are the
  // stubs that were tried" false with nothing on screen to say so.
  it("carries the count of candidates the engine capped out of the list", () => {
    const view = describeOutcome(
      wire({
        matched: false,
        tried: [{ stubIndex: 0, why: { reason: "skippedSpace" } }],
        triedOmitted: 40,
      }),
    );
    expect(view).toEqual({
      kind: "unmatched",
      tried: [{ label: "stub #0", why: "space did not match" }],
      omitted: 40,
    });
  });

  // `predicates` are AND-ed and the scan short-circuits, so the engine names the *first* rejecting
  // predicate. An outcome that names none is still a miss worth showing.
  it("still reports a failed predicate whose position the outcome does not name", () => {
    const view = describeOutcome(
      wire({ matched: false, tried: [{ stubIndex: 0, why: { reason: "failedPredicate" } }] }),
    );
    expect(view).toEqual({
      kind: "unmatched",
      tried: [{ label: "stub #0", why: "a predicate did not match" }],
      omitted: 0,
    });
  });

  it("reports a miss with nothing visited as a miss, not as an absent outcome", () => {
    // Every candidate was pruned by the stage-1 index, so nothing was ever evaluated. The request
    // still did not match, and that is a different fact from "no outcome was recorded".
    expect(describeOutcome(wire({ matched: false }))).toEqual({
      kind: "unmatched",
      tried: [],
      omitted: 0,
    });
  });
});

describe("describeOutcome on an entry carrying no outcome", () => {
  // The distinction the schema states in bold: absence means *not recorded* — an entry from an
  // engine predating the field, an `X-Rift-Debug` request, or a matcher error — never "did not
  // match". Folding it into a miss would tell an operator their stub was rejected when nothing
  // judged it.
  it("reads an absent outcome as no diagnostics, never as a failed match", () => {
    expect(describeOutcome(undefined)).toEqual({ kind: "none" });
  });

  // The engine omits the field rather than nulling it, but a `null` carries the same meaning, and
  // calling a serializer's null "unreadable" would teach operators to ignore that word.
  it("reads a null outcome the same way it reads an absent one", () => {
    expect(describeOutcome(wire(null))).toEqual({ kind: "none" });
  });
});

describe("describeOutcome on an outcome it cannot read", () => {
  // A shape the console does not recognise is not the same as an entry with no outcome: one says
  // the node answered with something wrong, the other says nothing was recorded. Collapsing them
  // would hide a broken engine behind a sentence that reads as routine.
  it("reports a malformed outcome as unreadable rather than as absent", () => {
    for (const malformed of [
      { matched: "yes" },
      { stubIndex: 1 },
      { matched: false, tried: "none" },
      { matched: false, tried: [{ why: { reason: "skippedSpace" } }] },
      { matched: false, tried: [{ stubIndex: 0, why: { reason: 7 } }] },
      { matched: false, tried: [{ stubIndex: 0, why: { reason: "" } }] },
      { matched: false, tried: [{ stubIndex: 0 }] },
      { matched: false, triedOmitted: "many" },
      { matched: true, stubId: 7 },
      { matched: true, stubIndex: -1 },
      "matched",
      7,
      [],
    ]) {
      expect([malformed, describeOutcome(wire(malformed)).kind]).toEqual([malformed, "unreadable"]);
    }
  });

  // Guards the guard: if `describeOutcome` answered `unreadable` for anything it did not fully
  // understand, the loop above would pass while the panel never rendered a real diagnosis.
  it("does not call a well-formed outcome unreadable", () => {
    expect(describeOutcome(wire({ matched: true, stubIndex: 0 })).kind).toBe("matched");
    expect(describeOutcome(wire({ matched: false, tried: [] })).kind).toBe("unmatched");
  });

  // The schema is `additionalProperties: true` on all three shapes, so a newer engine adding a
  // field must not turn a readable outcome into an unreadable one.
  it("ignores fields the contract does not declare rather than refusing the outcome", () => {
    const view = describeOutcome(
      wire({
        matched: false,
        cost: { micros: 42 },
        tried: [{ stubIndex: 0, why: { reason: "skippedSpace", note: "x" }, elapsedMicros: 3 }],
      }),
    );
    expect(view).toEqual({
      kind: "unmatched",
      tried: [{ label: "stub #0", why: "space did not match" }],
      omitted: 0,
    });
  });
});
