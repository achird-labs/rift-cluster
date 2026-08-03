import { describe, expect, it } from "vitest";

import { FAULT_KINDS, canonicalFaultKind } from "./behaviors.ts";
import {
  describeResponses,
  faultFiresAsRift,
  faultIsArmed,
  foreignBehaviorsOf,
  projectResponses,
  renderResponses,
} from "./responses.ts";

/** Project one response and return it, failing loudly if the form refused the stub. */
function one(response: unknown) {
  const projected = projectResponses({ responses: [response] });
  if (projected.kind !== "responses") {
    throw new Error(`expected a response list, got rawOnly: ${projected.unmodelledKeys.join(", ")}`);
  }
  const [item] = projected.items;
  if (item === undefined) throw new Error("expected one response");
  return item;
}

/** Project one response expecting a refusal, and return the keys it named. */
function refused(response: unknown): string[] {
  const projected = projectResponses({ responses: [response] });
  if (projected.kind !== "rawOnly") throw new Error("expected rawOnly");
  return projected.unmodelledKeys;
}

describe("AC1 — wait and repeat round-trip in whichever of the three spellings they arrived in", () => {
  it("reads a fixed wait and a repeat, and writes them back unchanged", () => {
    const source = { is: { statusCode: 200 }, _behaviors: { wait: 50, repeat: 3 } };
    const response = one(source);
    expect(response.behaviors?.wait).toEqual({ kind: "fixed", ms: 50 });
    expect(response.behaviors?.repeat).toBe(3);
    expect(renderResponses([response])).toEqual([source]);
  });

  it("reads a random-range wait", () => {
    const source = { is: { statusCode: 200 }, _behaviors: { wait: { min: 10, max: 100 } } };
    const response = one(source);
    expect(response.behaviors?.wait).toEqual({ kind: "range", min: 10, max: 100 });
    expect(renderResponses([response])).toEqual([source]);
  });

  it("preserves each of the three spellings rather than normalising to one", () => {
    /*
     * The engine accepts `_behaviors` as an object, `behaviors` as an object, and `behaviors` as an
     * ARRAY of single-key objects, and re-emits whichever it was given — its SDK parse-fidelity gate
     * requires `GET /imposters` to round-trip. Normalising here would show as a diff on every export
     * of a stub the console merely opened.
     */
    for (const source of [
      { is: { statusCode: 200 }, _behaviors: { wait: 50 } },
      { is: { statusCode: 200 }, behaviors: { wait: 50 } },
      { is: { statusCode: 200 }, behaviors: [{ wait: 50 }] },
    ]) {
      expect(renderResponses([one(source)])).toEqual([source]);
    }
  });

  it("preserves the order of an array-spelled behaviours list, in BOTH directions", () => {
    /*
     * Reordering is not data loss, but it is a diff on a document the operator may be reviewing
     * beside a file. Both orders are asserted deliberately: a renderer that sorted its keys would
     * still round-trip whichever order happens to be the sorted one, so testing only that order
     * proves nothing about ordering at all.
     */
    for (const behaviors of [
      [{ repeat: 2 }, { wait: 50 }],
      [{ wait: 50 }, { repeat: 2 }],
    ]) {
      const source = { is: { statusCode: 200 }, behaviors };
      expect(renderResponses([one(source)])).toEqual([source]);
    }
  });

  it("carries behaviours on a flat response, where they sit beside statusCode rather than inside it", () => {
    // The flat form is the fiddly one: `_behaviors` and `statusCode` are genuinely adjacent, so
    // telling them apart is by name, not by depth.
    const source = { statusCode: 200, body: "hi", _behaviors: { wait: 25 } };
    const response = one(source);
    expect(response.wrapped).toBe(false);
    expect(response.behaviors?.wait).toEqual({ kind: "fixed", ms: 25 });
    expect(renderResponses([response])).toEqual([source]);
  });

  it("distinguishes a response with no behaviours key from one carrying an empty container", () => {
    expect(one({ is: { statusCode: 200 } }).behaviors).toBeNull();
    expect(renderResponses([one({ is: { statusCode: 200 } })])).toEqual([{ is: { statusCode: 200 } }]);

    /*
     * An empty container is REFUSED, in all three spellings. `BehaviorModel` has no way to record
     * "the source had a container and it was empty", so accepting one would render it back to
     * nothing and drop the key unnamed — the diff-on-export this module exists to prevent. Refusing
     * is safe because the engine never emits an empty container (`behaviors_to_array` maps empty to
     * `None`), so only hand-written JSON can carry one.
     */
    expect(refused({ is: { statusCode: 200 }, _behaviors: {} })).toEqual(["responses[0]._behaviors"]);
    expect(refused({ is: { statusCode: 200 }, behaviors: {} })).toEqual(["responses[0].behaviors"]);
    expect(refused({ is: { statusCode: 200 }, behaviors: [] })).toEqual(["responses[0].behaviors"]);
  });

  it("carries a behaviour and a fault on the same response", () => {
    // Two independent `if`s build the response-level keys; an `else if` slipped in later would
    // silently drop one of them and nothing else in the suite would notice.
    const source = { is: { statusCode: 200 }, _behaviors: { wait: 50 }, fault: "EMPTY_RESPONSE" };
    const response = one(source);
    expect(response.behaviors?.wait).toEqual({ kind: "fixed", ms: 50 });
    expect(response.fault).toEqual({ form: "responseKey", kind: "EMPTY_RESPONSE" });
    expect(renderResponses([response])).toEqual([source]);
  });
});

describe("AC2 — the four fault kinds, and the probability form", () => {
  it("models every canonical fault kind on the response key", () => {
    for (const kind of FAULT_KINDS) {
      const source = { fault: kind };
      expect(one(source).fault).toEqual({ form: "responseKey", kind });
      expect(renderResponses([one(source)])).toEqual([source]);
    }
  });

  it("keeps a bare `_rift.fault.tcp` string bare, and a probabilistic one an object", () => {
    /*
     * `RiftTcpFault` is serialized back in exactly the form it was parsed from — the engine's own
     * comment says so, because its parse-fidelity gate depends on it. The object form exists SOLELY
     * to carry a probability, so it is not a second spelling of the bare form.
     */
    const bare = { _rift: { fault: { tcp: "EMPTY_RESPONSE" } } };
    expect(one(bare).fault).toEqual({ form: "riftString", kind: "EMPTY_RESPONSE" });
    expect(renderResponses([one(bare)])).toEqual([bare]);

    const probabilistic = { _rift: { fault: { tcp: { probability: 0.1, type: "EMPTY_RESPONSE" } } } };
    expect(one(probabilistic).fault).toEqual({
      form: "riftObject",
      kind: "EMPTY_RESPONSE",
      probability: 0.1,
    });
    expect(renderResponses([one(probabilistic)])).toEqual([probabilistic]);
  });

  it("accepts the engine's short aliases on the way in without rewriting them on the way out", () => {
    // `TcpFaultKind::parse` takes both spellings. The picker writes canonical names, but a document
    // that already says `reset` is not rewritten — that would be a diff nobody asked for.
    const source = { fault: "reset" };
    expect(one(source).fault).toEqual({ form: "responseKey", kind: "reset" });
    expect(renderResponses([one(source)])).toEqual([source]);
    expect(canonicalFaultKind("reset")).toBe("CONNECTION_RESET_BY_PEER");
  });

  it("refuses an object fault with no probability, exactly as the engine's parser does", () => {
    // `{type: X}` alone would be a second spelling of the bare form and would break the round-trip
    // the engine's own gate enforces, so the engine errors — and so does this.
    expect(refused({ _rift: { fault: { tcp: { type: "EMPTY_RESPONSE" } } } })).toEqual([
      "responses[0]._rift.fault.tcp.probability",
    ]);
  });

  it("refuses a probability outside 0..1 and an unknown fault kind", () => {
    expect(refused({ _rift: { fault: { tcp: { probability: 1.5, type: "EMPTY_RESPONSE" } } } })).toEqual([
      "responses[0]._rift.fault.tcp.probability",
    ]);
    expect(refused({ fault: "NOT_A_FAULT" })).toEqual(["responses[0].fault"]);
  });

  it("refuses a response carrying two faults at once, which it could not re-emit", () => {
    expect(
      refused({ fault: "EMPTY_RESPONSE", _rift: { fault: { tcp: "CONNECTION_RESET_BY_PEER" } } }),
    ).toEqual(["responses[0]._rift.fault.tcp"]);
  });

  it("still labels a fault as a fault, since it REPLACES the response", () => {
    expect(describeResponses({ responses: [{ fault: "EMPTY_RESPONSE" }] })).toEqual([
      { index: 0, kind: "fault", detail: "EMPTY_RESPONSE" },
    ]);
  });
});

describe("which fault form fires depends on the `is` KEY, not on having a body", () => {
  /*
   * Verified against the running engine, not inferred. The two dispatch tests point opposite ways:
   *
   *   {"is":{"statusCode":201,"body":"hi"},"_rift":{"fault":{"tcp":"EMPTY_RESPONSE"}}}  -> fires
   *   {"is":{"statusCode":201,"body":"hi"},"fault":"EMPTY_RESPONSE"}                    -> DEAD
   *   {"statusCode":201,"body":"hi","fault":"EMPTY_RESPONSE"}                           -> fires
   *   {"statusCode":201,"body":"hi","_rift":{"fault":{"tcp":"EMPTY_RESPONSE"}}}         -> DEAD,
   *      and the status and body are erased on the next read (the response becomes a RiftScript).
   *
   * The last row is the one that matters most: flat IS the recorded-imposter form, so a predicate
   * keyed on "has a status or body" gets every recorded stub exactly backwards.
   */
  it("arms a rift fault on a wrapped response and a response-key fault on a flat one", () => {
    const wrappedRift = one({ is: { statusCode: 201 }, _rift: { fault: { tcp: "EMPTY_RESPONSE" } } });
    expect(faultFiresAsRift(wrappedRift)).toBe(true);
    expect(faultIsArmed(wrappedRift)).toBe(true);

    const flatResponseKey = one({ statusCode: 201, body: "hi", fault: "EMPTY_RESPONSE" });
    expect(faultFiresAsRift(flatResponseKey)).toBe(false);
    expect(faultIsArmed(flatResponseKey)).toBe(true);
  });

  it("reports a fault as unarmed in each of the two dead spellings", () => {
    // Wrapped + top-level `fault`: never reached, and dropped on the next read.
    expect(faultIsArmed(one({ is: { statusCode: 201 }, fault: "EMPTY_RESPONSE" }))).toBe(false);
    // Flat + `_rift`: becomes a RiftScript, so the fault never fires AND the body is erased.
    expect(
      faultIsArmed(one({ statusCode: 201, body: "hi", _rift: { fault: { tcp: "EMPTY_RESPONSE" } } })),
    ).toBe(false);
  });

  it("does not treat a flat response's body as evidence that a rift fault would fire", () => {
    // The exact regression: `hasIsBody` was true here, so the picker wrote `_rift` — inert.
    expect(faultFiresAsRift(one({ statusCode: 201, headers: { A: "1" }, body: "hi" }))).toBe(false);
  });
});

describe("AC5 — behaviours this form does not edit are named, not swallowed", () => {
  it("refuses each of copy, lookup, decorate and shellTransform, naming the key", () => {
    // Form editors for a shell command and two JS hooks are a separate decision with their own
    // security surface. Silently hiding them is what the raw-only rule exists to prevent.
    for (const key of ["copy", "lookup", "decorate", "shellTransform"]) {
      expect(refused({ is: { statusCode: 200 }, _behaviors: { [key]: {} } })).toEqual([
        `responses[0]._behaviors.${key}`,
      ]);
    }
  });

  it("refuses a function-form wait rather than pretending to edit JavaScript", () => {
    expect(refused({ is: { statusCode: 200 }, _behaviors: { wait: "function () { return 50; }" } })).toEqual([
      "responses[0]._behaviors.wait",
    ]);
  });

  it("names a modelled behaviour alongside an unmodelled one, not merely the first", () => {
    const keys = refused({ is: { statusCode: 200 }, _behaviors: { wait: 50, decorate: "fn" } });
    expect(keys).toEqual(["responses[0]._behaviors.decorate"]);
  });

  it("refuses a `_rift` that carries nothing this form models, rather than deleting the key", () => {
    /*
     * Not inert. On a FLAT response the engine checks `raw.rift` BEFORE the flat statusCode/body
     * branch, so the mere presence of `_rift` decides whether it builds a `RiftScript` or an `Is`.
     * Dropping the key silently would change which response variant the engine constructs.
     */
    expect(refused({ is: { statusCode: 200 }, _rift: {} })).toEqual(["responses[0]._rift"]);
    expect(refused({ is: { statusCode: 200 }, _rift: { fault: {} } })).toEqual([
      "responses[0]._rift.fault",
    ]);
  });

  it("names the behaviours a response runs that the form cannot edit, per response (AC5)", () => {
    // "Recognised" is the half that must survive the refusal: without this a response running
    // `decorate` is labelled identically to a plain one, and the only trace is a dotted key buried
    // in the generic unmodelled-keys banner.
    expect(foreignBehaviorsOf({ is: { statusCode: 200 }, _behaviors: { decorate: "fn" } })).toEqual([
      "decorate",
    ]);
    expect(
      foreignBehaviorsOf({ is: { statusCode: 200 }, _behaviors: { wait: "function () {}" } }),
    ).toEqual(["wait (function)"]);
    expect(
      foreignBehaviorsOf({ is: { statusCode: 200 }, behaviors: [{ wait: 50 }, { shellTransform: "x" }] }),
    ).toEqual(["shellTransform"]);
    // A response running only modelled behaviours has nothing extra to announce.
    expect(foreignBehaviorsOf({ is: { statusCode: 200 }, _behaviors: { wait: 50 } })).toEqual([]);
  });

  it("refuses the `_rift` extensions that are not `fault.tcp`", () => {
    expect(refused({ is: { statusCode: 200 }, _rift: { templated: true } })).toEqual([
      "responses[0]._rift.templated",
    ]);
    expect(refused({ is: { statusCode: 200 }, _rift: { fault: { latency: { ms: 5 } } } })).toEqual([
      "responses[0]._rift.fault.latency",
    ]);
  });

  it("refuses both behaviour spellings at once rather than silently picking one", () => {
    // The engine takes one and drops the other; there is no honest single model of "these disagree".
    expect(
      refused({ is: { statusCode: 200 }, _behaviors: { wait: 1 }, behaviors: { wait: 2 } }),
    ).toEqual(["responses[0]._behaviors", "responses[0].behaviors"]);
  });

  it("refuses a repeated behaviour key in the array spelling", () => {
    expect(refused({ is: { statusCode: 200 }, behaviors: [{ wait: 1 }, { wait: 2 }] })).toEqual([
      "responses[0].behaviors.wait",
    ]);
  });
});
