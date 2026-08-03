import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  type BehaviorModel,
  type BehaviorSpelling,
  FAULT_KINDS,
  type FaultModel,
  type ModelledBehavior,
  type WaitModel,
} from "./behaviors.ts";
import {
  type ResponseBody,
  type ResponseHeader,
  type ResponseModel,
  blankResponse,
  describeResponses,
  projectResponses,
  renderResponses,
} from "./responses.ts";

/**
 * An arbitrary response list — the input the round-trip property is about.
 *
 * Header names are deliberately NOT forced unique. Two rows sharing a name render to one JSON array
 * (that is what a multi-value header *is*), which reads back as two rows again — so the property
 * holds at the JSON level even though the model is not fixed point. Generating duplicates is the
 * point: it is the shape `Set-Cookie` actually takes, and the one a naive `Object.fromEntries`
 * would silently collapse to a single value.
 */
const anyHeader: fc.Arbitrary<ResponseHeader> = fc.record({
  name: fc.constantFrom("Content-Type", "Set-Cookie", "X-Trace", "Location", ""),
  // Carried verbatim, so the generator must offer the non-string scalars the engine tolerates.
  value: fc.oneof(fc.string(), fc.integer(), fc.boolean()),
  multi: fc.boolean(),
});

const anyBody: fc.Arbitrary<ResponseBody> = fc.oneof(
  fc.constant<ResponseBody>({ kind: "absent" }),
  fc.string().map<ResponseBody>((text) => ({ kind: "text", text })),
  // Every non-string JSON shape, because `projectResponses` classifies ALL of them as `json` — a
  // generator of flat objects-of-numbers would never stress a nested body, and `null`/`42`/`true`
  // at `body` are legal documents (`Option<serde_json::Value>`) that must round-trip too.
  fc
    .oneof(
      fc.constant(null),
      fc.integer(),
      fc.boolean(),
      fc.array(fc.integer()),
      fc.dictionary(fc.string(), fc.oneof(fc.integer(), fc.array(fc.string()))),
      fc.dictionary(fc.string(), fc.dictionary(fc.string(), fc.integer())),
    )
    .map<ResponseBody>((value) => ({ kind: "json", value })),
);

/**
 * Behaviours in all three spellings, including the array form whose ORDER must survive. `null` (no
 * behaviours key at all) is generated too: it is a different document from an empty one.
 */
const anyBehaviors: fc.Arbitrary<BehaviorModel | null> = fc.option(
  fc
    .record({
      spelling: fc.constantFrom<BehaviorSpelling>("_behaviors", "behaviorsObject", "behaviorsArray"),
      order: fc.shuffledSubarray(["wait", "repeat"] as ModelledBehavior[], { minLength: 1 }),
      wait: fc.oneof(
        fc.constant<WaitModel>({ kind: "none" }),
        fc.integer({ min: 0, max: 5000 }).map<WaitModel>((ms) => ({ kind: "fixed", ms })),
        fc
          .tuple(fc.integer({ min: 0, max: 100 }), fc.integer({ min: 100, max: 5000 }))
          .map<WaitModel>(([min, max]) => ({ kind: "range", min, max })),
      ),
      repeat: fc.option(fc.integer({ min: 1, max: 10 }), { nil: null }),
    })
    // A model whose `order` names a key it does not carry renders nothing for it, which is fine —
    // but then `order` itself cannot round-trip, so the generator only lists keys that are present.
    .map((model) => ({
      ...model,
      order: model.order.filter((key) =>
        key === "repeat" ? model.repeat !== null : model.wait.kind !== "none",
      ),
    })),
  { nil: null },
);

/** All three fault spellings — the response key, and `_rift.fault.tcp` bare and probabilistic. */
const anyFault: fc.Arbitrary<FaultModel | null> = fc.option(
  fc.oneof(
    fc.constantFrom(...FAULT_KINDS).map<FaultModel>((kind) => ({ form: "responseKey", kind })),
    fc.constantFrom(...FAULT_KINDS).map<FaultModel>((kind) => ({ form: "riftString", kind })),
    fc
      .tuple(fc.constantFrom(...FAULT_KINDS), fc.double({ min: 0, max: 1, noNaN: true }))
      .map<FaultModel>(([kind, probability]) => ({ form: "riftObject", kind, probability })),
  ),
  { nil: null },
);

const anyResponse: fc.Arbitrary<ResponseModel> = fc.record({
  wrapped: fc.boolean(),
  /*
   * NOT restricted to 100..599. That range is what a status code sensibly is, but it is not what
   * this field can HOLD: the status input accepts any finite number, and a generator confined to
   * plausible values could never reach the case where the string spelling and the number disagree
   * (`200.5` stringifies to `"200.5"`, which is not a spelling the parser accepts back).
   */
  statusCode: fc.option(
    fc.oneof(
      fc.integer({ min: 0, max: 65535 }),
      fc.integer(),
      fc.double({ noNaN: true, noDefaultInfinity: true }),
    ),
    { nil: null },
  ),
  // Both spellings, because the round-trip has to be a fixed point over each of them — and the
  // string one is what every response read back from the engine actually carries.
  statusText: fc.boolean(),
  headersPresent: fc.boolean(),
  headers: fc.array(anyHeader, { maxLength: 4 }),
  body: anyBody,
  behaviors: anyBehaviors,
  fault: anyFault,
});

const anyResponseList: fc.Arbitrary<ResponseModel[]> = fc.array(anyResponse, { maxLength: 4 });

describe("the response list ⟷ JSON projection round-trips (AC4, AC6)", () => {
  it("projects everything it renders back to a response list, never to raw-only", () => {
    fc.assert(
      fc.property(anyResponseList, (items) => {
        expect(projectResponses({ responses: renderResponses(items) }).kind).toBe("responses");
      }),
    );
  });

  it("renders the projection of a rendered list to the same JSON", () => {
    // `render ∘ project ∘ render == render`, the same lossless claim `projection.test.ts` makes.
    // Stated over the JSON rather than the model on purpose: two models render identically (two
    // header rows sharing a name ARE one multi-value header), and it is the JSON the fleet stores.
    fc.assert(
      fc.property(anyResponseList, (items) => {
        const json = renderResponses(items);
        const projected = projectResponses({ responses: json });
        if (projected.kind !== "responses") throw new Error("expected a response list");
        expect(renderResponses(projected.items)).toEqual(json);
      }),
    );
  });

  it("round-trips a flat response flat and an is-wrapped one wrapped, in the same list", () => {
    // AC4 head-on. A recorded stub is flat; rewriting it into `is` form would show as a spurious
    // diff on every export and defeat #251, so the shape each response ARRIVED in is what it leaves in.
    const source = [
      { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" },
      { is: { statusCode: 500 } },
    ];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(projected.items.map((item) => item.wrapped)).toEqual([false, true]);
    expect(renderResponses(projected.items)).toEqual(source);
  });
});

describe("arbitrary headers (AC2)", () => {
  it("carries every header as its own row, with Content-Type no longer special-cased", () => {
    const projected = projectResponses({
      responses: [
        {
          is: {
            headers: { "Content-Type": "application/json", "X-Trace": "1", Location: "/there" },
          },
        },
      ],
    });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    const [response] = projected.items;
    if (response === undefined) throw new Error("expected one response");
    expect(response.headers.map((header) => header.name)).toEqual([
      "Content-Type",
      "X-Trace",
      "Location",
    ]);
  });

  it("reads a multi-value header as one row per value and writes it back as an array", () => {
    // The `Set-Cookie` case the issue names. The engine models headers as `Vec<String>`; collapsing
    // two cookies into one value would silently drop a header line the mock used to send.
    const source = [{ is: { headers: { "Set-Cookie": ["a=1", "b=2"] } } }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    const [response] = projected.items;
    if (response === undefined) throw new Error("expected one response");
    expect(response.headers).toHaveLength(2);
    expect(response.headers.every((header) => header.name === "Set-Cookie")).toBe(true);
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("keeps a single-element array an array rather than collapsing it to a bare string", () => {
    // `["a"]` and `"a"` are the same header on the wire but different documents; rewriting one into
    // the other is the spurious export diff AC4 exists to prevent.
    const source = [{ is: { headers: { "Set-Cookie": ["a=1"] } } }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("keeps an empty `headers: {}`, which the engine emits on every header-less response", () => {
    /*
     * The regression that made this a blocker: `IsResponseOut` has no `skip_serializing_if` on its
     * headers map, so `"headers": {}` is on essentially every recorded response. Projecting it to
     * zero rows and rendering nothing back deletes the key from the operator's document — silently,
     * with nothing named — and shows up as a diff on nearly every response of an export (#251).
     */
    const source = [{ is: { statusCode: 200, headers: {} } }];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(projected.items[0]?.headers).toEqual([]);
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("distinguishes an absent headers key from an empty one", () => {
    const projected = projectResponses({ responses: [{ is: { statusCode: 200 } }] });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(projected.items[0]?.headersPresent).toBe(false);
    // No `headers` key going in, so none coming out — the mirror of the test above.
    expect(renderResponses(projected.items)).toEqual([{ is: { statusCode: 200 } }]);
  });

  it("refuses a header name mapped to zero values rather than letting the name vanish", () => {
    // One row per VALUE means zero values is zero rows, and the name would disappear on the next
    // save with nothing named. Degenerate enough that no recorded stub has one, so refusing costs
    // a hand-written document only a trip through the raw editor.
    const projected = projectResponses({ responses: [{ is: { headers: { "X-Trace": [] } } }] });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(['responses[0].is.headers["X-Trace"]']);
  });

  it("refuses an unnamed header rather than showing it as a row and then deleting it", () => {
    /*
     * `renderHeaders` drops unnamed rows on purpose — that is what stops the builder's freshly
     * added rows merging with each other. Accepting one on the way IN would therefore be the worst
     * of both: the key is in the source, it renders as a row the operator can see and type into,
     * and an unrelated edit (changing the status code) silently deletes it with nothing named.
     */
    const projected = projectResponses({
      responses: [{ is: { statusCode: 200, headers: { "": "x", "X-Real": "1" } } }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(['responses[0].is.headers[""]']);
  });

  it("round-trips a header named `__proto__` instead of letting it hit the prototype setter", () => {
    // `rendered["__proto__"] = v` on an object literal invokes the inherited setter: it reassigns
    // the prototype and creates NO own property, so the header would vanish with nothing named.
    for (const source of [
      [{ is: { headers: { ["__proto__"]: "x" } } }],
      [{ is: { headers: { ["__proto__"]: ["a", "b"] } } }],
    ]) {
      const projected = projectResponses({ responses: source });
      expect(projected.kind).toBe("responses");
      if (projected.kind !== "responses") continue;
      const rendered = renderResponses(projected.items);
      expect(JSON.stringify(rendered)).toBe(JSON.stringify(source));
    }
  });

  it("refuses a headers value that is not an object, and a header value that is not a scalar", () => {
    // The refusal branches. Without these the `issues.push` call sites could be deleted and the
    // suite would stay green, while the form silently started accepting shapes it then mangles.
    for (const [headers, expected] of [
      ["not-an-object", "responses[0].is.headers"],
      [["a", "b"], "responses[0].is.headers"],
      [{ "X-Bad": { nested: true } }, 'responses[0].is.headers["X-Bad"]'],
      [{ "Set-Cookie": ["a", { bad: 1 }] }, 'responses[0].is.headers["Set-Cookie"]'],
    ] as const) {
      const projected = projectResponses({ responses: [{ is: { headers } }] });
      expect([headers, projected.kind]).toEqual([headers, "rawOnly"]);
      if (projected.kind !== "rawOnly") continue;
      expect(projected.unmodelledKeys).toEqual([expected]);
    }
  });

  it("preserves a non-string header value verbatim rather than coercing or refusing it", () => {
    // Mountebank's recorders emit `"Content-Length": 124` and the engine tolerates it deliberately
    // (upstream #754). Refusing would send every recorded imposter to raw-only; stringifying would
    // rewrite the operator's document on the way through the form.
    const source = [{ is: { headers: { "Content-Length": 124, "X-Flag": true } } }];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(renderResponses(projected.items)).toEqual(source);
  });
});

describe("bodies (AC3)", () => {
  it("opens a JSON-object body in the form and writes it back as a JSON value, never stringified", () => {
    const source = [{ is: { body: { ok: true, items: [1, 2] } } }];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    const [response] = projected.items;
    if (response === undefined) throw new Error("expected one response");
    expect(response.body).toEqual({ kind: "json", value: { ok: true, items: [1, 2] } });
    // The whole point: an object at `body` stays an object. Stringifying it would change what the
    // mock returns — a client parsing the body would get a string where it had an object.
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("keeps a JSON-array body an array", () => {
    const source = [{ is: { body: [{ id: 1 }] } }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("keeps a JSON `null` body, which is a body the stub sends and not an absent one", () => {
    const source = [{ is: { body: null } }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(projected.items[0]?.body).toEqual({ kind: "json", value: null });
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("keeps a text body text, and distinguishes it from an absent body", () => {
    const projected = projectResponses({
      responses: [{ is: { body: "hello" } }, { is: { statusCode: 204 } }],
    });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(projected.items.map((item) => item.body.kind)).toEqual(["text", "absent"]);
    // An absent body emits no key — `body: ""` is a stub that answers with an empty body, which is
    // a different document from one that carries no body at all.
    expect(renderResponses(projected.items)).toEqual([{ is: { body: "hello" } }, { is: { statusCode: 204 } }]);
  });
});

describe("responses richer than the form are recognised, labelled, and refused (AC5)", () => {
  it("names a proxy response rather than modelling it, and labels it as a proxy", () => {
    const stub = {
      responses: [{ is: { statusCode: 200 } }, { proxy: { to: "http://api.example.com" } }],
    };
    const projected = projectResponses(stub);
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[1].proxy"]);
    // Recognised ≠ editable: the operator must still be able to SEE that the stub has a proxy,
    // which is what the label is for.
    expect(describeResponses(stub)).toEqual([
      { index: 0, kind: "is", detail: "200" },
      { index: 1, kind: "proxy", detail: "http://api.example.com" },
    ]);
  });

  it("names an inject response and labels it, without offering a JavaScript editor", () => {
    const stub = { responses: [{ inject: "function (request) { return {}; }" }] };
    const projected = projectResponses(stub);
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[0].inject"]);
    expect(describeResponses(stub).map((label) => label.kind)).toEqual(["inject"]);
  });

  it("models a fault response now, and still labels it as a fault", () => {
    // #248 refused this; #249 models it, because the fault picker is the whole point of that slice.
    // The LABEL is unchanged either way — a fault still replaces the response.
    const stub = { responses: [{ fault: "CONNECTION_RESET_BY_PEER" }] };
    const projected = projectResponses(stub);
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(projected.items[0]?.fault).toEqual({
      form: "responseKey",
      kind: "CONNECTION_RESET_BY_PEER",
    });
    expect(renderResponses(projected.items)).toEqual(stub.responses);
    expect(describeResponses(stub)).toEqual([
      { index: 0, kind: "fault", detail: "CONNECTION_RESET_BY_PEER" },
    ]);
  });

  it("refuses a `_mode`-carrying response rather than corrupting a base64 body, and still labels it", () => {
    const stub = { responses: [{ is: { body: "aGk=", _mode: "binary" } }] };
    const projected = projectResponses(stub);
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[0].is._mode"]);
    // The AC names `_mode` alongside proxy/inject as needing to be *labelled*, not merely refused.
    // It is still an `is` response — a binary one — so that is what the label says.
    expect(describeResponses(stub)).toEqual([{ index: 0, kind: "is", detail: "200" }]);
  });

  it("labels a `_rift` script response as a script, not as a 200 it does not answer", () => {
    // `StubResponseRaw` gives `_rift` its own field and a `_rift`-only response becomes a
    // `RiftScript` — a fifth variant. Falling through to the `is` branch reported `200`, telling the
    // operator the opposite of what the stub does, in exactly the banner that exists to inform them.
    const stub = { responses: [{ _rift: { script: "x" } }] };
    const projected = projectResponses(stub);
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    // #249 made this more precise: `_rift.fault.tcp` is modelled now, so the refusal names the
    // part that is NOT (`script`) rather than the whole extension.
    expect(projected.unmodelledKeys).toEqual(["responses[0]._rift.script"]);
    expect(describeResponses(stub)).toEqual([{ index: 0, kind: "_rift", detail: "" }]);
  });

  it("labels a string status code by what it says, not by the default (the API's own wire form)", () => {
    /*
     * `IsResponseOut` serializes `statusCode` AS A STRING, so every response read back from the
     * admin API carries one — and a string `statusCode` is precisely what sends a stub to raw-only.
     * That makes this the common path, not an edge case, and raw-only is exactly where these labels
     * are the operator's only readout. Reporting "200" for a stub that answers 404 is confidently
     * wrong in the one place it is most relied on.
     */
    const stub = { responses: [{ is: { statusCode: "404" } }] };
    // #257 made this the MODELLED path — it used to be raw-only, which meant every stub read back
    // from the API opened raw-only. The label was correct even then and still is.
    expect(projectResponses(stub).kind).toBe("responses");
    expect(describeResponses(stub)).toEqual([{ index: 0, kind: "is", detail: "404" }]);
  });

  it("labels a response by what the ENGINE would render, when it carries more than one variant", () => {
    // The engine's `From<StubResponseRaw>` precedence is is > proxy > inject > fault > _rift, so a
    // response holding both `is` and `proxy` answers with its `is`. Refusing to EDIT it (the extra
    // key is named) and describing what it DOES are different questions.
    expect(describeResponses({ responses: [{ is: { statusCode: 201 }, proxy: { to: "http://a" } }] })).toEqual([
      { index: 0, kind: "is", detail: "201" },
    ]);
    expect(describeResponses({ responses: [{ is: { statusCode: 201 }, _rift: { script: "x" } }] })).toEqual([
      { index: 0, kind: "is", detail: "201" },
    ]);
  });

  it("refuses a response element that is not a JSON object", () => {
    for (const element of [42, null, "x", ["a"]]) {
      const projected = projectResponses({ responses: [element] });
      expect([element, projected.kind]).toEqual([element, "rawOnly"]);
      if (projected.kind !== "rawOnly") continue;
      expect(projected.unmodelledKeys).toEqual(["responses[0]"]);
    }
  });

  it("models a `_behaviors` wait beside `is`, which #248 refused", () => {
    const source = [{ is: { statusCode: 200 }, _behaviors: { wait: 50 } }];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(projected.items[0]?.behaviors?.wait).toEqual({ kind: "fixed", ms: 50 });
    expect(renderResponses(projected.items)).toEqual(source);
  });

  it("treats a status code of a type the engine never emits as unmodelled, not as a coercion", () => {
    /*
     * A number and a canonical numeric string are both real wire spellings and both model now
     * (#257). Everything else is still named rather than guessed at — coercing would rewrite the
     * operator's document on the way through the form.
     */
    // Note `200.5` is deliberately absent: it is a `number`, and this form has never validated the
    // numeric range or integrality of a status code — the server and `rift-lint` own that. Refusing
    // it here would be new behaviour smuggled in under a serialization fix.
    for (const statusCode of [true, null, [200], { code: 200 }]) {
      const projected = projectResponses({ responses: [{ is: { statusCode } }] });
      expect([statusCode, projected.kind]).toEqual([statusCode, "rawOnly"]);
      if (projected.kind !== "rawOnly") continue;
      expect(projected.unmodelledKeys).toEqual(["responses[0].is.statusCode"]);
    }
  });

  it("names every unmodelled response, not merely the first", () => {
    // An operator told about one would edit the form, save, and silently drop the other.
    const projected = projectResponses({
      responses: [{ proxy: { to: "http://a" } }, { inject: "fn" }],
    });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses[0].proxy", "responses[1].inject"]);
  });

  it("refuses a responses value that is not an array", () => {
    const projected = projectResponses({ responses: { is: { statusCode: 200 } } });
    expect(projected.kind).toBe("rawOnly");
    if (projected.kind !== "rawOnly") return;
    expect(projected.unmodelledKeys).toEqual(["responses"]);
  });
});

describe("the empty and absent cases", () => {
  it("reads a stub with no responses key as an empty list, not a refusal", () => {
    // Nothing about a response-less stub is unmodelled; it just answers with the engine's default.
    const projected = projectResponses({ id: "s-1" });
    expect(projected).toEqual({ kind: "responses", items: [] });
  });

  it("reads an empty responses array as an empty list", () => {
    expect(projectResponses({ responses: [] })).toEqual({ kind: "responses", items: [] });
  });

  it("renders an empty list to an empty array, so the editor can omit the key entirely", () => {
    expect(renderResponses([])).toEqual([]);
  });

  it("renders a blank response as an is-wrapped 200, the shape the editor appends", () => {
    expect(renderResponses([blankResponse()])).toEqual([{ is: { statusCode: 200 } }]);
  });
});

describe("#257 — the string status code the engine actually emits", () => {
  /*
   * `IsResponseOut.status_code` goes through `serialize_status_code_as_string`, deliberately, for
   * Mountebank wire compatibility. So EVERY response read back from `GET /imposters/:port` carries
   * `"statusCode": "200"` — and modelling only the number spelling meant every existing stub opened
   * raw-only. The form was reachable for a never-saved stub and nothing else.
   *
   * Nothing caught it because the e2e only ever clicked "Add stub" (which starts from a local
   * constant with a numeric status) and every unit fixture used the numeric spelling too.
   */
  it("opens a stub whose statusCode is a string, and writes it back as a string", () => {
    const source = [{ is: { statusCode: "200", headers: { "Content-Type": "application/json" } } }];
    const projected = projectResponses({ responses: source });
    expect(projected.kind).toBe("responses");
    if (projected.kind !== "responses") return;
    expect(projected.items[0]?.statusCode).toBe(200);
    expect(projected.items[0]?.statusText).toBe(true);
    // Byte-identical: the spelling it arrived in is the spelling it leaves in, so opening a stub
    // and saving it untouched is not a diff.
    expect(JSON.stringify(renderResponses(projected.items))).toBe(JSON.stringify(source));
  });

  it("does the same for the flat, wrapper-less form recorded mocks use", () => {
    const source = [{ statusCode: "204" }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(projected.items[0]?.statusText).toBe(true);
    expect(JSON.stringify(renderResponses(projected.items))).toBe(JSON.stringify(source));
  });

  it("keeps the numeric spelling numeric, so a hand-written stub is not rewritten either", () => {
    const source = [{ is: { statusCode: 201 } }];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(projected.items[0]?.statusText).toBe(false);
    expect(JSON.stringify(renderResponses(projected.items))).toBe(JSON.stringify(source));
  });

  it("refuses a numeric string the engine could never have emitted", () => {
    /*
     * `u16::to_string()` produces digits with no sign, no padding, no leading zero. A `"0200"` is
     * legal input to the engine but the model holds a number, so accepting it would rewrite it to
     * `"200"` on the first save — the silent rewrite this whole module exists to prevent. Refusing
     * costs a hand-written document a trip through the raw editor and costs a real one nothing.
     */
    for (const statusCode of ["0200", "+200", "20x", "", " 200", "200 ", "2e2"]) {
      const projected = projectResponses({ responses: [{ is: { statusCode } }] });
      expect([statusCode, projected.kind]).toEqual([statusCode, "rawOnly"]);
      if (projected.kind !== "rawOnly") continue;
      expect(projected.unmodelledKeys).toEqual(["responses[0].is.statusCode"]);
    }
  });

  it("round-trips the wire shape the conformance corpus pins", () => {
    // `vendor/rift/sdk-conformance/corpus/imposters/20-migration-compat.json` carries the string
    // form; that corpus is why the engine's serialization cannot simply be changed instead.
    const source = [
      { is: { statusCode: "200", headers: { "Content-Type": "application/json" }, body: '{"ok":true}' } },
    ];
    const projected = projectResponses({ responses: source });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    expect(JSON.stringify(renderResponses(projected.items))).toBe(JSON.stringify(source));
  });

  it("keeps the arrival spelling when the operator edits the status", () => {
    /*
     * The flag survives an edit because `ResponseBuilder` updates the model with a spread. An
     * operator changing 200 to 201 on a stub read from the API gets `"201"` back, not `201` — the
     * response is re-emitted in its own spelling rather than silently converted.
     */
    const projected = projectResponses({ responses: [{ is: { statusCode: "200" } }] });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    const [item] = projected.items;
    if (item === undefined) throw new Error("expected one response");
    expect(renderResponses([{ ...item, statusCode: 201 }])).toEqual([{ is: { statusCode: "201" } }]);
  });
});

describe("#257 — render and parse agree about what a string status code can be", () => {
  /*
   * The invariant: `renderIsBody` must never emit a `statusCode` that `parseIsBody` would refuse.
   * Break it and the form ejects to raw-only mid-edit — on exactly the engine-read stubs #257
   * exists to make editable, while the identical keystroke on a number-spelled stub stays put.
   */
  it("falls back to the number spelling for a value no canonical string can express", () => {
    const projected = projectResponses({ responses: [{ is: { statusCode: "200" } }] });
    if (projected.kind !== "responses") throw new Error("expected a response list");
    const [item] = projected.items;
    if (item === undefined) throw new Error("expected one response");

    // Each of these is typeable in the status field, and none is a canonical u16 spelling.
    for (const [statusCode, expected] of [
      [200.5, 200.5],
      [-1, -1],
      [1e21, 1e21],
    ] as const) {
      const rendered = renderResponses([{ ...item, statusCode }]) as { is: { statusCode: unknown } }[];
      expect([statusCode, rendered[0]?.is.statusCode]).toEqual([statusCode, expected]);
    }

    // And a value that CAN be spelled canonically still keeps the string spelling.
    expect(renderResponses([{ ...item, statusCode: 201 }])).toEqual([{ is: { statusCode: "201" } }]);
  });

  it("never renders a status the projection would then refuse", () => {
    // Stated as a property over the number, because that is the domain the status input admits.
    fc.assert(
      fc.property(
        fc.oneof(fc.integer(), fc.double({ noNaN: true, noDefaultInfinity: true })),
        (statusCode) => {
          const model = { ...blankResponse(), statusCode, statusText: true };
          const rendered = renderResponses([model]);
          expect(projectResponses({ responses: rendered }).kind).toBe("responses");
        },
      ),
    );
  });
});
