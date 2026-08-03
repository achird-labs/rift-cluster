/** @vitest-environment jsdom */
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { type ReactNode, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ResponseBuilder, describeResponseList } from "./ResponseBuilder.tsx";
import { FAULT_KINDS } from "./behaviors.ts";
import { type ResponseModel, blankResponse, renderResponses } from "./responses.ts";

afterEach(cleanup);

function response(statusCode: number, over: Partial<ResponseModel> = {}): ResponseModel {
  return { ...blankResponse(), statusCode, ...over };
}

/**
 * Render the builder the way `StubEditor` actually drives it — controlled, with each proposed list
 * fed straight back in as the next `items`.
 *
 * The naive harness (a bare `vi.fn` and a fixed `items`) silently makes multi-character typing
 * untestable: every keystroke re-renders the inputs from the ORIGINAL props, so `user.type` of
 * "hello" asserts on a model that only ever saw "o". That failure looks like a component bug and is
 * really a harness bug, so the feedback loop is part of the harness.
 */
function mount(initial: ResponseModel[]) {
  const onChange = vi.fn<(items: ResponseModel[]) => void>();
  function Harness(): ReactNode {
    const [items, setItems] = useState(initial);
    return (
      <ResponseBuilder
        items={items}
        onChange={(next) => {
          onChange(next);
          setItems(next);
        }}
      />
    );
  }
  render(<Harness />);
  return { onChange, last: () => onChange.mock.calls.at(-1)?.[0] };
}

describe("AC1 — N responses, add / remove / reorder, and the cycling order is stated", () => {
  it("appends a response without disturbing the ones already there", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    await user.click(screen.getByRole("button", { name: "Add response" }));

    expect(last()?.map((item) => item.statusCode)).toEqual([200, 200]);
  });

  it("removes the response the operator pointed at, not the last one", async () => {
    // The off-by-one that positional list editors are famous for. Removing the FIRST of three and
    // getting [200, 404] back would look plausible and be wrong.
    const user = userEvent.setup();
    const { last } = mount([response(200), response(404), response(500)]);

    await user.click(screen.getByRole("button", { name: "Remove response 1" }));

    expect(last()?.map((item) => item.statusCode)).toEqual([404, 500]);
  });

  it("reorders responses, because the order IS the cycling order", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(202), response(200)]);

    await user.click(screen.getByRole("button", { name: "Move response 2 up" }));

    expect(last()?.map((item) => item.statusCode)).toEqual([200, 202]);
  });

  it("cannot move the first response up or the last one down", () => {
    mount([response(202), response(200)]);
    const up = screen.getByRole("button", { name: "Move response 1 up" }) as HTMLButtonElement;
    const down = screen.getByRole("button", { name: "Move response 2 down" }) as HTMLButtonElement;
    expect([up.disabled, down.disabled]).toEqual([true, true]);
  });

  it("states the cycling semantics once there is more than one response, and not before", () => {
    // The behaviour is non-obvious — a second response silently changes what the stub does on the
    // SECOND call — so the note is part of the feature, not decoration.
    const { unmount } = render(<ResponseBuilder items={[response(200)]} onChange={vi.fn()} />);
    expect(screen.queryByTestId("response-cycling-note")).toBeNull();
    unmount();

    render(<ResponseBuilder items={[response(202), response(200)]} onChange={vi.fn()} />);
    expect(screen.getByTestId("response-cycling-note").textContent).toMatch(/cycle/i);
  });
});

describe("the status code field", () => {
  it("edits the status code, and an empty box means the key is absent", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    const status = screen.getByLabelText("Status code for response 1");
    await user.clear(status);
    expect(last()?.[0]?.statusCode).toBeNull();

    await user.type(status, "404");
    expect(last()?.[0]?.statusCode).toBe(404);
  });

  it("refuses a non-finite status rather than serializing it as JSON null", async () => {
    // `Number("1e999")` is `Infinity`, which survives an isNaN guard and stringifies to `null` —
    // flipping the whole stub to raw-only on something the operator can genuinely type.
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    const status = screen.getByLabelText("Status code for response 1");
    await user.clear(status);
    await user.type(status, "1e999");

    expect(last()?.[0]?.statusCode).toBeNull();
  });
});

describe("the empty list", () => {
  it("says what an empty response list means instead of claiming a status", () => {
    // The engine has nothing to answer with when `responses` is empty — it is not a 200.
    mount([]);
    expect(screen.getByTestId("response-builder").textContent).toMatch(/no responses/i);
    expect(screen.getByTestId("response-builder").textContent).not.toMatch(/\b200\b/);
  });

  it("appends a response that matches its neighbour's wrapper shape", async () => {
    // A flat, recorded stub should not acquire an `is`-wrapped response beside its flat ones: that
    // mixed document is a diff on export (#251) that the operator did not ask for.
    const user = userEvent.setup();
    const { last } = mount([response(200, { wrapped: false })]);

    await user.click(screen.getByRole("button", { name: "Add response" }));

    expect(last()?.map((item) => item.wrapped)).toEqual([false, false]);
  });
});

describe("AC2 — arbitrary headers, with Content-Type no longer special-cased", () => {
  it("adds a header row and writes it into the response it belongs to", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200), response(500)]);

    await user.click(screen.getByRole("button", { name: "Add header to response 2" }));
    await user.type(screen.getByRole("textbox", { name: "Header 1 name for response 2" }), "Location");
    await user.type(screen.getByRole("textbox", { name: "Header 1 value for response 2" }), "/there");

    const items = last();
    expect(items?.[0]?.headers).toEqual([]);
    expect(items?.[1]?.headers).toEqual([{ name: "Location", value: "/there", multi: false }]);
  });

  it("removes one header row and leaves its siblings alone", async () => {
    const user = userEvent.setup();
    const { last } = mount([
      response(200, {
        headers: [
          { name: "Content-Type", value: "application/json", multi: false },
          { name: "X-Trace", value: "1", multi: false },
        ],
      }),
    ]);

    await user.click(screen.getByRole("button", { name: "Remove header 1 from response 1" }));

    expect(last()?.[0]?.headers).toEqual([{ name: "X-Trace", value: "1", multi: false }]);
  });

  it("does not rewrite an untouched header when a second, still-unnamed row is added", async () => {
    /*
     * The collision latch. The document is re-rendered and re-projected on every keystroke, so two
     * rows sharing a name become one JSON array — correct, that IS a multi-value header — and
     * re-projection then marks both rows `multi`. Clicking "Add header" gives the new row an empty
     * name, so without care two clicks put two rows under `""`, and separating them again leaves
     * the *pre-existing, untouched* header permanently rewritten from `"1"` to `["1"]`.
     */
    const user = userEvent.setup();
    const { last } = mount([
      response(200, { headers: [{ name: "A", value: "1", multi: false }] }),
    ]);

    await user.click(screen.getByRole("button", { name: "Add header to response 1" }));
    await user.click(screen.getByRole("button", { name: "Add header to response 1" }));

    const rendered = renderResponses(last() ?? []) as { is: { headers: Record<string, unknown> } }[];
    expect(rendered[0]?.is.headers).toEqual({ A: "1" });
  });

  it("edits a Content-Type header through the same rows as any other header", async () => {
    // The whole point of AC2: `Content-Type` is a header, not a form field with a box of its own.
    const user = userEvent.setup();
    const { last } = mount([
      response(200, { headers: [{ name: "Content-Type", value: "text/plain", multi: false }] }),
    ]);

    const value = screen.getByRole("textbox", { name: "Header 1 value for response 1" });
    await user.clear(value);
    await user.type(value, "application/xml");

    expect(last()?.[0]?.headers).toEqual([
      { name: "Content-Type", value: "application/xml", multi: false },
    ]);
  });
});

describe("AC3 — a JSON body is edited as JSON and written back as a JSON value", () => {
  it("keeps an object body an object rather than stringifying it", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200, { body: { kind: "json", value: { ok: true } } })]);

    const body = screen.getByRole("textbox", { name: "Body for response 1" });
    await user.clear(body);
    await user.type(body, '{{"ok":false}');

    expect(last()?.[0]?.body).toEqual({ kind: "json", value: { ok: false } });
    // And it reaches the document as a value, not as a quoted blob.
    const rendered = renderResponses(last() ?? []) as { is: { body: unknown } }[];
    expect(rendered[0]?.is.body).toEqual({ ok: false });
  });

  it("says so, and does not touch the model, while the JSON is mid-edit and invalid", async () => {
    // Half-typed JSON is the normal state of a text box. Writing `{"ok":` back as a *string* body
    // would silently change what the mock returns; refusing to parse and saying so keeps the last
    // good value in the document until the operator finishes.
    const user = userEvent.setup();
    const { onChange } = mount([response(200, { body: { kind: "json", value: { ok: true } } })]);

    const body = screen.getByRole("textbox", { name: "Body for response 1" });
    await user.clear(body);
    await user.type(body, '{{"ok":');

    expect(screen.getByTestId("response-body-error-0")).toBeTruthy();
    // Nothing unparseable is ever proposed — not as a `json` body it cannot represent, and above all
    // not as a `text` body, which would silently change what the mock returns from an object to a
    // string. Asserting over EVERY call, not just the last: one bad proposal mid-sequence is enough
    // to corrupt the document even if a later call puts it back.
    for (const [items] of onChange.mock.calls) {
      expect(items[0]?.body).toEqual({ kind: "json", value: { ok: true } });
    }
  });

  it("survives a half-typed object when the editor round-trips the document, as it really does", async () => {
    /*
     * The regression that the plain harness above structurally cannot see.
     *
     * `StubEditor` does not hand the model straight back: every edit is re-rendered to JSON text and
     * re-parsed, so the `value` this field receives is a STRUCTURALLY equal but REFERENTIALLY new
     * object on every keystroke. A resync keyed on `value !== lastValue` therefore fires constantly,
     * throwing away the operator's in-progress text the instant it stops parsing — the exact thing
     * the local text state exists to protect. Identity is not a usable change signal here.
     */
    const user = userEvent.setup();
    const onChange = vi.fn<(items: ResponseModel[]) => void>();
    function RoundTripHarness(): ReactNode {
      const [items, setItems] = useState<ResponseModel[]>([
        response(200, { body: { kind: "json", value: { ok: true } } }),
      ]);
      return (
        <ResponseBuilder
          items={items}
          onChange={(next) => {
            onChange(next);
            // Exactly what `composeStubText` -> `setText` -> `projectResponses` does to the model.
            setItems(JSON.parse(JSON.stringify(next)) as ResponseModel[]);
          }}
        />
      );
    }
    render(<RoundTripHarness />);

    const body = screen.getByRole("textbox", { name: "Body for response 1" }) as HTMLTextAreaElement;
    await user.clear(body);
    await user.type(body, '{{"ok":');

    expect(body.value).toBe('{"ok":');
    expect(screen.getByTestId("response-body-error-0")).toBeTruthy();
  });

  it("switches a body between absent, text and JSON without inventing content", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(204)]);

    await user.selectOptions(screen.getByRole("combobox", { name: "Body type for response 1" }), "text");
    expect(last()?.[0]?.body).toEqual({ kind: "text", text: "" });
  });

  it("edits a text body as text, leaving it a string", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200, { body: { kind: "text", text: "" } })]);

    await user.type(screen.getByRole("textbox", { name: "Body for response 1" }), "hello");

    expect(last()?.[0]?.body).toEqual({ kind: "text", text: "hello" });
  });
});

describe("#249 — the latency & faults panel", () => {
  it("builds a fixed delay on a response that had no behaviours key at all", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    await user.selectOptions(screen.getByLabelText("Delay for response 1"), "fixed");
    const ms = screen.getByLabelText("Delay milliseconds for response 1");
    await user.clear(ms);
    await user.type(ms, "250");

    expect(last()?.[0]?.behaviors).toEqual({
      spelling: "_behaviors",
      order: ["wait"],
      wait: { kind: "fixed", ms: 250 },
      repeat: null,
    });
    // `order` must never name a key whose value is absent, or the round-trip breaks.
    expect(renderResponses(last() ?? [])).toEqual([{ is: { statusCode: 200 }, _behaviors: { wait: 250 } }]);
  });

  it("builds a random range", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    await user.selectOptions(screen.getByLabelText("Delay for response 1"), "range");
    const min = screen.getByLabelText("Minimum delay milliseconds for response 1");
    const max = screen.getByLabelText("Maximum delay milliseconds for response 1");
    await user.clear(min);
    await user.type(min, "10");
    await user.clear(max);
    await user.type(max, "100");

    expect(last()?.[0]?.behaviors?.wait).toEqual({ kind: "range", min: 10, max: 100 });
  });

  it("removes the behaviours key entirely when the last behaviour is cleared", async () => {
    // Leaving `_behaviors: {}` behind would put a key in the document that the operator never
    // wrote and cannot see — and that they would then find in an export.
    const user = userEvent.setup();
    const { last } = mount([
      response(200, {
        behaviors: { spelling: "_behaviors", order: ["wait"], wait: { kind: "fixed", ms: 5 }, repeat: null },
      }),
    ]);

    await user.selectOptions(screen.getByLabelText("Delay for response 1"), "none");

    expect(last()?.[0]?.behaviors).toBeNull();
    expect(renderResponses(last() ?? [])).toEqual([{ is: { statusCode: 200 } }]);
  });

  it("keeps the behaviours key when a repeat survives the delay being cleared", async () => {
    const user = userEvent.setup();
    const { last } = mount([
      response(200, {
        behaviors: { spelling: "_behaviors", order: ["wait", "repeat"], wait: { kind: "fixed", ms: 5 }, repeat: 3 },
      }),
    ]);

    await user.selectOptions(screen.getByLabelText("Delay for response 1"), "none");

    expect(last()?.[0]?.behaviors?.repeat).toBe(3);
    expect(last()?.[0]?.behaviors?.order).toEqual(["repeat"]);
    expect(renderResponses(last() ?? [])).toEqual([{ is: { statusCode: 200 }, _behaviors: { repeat: 3 } }]);
  });

  it("offers every fault kind the engine defines, by its canonical name", () => {
    mount([response(200)]);
    const select = screen.getByLabelText("Fault for response 1") as HTMLSelectElement;
    const values = [...select.options].map((option) => option.value);
    expect(values).toEqual(["", ...FAULT_KINDS]);
  });

  it("selects a fault and says, visibly, that it REPLACES the response (AC4)", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200)]);

    expect(screen.queryByTestId("response-fault-warning-0")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Fault for response 1"), "EMPTY_RESPONSE");

    /*
     * `riftString`, NOT the top-level `fault` key. The engine dispatches `is > proxy > inject >
     * fault`, so a top-level fault beside a body never fires and is dropped on the next read;
     * `_rift.fault.tcp` is handed to `new_is(..., raw.rift)` and does fire. Writing the dead form
     * here would have configured nothing while the banner claimed the response was replaced.
     */
    expect(last()?.[0]?.fault).toEqual({ form: "riftString", kind: "EMPTY_RESPONSE" });
    // Visible text, not a tooltip: a fault silently discarding the status/headers/body above it is
    // the single most surprising thing this panel can do.
    const warning = screen.getByTestId("response-fault-warning-0");
    expect(warning.textContent).toMatch(/instead of|replaces/i);
  });

  it("switches to the probabilistic form when a probability is given, and back when cleared", async () => {
    const user = userEvent.setup();
    const { last } = mount([response(200, { fault: { form: "responseKey", kind: "EMPTY_RESPONSE" } })]);

    const probability = screen.getByLabelText("Fault probability for response 1");
    await user.type(probability, "0.25");
    expect(last()?.[0]?.fault).toEqual({
      form: "riftObject",
      kind: "EMPTY_RESPONSE",
      probability: 0.25,
    });

    // Clearing lands back on `riftString`, the form that still fires beside a body — not on the
    // top-level key, which would silently switch the fault off.
    await user.clear(probability);
    expect(last()?.[0]?.fault).toEqual({ form: "riftString", kind: "EMPTY_RESPONSE" });
  });

  it("writes the top-level fault key on a FLAT response, even when it has a body", async () => {
    /*
     * The flat form is the recorded-imposter form, and there the engine's dispatch inverts: it tests
     * `raw.rift` BEFORE the flat statusCode/body branch, so a flat response carrying `_rift` becomes
     * a RiftScript — the fault never fires and the status and body are erased on the next read. The
     * top-level `fault` key is the one that works here. A predicate keyed on "has a body" got every
     * recorded stub backwards.
     */
    const user = userEvent.setup();
    const { last } = mount([response(201, { wrapped: false, body: { kind: "text", text: "hi" } })]);

    await user.selectOptions(screen.getByLabelText("Fault for response 1"), "EMPTY_RESPONSE");

    expect(last()?.[0]?.fault).toEqual({ form: "responseKey", kind: "EMPTY_RESPONSE" });
    expect(renderResponses(last() ?? [])).toEqual([
      { statusCode: 201, body: "hi", fault: "EMPTY_RESPONSE" },
    ]);
  });

  it("does not tell the operator to rewrite a flat response's WORKING fault", async () => {
    // The old warning fired on exactly this document and instructed the operator to switch a
    // firing fault into the inert `_rift` form.
    mount([
      response(201, {
        wrapped: false,
        body: { kind: "text", text: "hi" },
        fault: { form: "responseKey", kind: "EMPTY_RESPONSE" },
      }),
    ]);

    const warning = screen.getByTestId("response-fault-warning-0");
    expect(warning.textContent).not.toMatch(/never fires/i);
    expect(warning.textContent).toMatch(/instead of the status/i);
  });

  it("does not downgrade a working _rift fault when only the kind is changed", async () => {
    // The regression that destroys config that was already correct: rewriting a firing
    // `_rift.fault.tcp` into the dead top-level key just because the operator picked another kind.
    const user = userEvent.setup();
    const { last } = mount([
      response(200, { fault: { form: "riftObject", kind: "EMPTY_RESPONSE", probability: 0.5 } }),
    ]);

    await user.selectOptions(screen.getByLabelText("Fault for response 1"), "CONNECTION_RESET_BY_PEER");

    expect(last()?.[0]?.fault).toEqual({
      form: "riftObject",
      kind: "CONNECTION_RESET_BY_PEER",
      probability: 0.5,
    });
  });

  it("says a hand-authored top-level fault beside a body never fires, rather than calling it armed", async () => {
    /*
     * The picker never writes this shape, but a hand-written stub can arrive in it, and it is the
     * engine's sharpest footgun here: the fault is inert AND disappears on the next read. Claiming
     * "this replaces the response" about it would be actively misleading.
     */
    mount([response(200, { fault: { form: "responseKey", kind: "EMPTY_RESPONSE" } })]);

    const warning = screen.getByTestId("response-fault-warning-0");
    expect(warning.textContent).toMatch(/never fires/i);
    expect(warning.textContent).not.toMatch(/instead of the status/i);
  });

  it("refuses a probability outside 0..1 rather than writing a document it cannot read back", async () => {
    /*
     * The input's `min`/`max` attributes do not stop the operator typing `1.5`. The engine refuses
     * such a probability and so does `parseRiftTcpFault`, so writing it would produce a document
     * this very form can no longer project — the panel would vanish into raw-only mid-keystroke.
     */
    const user = userEvent.setup();
    const { onChange } = mount([
      response(200, { fault: { form: "responseKey", kind: "EMPTY_RESPONSE" } }),
    ]);

    // `5`, not `1.5`: typing is keystroke by keystroke, and `1.5` passes through `1` — which is a
    // perfectly valid probability that the field is right to accept. A value with no valid prefix
    // is the only one that actually exercises the guard.
    await user.type(screen.getByLabelText("Fault probability for response 1"), "5");

    // Nothing is proposed at all — not a corrected value, not the old one. Asserting over every
    // call rather than the last: one bad proposal mid-sequence is enough to break the document.
    expect(onChange.mock.calls).toEqual([]);
  });

  it("shows an aliased fault kind under its canonical name without rewriting the document", async () => {
    // `reset` is a spelling the engine accepts. The picker shows the canonical name so the operator
    // knows what it is, but the document keeps its own spelling until they actually change it.
    const { last } = mount([response(200, { fault: { form: "responseKey", kind: "reset" } })]);

    const select = screen.getByLabelText("Fault for response 1") as HTMLSelectElement;
    expect(select.value).toBe("CONNECTION_RESET_BY_PEER");
    expect(last()).toBeUndefined();
  });

  it("is collapsed by default, so a plain stub stays plain", () => {
    mount([response(200)]);
    const panel = screen.getByTestId("response-chaos-0") as HTMLDetailsElement;
    expect(panel.open).toBe(false);
  });
});

describe("the one-line summary of a response list (AC7 feeds on this)", () => {
  it("reports a single response by its effect", () => {
    expect(describeResponseList([response(201)])).toBe("answers 201");
  });

  it("reports the count and the cycling when there is more than one", () => {
    expect(describeResponseList([response(202), response(200), response(500)])).toBe(
      "answers 202, then cycles through 2 more",
    );
  });

  it("says what a response-less stub does rather than claiming a status it has not got", () => {
    expect(describeResponseList([])).toBe("carries no responses");
  });
});
