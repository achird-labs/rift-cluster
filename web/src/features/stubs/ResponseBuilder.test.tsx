/** @vitest-environment jsdom */
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { type ReactNode, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ResponseBuilder, describeResponseList } from "./ResponseBuilder.tsx";
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
