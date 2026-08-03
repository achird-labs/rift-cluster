/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import { focusManager } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { REVISION_HEADER } from "../api/client.ts";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { StubEditor } from "../screens/StubEditor.tsx";
import { renderInApp, whoamiWith } from "./harness.tsx";

const PORT = 4545;
const BY_ID = (id: string): string => `/imposters/${PORT}/stubs/by-id/${id}`;

/** A stub the form models completely. */
const MODELLED = {
  id: "s-1",
  predicates: [{ equals: { method: "GET", path: "/users" } }],
  responses: [{ is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "[]" } }],
};

/** A stub the form cannot hold: `space`, `scenarioName` and `behaviors` have no form controls. */
const UNMODELLED = {
  id: "s-2",
  space: "tenant-a",
  scenarioName: "checkout",
  behaviors: [{ wait: 50 }],
  responses: [{ is: { statusCode: 204 } }],
};

/** A stub with no `id` at all — nothing the by-id routes can address. */
const IDLESS = { predicates: [{ equals: { path: "/anon" } }], responses: [{ is: { statusCode: 200 } }] };

function imposter(stubs: unknown[]): Record<string, unknown> {
  return {
    port: PORT,
    host: "0.0.0.0",
    protocol: "http",
    name: "billing",
    recordRequests: false,
    enabled: true,
    stubs,
  };
}

type Call = { method: string; path: string; body: string | undefined; headers: Record<string, string> };

/**
 * A fetch double whose answers can change between calls — the only way to model a second editor
 * committing underneath this one, and the only way to assert which revision each write quoted.
 */
function stubFleet(replies: {
  read: () => { json: unknown; revision: string | null };
  write?: (call: Call) => { status: number; json: unknown };
  /** Reply for `GET /_fleet/ops/{id}` polls; without it those GETs fall through to `read`. */
  op?: () => { status: number; json?: unknown };
}): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      const call: Call = {
        method: init?.method ?? "GET",
        path,
        body: typeof init?.body === "string" ? init.body : undefined,
        headers: (init?.headers ?? {}) as Record<string, string>,
      };
      calls.push(call);
      if (call.method === "GET" && path.startsWith("/_fleet/ops/") && replies.op) {
        const reply = replies.op();
        return Promise.resolve(
          new Response(reply.json === undefined ? "" : JSON.stringify(reply.json), {
            status: reply.status,
          }),
        );
      }
      if (call.method === "GET") {
        const { json, revision } = replies.read();
        return Promise.resolve(
          new Response(JSON.stringify(json), {
            status: 200,
            headers: revision === null ? {} : { [REVISION_HEADER]: revision },
          }),
        );
      }
      const reply = replies.write?.(call) ?? { status: 200, json: replies.read().json };
      return Promise.resolve(new Response(JSON.stringify(reply.json), { status: reply.status }));
    }),
  );
  return calls;
}

/** Open the editor for one stub and wait for its JSON to be on screen. */
async function openEditor(stubId: string): Promise<HTMLTextAreaElement> {
  await userEvent.setup().click(await screen.findByRole("button", { name: `Edit ${stubId}` }));
  const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
  await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
  return editor;
}

async function retype(editor: HTMLTextAreaElement, text: string): Promise<void> {
  const user = userEvent.setup();
  await user.clear(editor);
  // `paste` rather than `type`: monaco's fallback is a plain textarea, and typing a JSON document
  // character by character would fire a parse of every prefix — slow, and not what a paste does.
  await user.click(editor);
  await user.paste(text);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AC2 — a stub the form cannot hold is raw-only, and its bytes survive the round trip", () => {
  it("names every unmodelled key rather than showing a form with holes in it", async () => {
    stubFleet({ read: () => ({ json: imposter([UNMODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-2");

    const banner = await screen.findByTestId("stub-raw-banner");
    expect(banner.textContent).toContain("space");
    expect(banner.textContent).toContain("scenarioName");
    expect(banner.textContent).toContain("behaviors[0].wait");
    // No form at all — a partly-filled one is what would silently drop those keys on save.
    expect(screen.queryByTestId("stub-form")).toBeNull();
  });

  it("sends the operator's own bytes, not a reserialization of them", async () => {
    const calls = stubFleet({ read: () => ({ json: imposter([UNMODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-2");

    // Key order and whitespace an operator chose, which a parse-and-restringify would destroy.
    const authored = '{"id":"s-2","space":"tenant-a","responses":[{"is":{"statusCode":204}}]}';
    await retype(editor, authored);
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    expect(calls.find((c) => c.method === "PUT")?.body).toBe(authored);
  });
});

describe("AC4 — the editor's JSON view and form view stay one document", () => {
  it("shows the form and the JSON side by side for a stub the model covers", async () => {
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    expect(screen.getByTestId("stub-form")).toBeTruthy();
    expect(screen.queryByTestId("stub-raw-banner")).toBeNull();
    expect(JSON.parse(editor.value)).toEqual(MODELLED);
    // `MODELLED`'s predicate is `{equals: {method, path}}` — the two-field-in-one-object shape this
    // console has always written (#247's load-bearing read case). That is a *read* shape, not one
    // the row editor's single-entry rows construct, so the builder shows it as a fixed summary
    // rather than a `Path` input this form does not have anymore.
    expect(screen.getByTestId("predicate-builder")).toBeTruthy();
    expect(screen.getByTestId("predicate-row-readonly").textContent).toContain('Path equals "/users"');
  });

  it("opens raw-only and names the predicate it could not model", async () => {
    /*
     * The regression test #247 asks for by name. `projectPredicates` refusing is well covered on
     * its own, but this is the *composition*: the editor must require both projections and put the
     * predicate projection's key into the same banner the form projection's keys go to. Without
     * this, a refusal could be computed correctly and then dropped on the floor, and the operator
     * would get a form for a stub the builder cannot represent — which is the silent-rewrite
     * failure the whole module exists to prevent.
     */
    const unrepresentable = {
      id: "s-1",
      predicates: [{ soundsLike: { path: "/users" } }],
      responses: [{ is: { statusCode: 200 } }],
    };
    stubFleet({ read: () => ({ json: imposter([unrepresentable]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    const banner = await screen.findByTestId("stub-raw-banner");
    expect(banner.textContent).toMatch(/predicates\[0\]|soundsLike/);
    // And no form — a half-form over a stub with an unmodelled predicate is the shape that saves
    // the parts it understood and drops the rest.
    expect(screen.queryByTestId("stub-form")).toBeNull();
    expect(screen.queryByTestId("predicate-builder")).toBeNull();
  });

  it("warns that a stub with no predicates matches everything, and stops once one exists", async () => {
    // The catch-all warning is the one thing `Summary` must keep saying after #247 moved predicates
    // out of `STUB_FIELDS` — it used to key off the method/path fields, which no longer exist.
    stubFleet({
      read: () => ({
        json: imposter([{ id: "s-1", responses: [{ is: { statusCode: 200 } }] }]),
        revision: "default:4545@7",
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    expect((await screen.findByTestId("stub-summary")).textContent).toMatch(
      /matches every|no predicates/i,
    );

    await userEvent.click(screen.getByRole("button", { name: /add predicate/i }));
    expect(screen.getByTestId("stub-summary").textContent).not.toMatch(/matches every request/i);
  });

  it("reports the response count and the cycling in the summary (#248 AC7)", async () => {
    // A cycling stub is the case the old summary could not describe at all: it reported
    // `responses[0]`'s status as though it were the whole story, so "202 then 200" read as a
    // constant 202 — the console actively misdescribing what the mock does on the second call.
    stubFleet({
      read: () => ({
        json: imposter([
          {
            id: "s-1",
            predicates: [{ equals: { path: "/orders" } }],
            responses: [{ is: { statusCode: 202 } }, { is: { statusCode: 200 } }],
          },
        ]),
        revision: "default:4545@7",
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    expect((await screen.findByTestId("stub-summary")).textContent).toMatch(
      /answers 202, then cycles through 1 more/i,
    );
  });

  it("labels a proxy response it cannot edit, instead of only naming the key (#248 AC5)", async () => {
    // "Recognised, not editable." The stub opens raw-only — correct, a form must not pretend to
    // edit a proxy rule — but the operator still has to be able to see that the stub HAS a proxy
    // and where it points without reading the JSON themselves.
    stubFleet({
      read: () => ({
        json: imposter([
          { id: "s-1", responses: [{ is: { statusCode: 200 } }, { proxy: { to: "http://api.example.com" } }] },
        ]),
        revision: "default:4545@7",
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    expect((await screen.findByTestId("stub-raw-banner")).textContent).toContain("responses[1].proxy");
    const labels = screen.getByTestId("stub-response-labels").textContent ?? "";
    expect(labels).toContain("proxy");
    expect(labels).toContain("http://api.example.com");
    expect(screen.queryByTestId("stub-form")).toBeNull();
  });

  it("moves an edit made in the response builder into the JSON, as a JSON body not a string", async () => {
    // The #248 counterpart of the predicate-builder test below. The body assertion is the load
    // bearing half: writing `{"ok":false}` back as a *string* would change what the mock returns.
    stubFleet({
      read: () => ({
        json: imposter([
          {
            id: "s-1",
            predicates: [{ equals: { path: "/users" } }],
            responses: [{ is: { statusCode: 200, body: { ok: true } } }],
          },
        ]),
        revision: "default:4545@7",
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    const user = userEvent.setup();
    const body = screen.getByRole("textbox", { name: "Body for response 1" });
    await user.clear(body);
    await user.type(body, '{{"ok":false}');

    await waitFor(() => expect(JSON.parse(editor.value).responses[0].is.body).toEqual({ ok: false }));
  });

  it("moves an edit made in the predicate builder into the JSON", async () => {
    // A single-entry predicate — the shape the builder's own rows write — unlike `MODELLED` above,
    // so this exercises the row the builder actually renders as an editable field.
    const singleEntry = {
      id: "s-1",
      predicates: [{ equals: { path: "/users" } }],
      responses: [{ is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "[]" } }],
    };
    stubFleet({ read: () => ({ json: imposter([singleEntry]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    const user = userEvent.setup();
    await user.clear(screen.getByLabelText("Value"));
    await user.type(screen.getByLabelText("Value"), "/orders");

    await waitFor(() => expect(JSON.parse(editor.value).predicates[0].equals.path).toBe("/orders"));
  });

  it("keeps rendering when `responses` is not an array, instead of throwing mid-edit", async () => {
    /*
     * A document the operator passes THROUGH while typing: `{"responses":{}}` is one keystroke away
     * from `{"responses":[]}`. Every other reader copes with it — `projectResponses` returns
     * rawOnly naming `responses`, `describeResponses` returns [] — but the per-response
     * "also runs" labels read the array with a bare cast and threw during render, taking the whole
     * editor down. `send()` already guards its own read of `parsed.value` for exactly this reason.
     */
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    for (const document of ['{"id":"s-1","responses":{}}', '{"id":"s-1","responses":"x"}', "null"]) {
      await retype(editor, document);
      // Still on screen, and still telling the operator why the form is unavailable.
      expect(await screen.findByTestId("stub-raw-banner")).toBeTruthy();
    }
  });

  it("drops to raw-only when a JSON edit introduces a key the form cannot hold", async () => {
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    await retype(editor, '{"id":"s-1","behaviors":[{"wait":50}]}');

    expect((await screen.findByTestId("stub-raw-banner")).textContent).toContain("behaviors[0].wait");
    expect(screen.queryByTestId("stub-form")).toBeNull();
  });

  it("refuses to save text that is not JSON, saying so rather than sending it", async () => {
    const calls = stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    await retype(editor, "{ not json");

    expect((await screen.findByTestId("stub-json-error")).textContent).toMatch(/not valid json/i);
    expect((screen.getByRole("button", { name: /save stub/i }) as HTMLButtonElement).disabled).toBe(true);
    expect(calls.some((c) => c.method !== "GET")).toBe(false);
  });
});

describe("AC5 — the lint pane is advisory and the server's refusal is the authority", () => {
  it("says the linter is unavailable rather than showing an empty, reassuring finding list", async () => {
    // No wasm artifact on a dev/test build. "No findings" here would read as a clean bill from a
    // linter that never ran.
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    expect((await screen.findByTestId("stub-lint")).textContent).toMatch(
      /lint unavailable — the server still validates every save/i,
    );
  });

  it("surfaces a server rejection even when the local lint found nothing wrong", async () => {
    stubFleet({
      read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }),
      write: () => ({ status: 400, json: { message: "predicates[0].equals.method must be a string" } }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    await waitFor(() => expect(screen.getByTestId("stub-lint").textContent).toMatch(/unavailable/i));
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    const error = await screen.findByTestId("stub-server-error");
    expect(error.textContent).toContain("predicates[0].equals.method must be a string");
  });

  it("shows the engine's own fault-probability message verbatim, not a rewritten one (#249 AC3)", async () => {
    /*
     * `RiftTcpFault`'s deserializer is hand-written specifically so a malformed object form gets an
     * actionable message instead of serde's opaque "data did not match any variant". That message
     * is a gift — it names the fix ("use the bare fault-type string for an always-firing fault").
     * Replacing it with a generic "invalid fault" would throw away the only thing that tells the
     * operator what to do, so this pins that the server's text reaches them intact.
     */
    const engineMessage =
      "_rift.fault.tcp object form requires a numeric 'probability' (use the bare fault-type string for an always-firing fault)";
    stubFleet({
      read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }),
      write: () => ({ status: 400, json: { message: engineMessage } }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    expect((await screen.findByTestId("stub-server-error")).textContent).toContain(engineMessage);
  });
});

describe("#250 — a stub seeded from a recorded request", () => {
  it("opens the seed as the draft, and says the response is a default the journal never recorded", async () => {
    /*
     * `RecordedRequest` carries no response field at all, so the seeded 200 is invented by the
     * console. Letting an operator assume it was replayed from the journal would be the console
     * lying about what it knows — hence the line, and hence a test for the line.
     */
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    const seed = {
      predicates: [{ equals: { method: "POST", path: "/orders" } }],
      responses: [{ is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" } }],
    };
    renderInApp(
      <StubEditor port={PORT} target={{ kind: "new", seed }} original={null} revision="default:4545@7" onDone={() => {}} />,
      { whoami: whoamiWith("editor") },
    );

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
    expect(JSON.parse(editor.value)).toEqual(seed);
    expect((await screen.findByTestId("stub-seed-note")).textContent).toMatch(
      /records requests, not responses/i,
    );
  });

  it("offers no presets for a seeded stub — the seed IS the starting point", async () => {
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(
      <StubEditor
        port={PORT}
        target={{ kind: "new", seed: { responses: [{ is: { statusCode: 200 } }] } }}
        original={null}
        revision="default:4545@7"
        onDone={() => {}}
      />,
      { whoami: whoamiWith("editor") },
    );

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-presets")).toBeNull();
  });

  it("still offers presets for an unseeded new stub", async () => {
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(
      <StubEditor port={PORT} target={{ kind: "new" }} original={null} revision="default:4545@7" onDone={() => {}} />,
      { whoami: whoamiWith("editor") },
    );

    expect(await screen.findByTestId("stub-presets")).toBeTruthy();
    expect(screen.queryByTestId("stub-seed-note")).toBeNull();
  });
});

describe("AC7 — a stub body is rendered as text, never as markup", () => {
  it("shows a <script> body inert", async () => {
    const payload = '<script>window.__pwned = 1;<\/script>';
    stubFleet({
      read: () => ({
        json: imposter([{ id: "s-1", responses: [{ is: { statusCode: 200, body: payload } }] }]),
        revision: "default:4545@7",
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    expect(editor.value).toContain("window.__pwned");
    // The editor surface is a textarea and the preview a <pre>; neither parses markup. If either
    // ever did, this document would carry a script element it never had.
    expect(document.querySelector("script")).toBeNull();
    expect((globalThis as unknown as { __pwned?: number }).__pwned).toBeUndefined();
    expect((screen.getByTestId("stub-body-preview") as HTMLElement).textContent).toBe(payload);
  });
});

describe("the write is conditioned on the revision the read handed over", () => {
  it("sends If-Match with the token that came back on the imposter read", async () => {
    const calls = stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe(BY_ID("s-1"));
    expect(put?.headers["If-Match"]).toBe("default:4545@7");
  });

  it("refuses to save at all when the read carried no revision", async () => {
    // An unconditioned save is the lost-update bug wearing a default. Disabling with a stated
    // reason is the only honest option: sending nothing would win every race by discarding the
    // other editor's work.
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: null }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await openEditor("s-1");

    expect((await screen.findByTestId("stub-no-revision")).textContent).toMatch(
      /without it this save could silently overwrite/i,
    );
    expect((screen.getByRole("button", { name: /save stub/i }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("offers no by-id edit for a stub that has no id, and says why", async () => {
    stubFleet({ read: () => ({ json: imposter([IDLESS]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });

    const disabled = (await screen.findByTestId("stub-not-addressable")) as HTMLElement;
    expect(disabled.textContent).toMatch(/no id/i);
    expect((screen.getByRole("button", { name: /^edit$/i }) as HTMLButtonElement).disabled).toBe(true);
  });
});

describe("two concurrent editors — 409 becomes a rebase prompt, never an auto-merge", () => {
  it("names both sides, reapplies against the fresh token, and never merges by itself", async () => {
    const theirs = { ...MODELLED, responses: [{ is: { statusCode: 503, body: "theirs" } }] };
    let committedByOther = false;
    const calls = stubFleet({
      read: () =>
        committedByOther
          ? { json: imposter([theirs]), revision: "default:4545@9" }
          : { json: imposter([MODELLED]), revision: "default:4545@7" },
      write: (call) => {
        // The fleet refuses a write quoting a revision it has moved past.
        if (call.headers["If-Match"] === "default:4545@9") return { status: 200, json: imposter([theirs]) };
        return { status: 409, json: { message: "revision conflict" } };
      },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    const mine = '{"id":"s-1","responses":[{"is":{"statusCode":418,"body":"mine"}}]}';
    await retype(editor, mine);
    committedByOther = true;
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    const panel = await screen.findByTestId("stub-conflict");
    expect(panel.textContent).toMatch(/changed while you were editing/i);
    expect(screen.getByTestId("stub-conflict-mine").textContent).toContain("418");
    expect(screen.getByTestId("stub-conflict-theirs").textContent).toContain("503");
    // Nothing was merged and nothing was retried on its own.
    expect(calls.filter((c) => c.method === "PUT")).toHaveLength(1);

    await userEvent.setup().click(screen.getByRole("button", { name: /reapply my edit/i }));

    await waitFor(() => expect(calls.filter((c) => c.method === "PUT")).toHaveLength(2));
    const writes = calls.filter((c) => c.method === "PUT");
    // The retry quotes the FRESH token — the revision of the state the other editor left behind,
    // read after the conflict — and carries the operator's own edit unchanged.
    expect(writes[0]?.headers["If-Match"]).toBe("default:4545@7");
    expect(writes[1]?.headers["If-Match"]).toBe("default:4545@9");
    expect(writes[1]?.body).toBe(mine);
  });

  it("discards to the other editor's stub, not to the one this editor started from", async () => {
    const theirs = { ...MODELLED, responses: [{ is: { statusCode: 503, body: "theirs" } }] };
    let committedByOther = false;
    stubFleet({
      read: () =>
        committedByOther
          ? { json: imposter([theirs]), revision: "default:4545@9" }
          : { json: imposter([MODELLED]), revision: "default:4545@7" },
      write: () => ({ status: 409, json: { message: "revision conflict" } }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");

    await retype(editor, '{"id":"s-1","responses":[{"is":{"statusCode":418,"body":"mine"}}]}');
    committedByOther = true;
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));
    await screen.findByTestId("stub-conflict");

    await userEvent.setup().click(screen.getByRole("button", { name: /discard my edit/i }));

    await waitFor(() => expect(JSON.parse(editor.value)).toEqual(theirs));
    expect(screen.queryByTestId("stub-conflict")).toBeNull();
  });
});

describe("a parked (202) write through the editor", () => {
  it("closes only once the parked write actually applies", async () => {
    const calls = stubFleet({
      read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }),
      write: () => ({ status: 202, json: { opId: "op-1" } }),
      op: () => ({ status: 200, json: { state: "applied", revision: 8 } }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");
    await retype(editor, '{"id":"s-1","responses":[{"is":{"statusCode":201,"body":"ok"}}]}');
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    await waitFor(() => expect(screen.queryByTestId("stub-editor")).toBeNull());
    expect(calls.some((c) => c.path.startsWith("/_fleet/ops/op-1"))).toBe(true);
  });

  it("routes a parked write refused for a stale token into the rebase prompt, not a raw error", async () => {
    /*
     * Under async admin the precondition is judged inside apply, after the 202 — the refusal
     * arrives as a failed commit whose detail carries the state machine's "revision conflict"
     * prefix. Same refusal as a synchronous 409, same operator decision: it must get the same
     * mine/theirs panel.
     */
    const theirs = { ...MODELLED, responses: [{ is: { statusCode: 503, body: "theirs" } }] };
    let refused = false;
    stubFleet({
      read: () =>
        refused
          ? { json: imposter([theirs]), revision: "default:4545@9" }
          : { json: imposter([MODELLED]), revision: "default:4545@7" },
      write: () => {
        refused = true;
        return { status: 202, json: { opId: "op-2" } };
      },
      op: () => ({
        status: 200,
        json: { state: "failed", revision: 9, detail: "revision conflict: expected revision 7, stored revision 9 on port 4545" },
      }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");
    await retype(editor, '{"id":"s-1","responses":[{"is":{"statusCode":418,"body":"mine"}}]}');
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    const panel = await screen.findByTestId("stub-conflict");
    expect(panel.textContent).toMatch(/changed while you were editing/i);
    expect(screen.getByTestId("stub-conflict-theirs").textContent).toContain("503");
  });
});

describe("the poll must not refresh the token underneath a stale draft", () => {
  /*
   * The failure this pins is the quiet inverse of the conflict flow: the imposter query polls (and
   * refetches on focus — the literal second-tab workflow), so between opening the editor and
   * clicking Save the *token* goes fresh while the *draft* stays stale. A save that closes over the
   * polled token then commits with a valid precondition and silently discards the other editor's
   * write — the exact lost update If-Match exists to catch, with the conflict panel unreachable
   * outside the sub-poll-interval window.
   *
   * The token an editor saves with must be the one it OPENED with; only the 409 re-read advances it.
   */
  it("saves with the token from when the editor opened, not the one the poll fetched", async () => {
    const theirs = { ...MODELLED, responses: [{ is: { statusCode: 503, body: "theirs" } }] };
    let committedByOther = false;
    const calls = stubFleet({
      read: () =>
        committedByOther
          ? { json: imposter([theirs]), revision: "default:4545@9" }
          : { json: imposter([MODELLED]), revision: "default:4545@7" },
      write: (call) => {
        if (call.headers["If-Match"] === "default:4545@9") return { status: 200, json: imposter([theirs]) };
        return { status: 409, json: { message: "revision conflict" } };
      },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    const editor = await openEditor("s-1");
    const mine = '{"id":"s-1","responses":[{"is":{"statusCode":418,"body":"mine"}}]}';
    await retype(editor, mine);

    // The other editor commits, and THIS tab's poll delivers the new state before the click —
    // simulated with the focus refetch, which is the second-tab workflow verbatim.
    committedByOther = true;
    const gets = () => calls.filter((c) => c.method === "GET").length;
    const before = gets();
    focusManager.setFocused(false);
    focusManager.setFocused(true);
    await waitFor(() => expect(gets()).toBeGreaterThan(before));

    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    // The save quoted the OPENING token, so the fleet refused it and the rebase prompt appeared —
    // instead of a fresh-token 200 that would have discarded the other editor's write unseen.
    const writes = calls.filter((c) => c.method === "PUT");
    expect(writes[0]?.headers["If-Match"]).toBe("default:4545@7");
    await screen.findByTestId("stub-conflict");
  });
});

describe("deleting and adding a stub", () => {
  it("deletes by id, conditioned on the revision", async () => {
    const calls = stubFleet({
      read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }),
      write: () => ({ status: 200, json: imposter([]) }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await userEvent.setup().click(await screen.findByRole("button", { name: "Delete s-1" }));

    await waitFor(() => expect(calls.some((c) => c.method === "DELETE")).toBe(true));
    const del = calls.find((c) => c.method === "DELETE");
    expect(del?.path).toBe(BY_ID("s-1"));
    expect(del?.headers["If-Match"]).toBe("default:4545@7");
  });

  it("appends a new stub through the collection route, since a by-id PUT would 404", async () => {
    const calls = stubFleet({
      read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }),
      write: () => ({ status: 200, json: imposter([MODELLED]) }),
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });
    await userEvent.setup().click(await screen.findByRole("button", { name: /add stub/i }));

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await retype(editor, '{"id":"s-new","responses":[{"is":{"statusCode":200}}]}');
    await userEvent.setup().click(screen.getByRole("button", { name: /save stub/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "POST")).toBe(true));
    const post = calls.find((c) => c.method === "POST");
    expect(post?.path).toBe(`/imposters/${PORT}/stubs`);
    expect(post?.headers["If-Match"]).toBe("default:4545@7");
  });

  it("offers no write control at all to a role that may not write", async () => {
    stubFleet({ read: () => ({ json: imposter([MODELLED]), revision: "default:4545@7" }) });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("viewer") });
    await screen.findByTestId("stub-row-0");

    expect(screen.queryByRole("button", { name: /edit/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /add stub/i })).toBeNull();
  });
});
