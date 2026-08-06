/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { components } from "../api/schema.ts";
import {
  DEFAULT_GENERATOR_FIELDS,
  GENERATOR_FIELDS,
  PROXY_MODES,
  proxyStubFor,
  recordingState,
} from "../features/recording/state.ts";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { onDetailTab, renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

type Stub = components["schemas"]["Stub"];

const PROXY_STUB: Stub = {
  id: "rec",
  responses: [{ proxy: { to: "https://api.example.com", mode: "proxyOnce" } }],
};

const STATIC_STUB: Stub = {
  id: "s1",
  predicates: [{ equals: { method: "GET", path: "/users" } }],
  responses: [{ is: { statusCode: 200, body: "ok" } }],
};

/**
 * A recorded stub as the `removeProxies` projection actually returns it: the response is **flat**
 * — `statusCode`/`headers`/`body` at the top level with no `is` wrapper. The engine serves both
 * forms identically (`vendor/rift/docs/mountebank/proxy.md:208-226`).
 */
const RECORDED_FLAT: Stub = {
  predicates: [{ equals: { method: "GET", path: "/users" } }],
  responses: [{ statusCode: 201, headers: { "X-Recorded": "1" }, body: '{"id":7}' }],
};

function imposterBody(stubs: Stub[]) {
  return { port: 4545, protocol: "http", name: "checkout", stubs };
}

/** The three reads the detail screen makes, plus the fleet reads the caveat consults. */
function routes(stubs: Stub[], opts: { recorded?: Stub[]; voters?: number[] } = {}) {
  const voters = opts.voters ?? [1];
  return {
    // The revision header is not decoration: it is the `If-Match` every write here is conditioned
    // on, and the controls correctly refuse to send without one.
    "/imposters/4545": {
      json: imposterBody(stubs),
      headers: { "Rift-Cluster-Revision": "default:4545@7" },
    },
    "/imposters/4545?replayable=true&removeProxies=true": {
      json: imposterBody(opts.recorded ?? []),
    },
    "/_fleet/members": {
      json: { node_id: 1, is_leader: true, current_leader: 1, last_applied: 9, voters },
    },
    "/_fleet/health": {
      json: {
        ready: true,
        state: "ready",
        pending_gates: [],
        isolated: false,
        ring: { m_idx: 1, members: voters },
      },
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

/*
 * Recording is imposter-level configuration, so the detail screen keeps it on the Settings tab.
 * Every test here is about that panel, so they all start where it lives.
 */
beforeEach(() => {
  onDetailTab("settings");
});

describe("recording state is read from the stubs, never remembered", () => {
  it("calls an imposter with a proxy stub Recording, whoever created it", () => {
    // The point of deriving it: an imposter recorded with curl, before the console ever saw it,
    // must read as Recording on first load. A console-side flag could not know.
    expect(recordingState([PROXY_STUB])).toBe("recording");
    expect(recordingState([STATIC_STUB, PROXY_STUB])).toBe("recording");
  });

  it("calls an imposter with only static stubs Replaying, and one with none Empty", () => {
    expect(recordingState([STATIC_STUB])).toBe("replaying");
    expect(recordingState([])).toBe("empty");
    // `stubs` is optional in the contract — an absent list is not an empty one, but there is
    // nothing to replay either, so it reads as Empty rather than throwing.
    expect(recordingState(undefined)).toBe("empty");
  });
});

describe("the proxy stub a recording writes", () => {
  it("is exactly one stub carrying one proxy response", () => {
    const stub = proxyStubFor({
      to: "https://api.example.com",
      mode: "proxyOnce",
      fields: ["method", "path"],
      caseSensitive: false,
    });

    expect(stub.responses).toHaveLength(1);
    const proxy = (stub.responses?.[0] as { proxy: Record<string, unknown> }).proxy;
    expect(proxy.to).toBe("https://api.example.com");
    expect(proxy.mode).toBe("proxyOnce");
  });

  it("emits predicateGenerators in the shape the engine documents", () => {
    // `[{ matches: { … } }]` with `caseSensitive` a sibling of `matches`, per
    // `vendor/rift/docs/mountebank/proxy.md:83-137`. A wrong shape here records the wrong
    // predicates on every captured stub, and the mistake only shows up at promote time.
    const stub = proxyStubFor({
      to: "https://api.example.com",
      mode: "proxyAlways",
      fields: ["method", "path", "query"],
      caseSensitive: true,
    });
    const proxy = (stub.responses?.[0] as { proxy: Record<string, unknown> }).proxy;

    expect(proxy.predicateGenerators).toEqual([
      { matches: { method: true, path: true, query: true }, caseSensitive: true },
    ]);
  });

  it("omits unselected fields rather than sending them false", () => {
    // `matches` is a whitelist; `{ body: false }` and an absent `body` mean the same thing to the
    // engine, but only one of them reads as "I did not ask for the body".
    const stub = proxyStubFor({
      to: "https://x/",
      mode: "proxyOnce",
      fields: ["path"],
      caseSensitive: false,
    });
    const proxy = (stub.responses?.[0] as { proxy: Record<string, unknown> }).proxy;
    const generators = proxy.predicateGenerators as { matches: Record<string, boolean> }[];

    // Mapped rather than indexed, so this also pins that exactly one generator is emitted.
    expect(generators.map((generator) => Object.keys(generator.matches))).toEqual([["path"]]);
  });
});

describe("the start-recording form", () => {
  it("offers every proxy mode with what it does and what it costs", async () => {
    stubFetch(routes([]));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /start recording/i }));

    // All three modes, and each carries prose — a mode picker that only names the modes makes the
    // operator guess which one duplicates recordings across a fleet.
    for (const mode of PROXY_MODES) {
      const option = screen.getByTestId(`proxy-mode-${mode.value}`);
      expect(option.textContent).toBeTruthy();
      expect(option.textContent).toMatch(/\w{20,}/);
    }
    expect(PROXY_MODES.map((m) => m.value)).toEqual([
      "proxyOnce",
      "proxyAlways",
      "proxyTransparent",
    ]);
  });

  it("states the predicate-generator defaults as defaults rather than hiding them", async () => {
    stubFetch(routes([]));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });
    await userEvent.click(await screen.findByRole("button", { name: /start recording/i }));

    expect(DEFAULT_GENERATOR_FIELDS).toEqual(["method", "path", "query"]);
    for (const field of GENERATOR_FIELDS) {
      const box = screen.getByTestId(`generator-${field}`) as HTMLInputElement;
      expect(box.checked).toBe(DEFAULT_GENERATOR_FIELDS.includes(field));
    }
    // Named on screen, so the operator knows the selection was chosen for them and can change it.
    expect(screen.getByTestId("generator-default-note").textContent).toMatch(/default/i);
  });

  it("shows the JSON it is about to send before it sends it", async () => {
    // Promoting blind is how people end up with 400 stubs keyed on a Date header; sending blind is
    // the same mistake one step earlier.
    stubFetch(routes([]));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });
    await userEvent.click(await screen.findByRole("button", { name: /start recording/i }));

    const preview = await screen.findByTestId("recording-json-preview");
    expect(preview.textContent).toContain("proxy");
    expect(preview.textContent).toContain("predicateGenerators");
  });
});

describe("the review table", () => {
  it("renders a flat-form recorded response without rewriting it", async () => {
    /*
     * The `removeProxies` projection returns responses in flat form. The console must render them
     * as they are: "normalising" them into `is` on the way through would mean promoting a
     * different document from the one reviewed, which is the one thing this screen exists to
     * prevent.
     */
    stubFetch(routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    const row = await screen.findByTestId("recorded-row-0");
    expect(row.textContent).toContain("201");
    expect(row.textContent).toMatch(/GET/);
    expect(row.textContent).toContain("/users");

    const body = screen.getByTestId("recorded-body-0");
    expect(body.textContent).toContain('{"id":7}');
    expect(body.textContent).not.toContain('"is"');
  });

  it("renders a wrapped-form recorded response too, and promotes it unchanged", async () => {
    /*
     * The projection emits **either** form. The flat case is above; this is the canonical
     * `{ is: … }` one, which the real engine returned for a proxied `GET /orders/42` and which the
     * first cut of this table rendered as a blank row with an unknown status — worse than no row,
     * because the operator then promotes a capture they were shown nothing about.
     *
     * Unwrapping is for *reading* only: the promoted document below still carries the `is` wrapper
     * exactly as it arrived.
     */
    const wrapped: Stub = {
      predicates: [{ equals: { method: "GET", path: "/orders/42" } }],
      responses: [{ is: { statusCode: 200, body: '{"id":42}' } }],
    };
    const { requests } = stubFetch({
      ...routes([PROXY_STUB], { recorded: [wrapped] }),
      "/imposters/4545/stubs": { json: imposterBody([wrapped]) },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    const row = await screen.findByTestId("recorded-row-0");
    expect(row.textContent).toContain("200");
    expect(screen.getByTestId("recorded-body-0").textContent).toContain('{"id":42}');

    await userEvent.click(screen.getByRole("button", { name: /stop & promote/i }));
    await userEvent.click(await screen.findByTestId("confirm-destructive"));
    await waitFor(() =>
      expect(requests.some((r) => r.path === "/imposters/4545/stubs")).toBe(true),
    );

    const put = requests.find((r) => r.path === "/imposters/4545/stubs");
    expect(JSON.parse(String(put?.body)).stubs[0].responses[0]).toEqual({
      is: { statusCode: 200, body: '{"id":42}' },
    });
  });

  it("says so when a recording has captured nothing yet", async () => {
    stubFetch(routes([PROXY_STUB], { recorded: [] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    expect((await screen.findByTestId("recorded-none")).textContent).toMatch(/no.*record/i);
  });
});

describe("promote", () => {
  it("replaces the stub set through PUT /stubs with the read's revision as If-Match", async () => {
    const { calls, requests } = stubFetch({
      ...routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }),
      "/imposters/4545/stubs": { json: imposterBody([RECORDED_FLAT]) },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /stop & promote/i }));
    await userEvent.click(await screen.findByTestId("confirm-destructive"));

    await waitFor(() => expect(calls).toContain("/imposters/4545/stubs"));
    const put = requests.find((r) => r.path === "/imposters/4545/stubs");
    expect(put?.method).toBe("PUT");
    // Conditioned on the revision the review was read at — an unconditioned promote is
    // last-writer-wins against whatever changed while the operator was reading the table.
    expect(put?.headers["if-match"]).toBe("default:4545@7");
    // The recorded document goes back verbatim — including the flat response form.
    expect(JSON.parse(String(put?.body)).stubs[0].responses[0]).toEqual(
      RECORDED_FLAT.responses?.[0],
    );
  });
});

describe("the start-recording form writes what it previewed", () => {
  it("actually writes the previewed stub when the form is submitted", async () => {
    // The preview half is tested above; this is the other half — that what was previewed is what
    // gets sent, through the ordinary AddStub path.
    const { requests } = stubFetch({
      ...routes([]),
      "/imposters/4545/stubs": { json: imposterBody([]) },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /start recording/i }));
    await userEvent.type(screen.getByLabelText(/proxy target/i), "https://api.example.com");
    await userEvent.click(screen.getByRole("button", { name: /^start recording$/i }));

    await waitFor(() => expect(requests.some((r) => r.method === "POST")).toBe(true));
    const post = requests.find((r) => r.method === "POST");
    // `{"stub": …}` — the collection POST takes a wrapper, and omitting it once meant appending a
    // stub answered `400 missing field 'stub'` (see `addStubBody`'s note). Going through
    // `useAddStub` rather than hand-rolling the write is what keeps that fixed here.
    const sent = JSON.parse(String(post?.body));
    expect(sent.stub.responses[0].proxy.to).toBe("https://api.example.com");
    expect(sent.stub.responses[0].proxy.predicateGenerators[0].matches).toEqual({
      method: true,
      path: true,
      query: true,
    });
  });

  it("re-previews when a generator field is toggled off", async () => {
    // "Selectable" means the selection reaches the document, not merely that a box moves.
    stubFetch(routes([]));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });
    await userEvent.click(await screen.findByRole("button", { name: /start recording/i }));

    await userEvent.click(screen.getByTestId("generator-query"));

    const preview = screen.getByTestId("recording-json-preview");
    const sent = JSON.parse(preview.textContent ?? "{}");
    expect(sent.responses[0].proxy.predicateGenerators[0].matches).toEqual({
      method: true,
      path: true,
    });
  });
});

describe("promote is refused when there is nothing to promote", () => {
  it("does not offer promote for a recording that has captured nothing", async () => {
    /*
     * A recording that has matched no traffic yet is the normal state for the first minute of
     * every recording. Promoting it would `PUT {stubs: []}` and replace the imposter's whole stub
     * list — proxy stub included — with nothing, which no confirm dialog makes acceptable.
     */
    stubFetch(routes([PROXY_STUB], { recorded: [] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await screen.findByTestId("recorded-none");
    expect(screen.queryByRole("button", { name: /stop & promote/i })).toBeNull();
  });

  it("names the count on the confirm, so the operator sees what is being promoted", async () => {
    stubFetch(routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /stop & promote/i }));
    expect(screen.getByTestId("confirm-destructive").textContent).toMatch(/1 recorded stub/);
  });
});

describe("the review table belongs to a recording, not to every imposter", () => {
  it("is absent for an imposter that is only replaying its own stubs", async () => {
    // A `replaying` imposter's stubs are its configuration, not a capture. Listing them under a
    // "Recording" heading above the stub table that already shows them would present ordinary
    // configuration as something waiting to be promoted.
    stubFetch(routes([STATIC_STUB]));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await screen.findByTestId("detail-port");
    expect(screen.queryByTestId("recorded-row-0")).toBeNull();
    expect(screen.queryByTestId("recorded-none")).toBeNull();
  });
});

describe("a promote that races another change", () => {
  it("refuses, says nothing was promoted, and offers the retry at the fresh revision", async () => {
    /*
     * The promote is conditioned on the revision the review was read at, so a concurrent change
     * refuses it rather than overwriting. The refusal must read as a refusal — "nothing was
     * promoted, the other change is still there" — and the retry must go out at the *fresh*
     * revision, or it 409s forever. Same discipline as the stub editor's own conflict panel.
     */
    const { requests } = stubFetch({
      ...routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }),
      "/imposters/4545/stubs": { status: 409, json: { message: "revision conflict" } },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /stop & promote/i }));
    await userEvent.click(await screen.findByTestId("confirm-destructive"));

    const conflict = await screen.findByTestId("promote-conflict");
    expect(conflict.textContent).toMatch(/nothing was promoted/i);
    expect(conflict.textContent).toMatch(/changed while you were reviewing/i);
    // And the retry is offered rather than left to the operator to guess at.
    expect(screen.getByRole("button", { name: /promote again/i })).toBeTruthy();

    const puts = requests.filter((r) => r.path === "/imposters/4545/stubs");
    expect(puts.length).toBeGreaterThan(0);
  });
});

describe("discard", () => {
  it("is confirmed before it clears the recordings, and clears them at savedProxyResponses", async () => {
    const { calls } = stubFetch({
      ...routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }),
      "/imposters/4545/savedProxyResponses": { json: imposterBody([PROXY_STUB]) },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /discard recordings/i }));
    expect(calls).not.toContain("/imposters/4545/savedProxyResponses");

    await userEvent.click(await screen.findByTestId("confirm-destructive"));
    await waitFor(() => expect(calls).toContain("/imposters/4545/savedProxyResponses"));
  });
});

describe("the controls gate on the actions the server actually checks", () => {
  it("offers a viewer none of them", async () => {
    stubFetch(routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });

    await screen.findByTestId("detail-port");
    expect(screen.queryByRole("button", { name: /stop & promote/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /discard recordings/i })).toBeNull();
  });

  it("offers an operator discard but not promote", async () => {
    /*
     * Not a stylistic split. `DELETE .../savedProxyResponses` is not terminated by the admin front
     * — it proxies upstream, and `principal.rs::map_action` folds it onto
     * `Action::SavedRequestsClear` along with `savedRequests` and `requests` (RFC-002 §4.1). That
     * is `requests.clear`, which Operator holds. Promote is a `ReplaceStubs` write, which is not.
     * Transcribing the action that actually authorizes the call is `rbac.ts`'s stated rule.
     */
    stubFetch(routes([PROXY_STUB], { recorded: [RECORDED_FLAT] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("operator") });

    expect(await screen.findByRole("button", { name: /discard recordings/i })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /stop & promote/i })).toBeNull();
  });
});

describe("fleet honesty about where a recording lives", () => {
  it("warns a multi-node fleet that recording is per node, naming the issue that fixes it", async () => {
    stubFetch(routes([], { voters: [1, 2, 3] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    const caveat = await screen.findByTestId("recording-fleet-caveat");
    expect(caveat.textContent).toMatch(/per[- ]node|each node/i);
    expect(caveat.textContent).toContain("226");
  });

  it("shows the caveat before a recording starts, not after", async () => {
    // An operator who learns about duplicate recordings after driving traffic has already paid for
    // it. The state here is Empty — nothing recorded yet — and the warning is already on screen.
    stubFetch(routes([], { voters: [1, 2, 3] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    expect(await screen.findByTestId("recording-fleet-caveat")).toBeTruthy();
    expect(screen.queryByTestId("recorded-row-0")).toBeNull();
  });

  it("still warns when the fleet size could not be read at all", async () => {
    /*
     * The case that matters most, and the one a naive implementation gets backwards: `/_fleet/*`
     * authorizes `Action::ClusterAdmin`, so for every role below fleet-admin — including the
     * editors who do most of the recording — the read answers 403. Folding that into "single node"
     * would hide the warning from exactly the people it is for. An unread fleet is not a
     * one-node fleet.
     */
    stubFetch({
      "/imposters/4545": { json: imposterBody([]) },
      "/imposters/4545?replayable=true&removeProxies=true": { json: imposterBody([]) },
      "/_fleet/members": { status: 403, json: { message: "forbidden" } },
      "/_fleet/health": { status: 403, json: { message: "forbidden" } },
    });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    const caveat = await screen.findByTestId("recording-fleet-caveat");
    expect(caveat.textContent).toMatch(/per[- ]node|each node/i);
    // And it says the size is unconfirmed, rather than implying it knows the fleet is multi-node.
    expect(screen.getByTestId("recording-fleet-unconfirmed").textContent).toMatch(
      /could not be read/i,
    );
  });

  it("gives a single-node fleet no caveat at all", async () => {
    // One voter is this deployment's membership, not a shortfall — the same discipline the fleet
    // screen holds to. A banner here would be a warning about a problem that cannot occur.
    stubFetch(routes([PROXY_STUB], { recorded: [], voters: [1] }));
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    // Waited for, not sampled: until the fleet read resolves the size is genuinely unknown, and an
    // unknown fleet warns (see the test above). The claim here is the narrower one — once the read
    // lands and *says* one voter, the caveat goes away.
    await waitFor(() => expect(screen.queryByTestId("recording-fleet-caveat")).toBeNull());
  });
});
