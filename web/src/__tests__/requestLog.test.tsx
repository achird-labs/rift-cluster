/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { type ReactNode, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { REQUEST_POLL_INTERVAL_MS } from "../app/query.ts";
import { RequestLog } from "../screens/RequestLog.tsx";
import { renderInApp, setTabVisibility, stubFetch, whoamiWith } from "./harness.tsx";

const PORT = 4545;
const REQUESTS = `/imposters/${PORT}/requests`;

const THREE_NODE = {
  "/_fleet/members": {
    json: { node_id: 3, is_leader: false, current_leader: 1, last_applied: 12, voters: [1, 2, 3] },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 4, members: [1, 2, 3] },
    },
  },
};

const SINGLE_NODE = {
  "/_fleet/members": {
    json: { node_id: 1, is_leader: true, current_leader: 1, last_applied: 3, voters: [1] },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 1, members: [1] },
    },
  },
};

function recorded(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    requestFrom: "127.0.0.1:5000",
    method: "GET",
    path: "/v1/payments/status",
    query: {},
    // A single-valued header is a **bare string** on the wire, not a one-element array
    // (`multi_value_headers::serialize`). The array shape the server only emits for multi-valued
    // headers hid a crash in `formatHeaders`, so the common case is the fixture's default.
    headers: { "user-agent": "curl/8" },
    timestamp: "2026-07-31T10:00:00Z",
    ...overrides,
  };
}

beforeEach(() => {
  setTabVisibility("visible");
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  setTabVisibility("visible");
});

describe("degraded-mode label (RFC-006 §11 exit criterion)", () => {
  // The criterion says a test that only checks the rows would pass on the lying version, so this
  // asserts the scope label itself and the node it names.
  it("names the node in scope and how many nodes it does not represent", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    // Waits for the fleet reading to land: until it does the label correctly declines to name a
    // node, and asserting on that first render would test the pending state instead of the fact.
    await waitFor(() =>
      expect(screen.getByTestId("request-scope-label").textContent).toContain("node 3"),
    );
    const label = screen.getByTestId("request-scope-label");
    expect(label.textContent).toContain("2 other");
    expect(label.textContent).toMatch(/one node/i);
  });

  it("says a single-node fleet is the whole fleet rather than crying wolf", async () => {
    stubFetch({ ...SINGLE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() =>
      expect(screen.getByTestId("request-scope-label").textContent).toMatch(/whole fleet/i),
    );
  });

  // A principal below fleet-admin is refused `/_fleet/*`, so the node cannot be named. Saying
  // nothing would present a per-node log as if its coverage were settled.
  it("still says the view is per-node when the fleet projection is refused", async () => {
    stubFetch({
      "/_fleet/members": { status: 404, json: { message: "not found" } },
      "/_fleet/health": { status: 404, json: { message: "not found" } },
      [REQUESTS]: { json: [recorded()] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });

    const label = await screen.findByTestId("request-scope-label");
    expect(label.textContent).toMatch(/one node/i);
    expect(label.textContent).not.toMatch(/whole fleet/i);
  });
});

describe("unknown is not empty", () => {
  it("renders a node that answered with nothing as an empty log", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("request-log-empty")).toBeTruthy();
    expect(screen.queryByTestId("request-log-unknown")).toBeNull();
  });

  // The distinction the issue calls the most important on the screen: a node that cannot answer
  // has an *unknown* log, and rendering it as an empty table tells an operator their system under
  // test never called the mock.
  it("renders a node that could not answer as unknown, never as empty", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { status: 503, json: { message: "unavailable" } } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("request-log-unknown")).toBeTruthy();
    expect(screen.queryByTestId("request-log-empty")).toBeNull();
  });
});

describe("a busy imposter", () => {
  it("keeps the DOM bounded by paging a thousands-of-entries log", async () => {
    const many = Array.from({ length: 2500 }, (_, i) =>
      recorded({ path: `/v1/item/${i}`, timestamp: `2026-07-31T10:00:${String(i % 60).padStart(2, "0")}Z` }),
    );
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("request-scope-label");
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBeGreaterThan(0));
    expect(screen.getAllByTestId("request-row").length).toBeLessThanOrEqual(50);
    // The total is phrased per-node so it cannot be read as a fleet figure.
    expect(screen.getByTestId("request-total").textContent).toContain("2500");
    expect(screen.getByTestId("request-total").textContent).toMatch(/this node/i);
  });
});

describe("attacker-influenced payloads (RFC-006 §9.1)", () => {
  // Whatever called the mock chose this path, header and body, so this is the most
  // attacker-influenced surface in the console.
  it("renders a script tag in the path and an onerror attribute in the user-agent as text", async () => {
    const hostile = recorded({
      path: "/<script>alert(1)</script>",
      headers: { "user-agent": '<img src=x onerror="alert(1)">' },
      body: "<script>alert('body')</script>",
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [hostile] } });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    // The body and headers live in the collapsed detail row, so asserting before opening it would
    // check markup that was never mounted — the assertion would pass on an implementation that
    // renders them as HTML.
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-detail");

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    // The text must still be *shown* — escaping that also hides the evidence is not a fix.
    expect(container.textContent).toContain("<script>alert(1)</script>");
    expect(container.textContent).toContain("<script>alert('body')</script>");
    expect(container.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});

describe("why did this request not match (#208)", () => {
  // The question the screen exists to answer. Until the engine recorded an outcome per journal
  // entry it could not be answered here at all: the per-stub detail lived only on the
  // `X-Rift-Debug` response path, which is a different request judged against whatever the stubs
  // have since become.
  it("names every stub that was tried and the predicate that rejected it", async () => {
    const unmatched = recorded({
      matchOutcome: {
        matched: false,
        tried: [
          { stubIndex: 0, stubId: "payments", why: { reason: "failedPredicate", predicateIndex: 1 } },
          { stubIndex: 1, why: { reason: "skippedScenarioState" } },
        ],
        triedOmitted: 3,
      },
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [unmatched] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toContain('stub "payments"');
    expect(diagnostics.textContent).toContain("predicate 1 did not match");
    expect(diagnostics.textContent).toContain("stub #1");
    expect(diagnostics.textContent).toContain("scenario state did not match");
    // A silently truncated list would make "these are the stubs that were tried" false with
    // nothing on screen to say so.
    expect(diagnostics.textContent).toContain("3 more");
  });

  it("names the stub that served a matched request", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded({ matchOutcome: { matched: true, stubIndex: 2, stubId: "payments" } })],
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/matched/i);
    expect(diagnostics.textContent).toContain('stub "payments"');
  });

  // The schema states this in bold: absence means *no outcome was recorded* — an entry from an
  // engine predating the field, an `X-Rift-Debug` request, or a matcher error — never "did not
  // match". Rendering it as a miss would tell an operator their stub was rejected when nothing
  // ever judged it.
  it("says nothing was recorded rather than claiming the request did not match", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/no match diagnostics recorded/i);
    expect(diagnostics.textContent).not.toMatch(/did not match/i);
    expect(diagnostics.textContent).not.toMatch(/nothing matched/i);
  });

  // A shape the console cannot read is not an entry with no outcome: one says the node answered
  // with something wrong, the other says nothing was recorded.
  it("calls an outcome it cannot read unreadable rather than absent", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: "yes" } })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/unreadable/i);
    expect(diagnostics.textContent).not.toMatch(/no match diagnostics recorded/i);
  });

  // A stub id is operator-authored and reaches this screen through the journal, so it belongs to
  // the same attacker-influenced surface as the path and the body (RFC-006 §9.1).
  it("renders a script tag in a stub id as text", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            matchOutcome: {
              matched: false,
              tried: [
                {
                  stubIndex: 0,
                  stubId: "<script>alert('stub')</script>",
                  why: { reason: "<img src=x onerror=\"alert(1)\">" },
                },
              ],
            },
          }),
        ],
      },
    });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-diagnostics");

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    // Escaping that also hides the evidence is not a fix — the operator still needs to read the id.
    expect(container.textContent).toContain("<script>alert('stub')</script>");
    expect(container.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});

describe("the header shapes the engine actually emits", () => {
  // `multi_value_headers::serialize` emits a scalar for one value and an array only for many, and
  // its deserializer tolerates JSON numbers because real recordings carry `"Content-Length": 124`.
  // Rendering assumed arrays, so expanding any ordinary row threw and unmounted the screen.
  it("renders single-string, multi-value and numeric header values without throwing", async () => {
    const request = recorded({
      headers: { "user-agent": "curl/8", "set-cookie": ["a=1", "b=2"], "content-length": 124 },
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [request] } });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-detail");

    expect(container.textContent).toContain("user-agent: curl/8");
    expect(container.textContent).toContain("set-cookie: a=1, b=2");
    expect(container.textContent).toContain("content-length: 124");
  });

  // The mode token is `binary`, not `base64` — `ResponseMode` serializes lowercase variant names
  // and the *encoding* it implies is base64. This fixture asserted `base64` and the screen compared
  // against `base64`, so the two agreed with each other and disagreed with the engine: the label
  // could never render, and the test that existed to prove it did still passed (#212).
  it("labels a base64 body rather than showing it as text", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ body: "3q2+7w==", _mode: "binary" })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    expect((await screen.findByTestId("request-detail")).textContent).toContain("Body (base64)");
  });

  it("leaves a text body unlabelled, which is how absence of _mode reads", async () => {
    // Guards the guard: if the label were unconditional the test above would pass for the wrong
    // reason. A text body omits `_mode` entirely rather than sending "text".
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ body: "hello" })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    expect((await screen.findByTestId("request-detail")).textContent).not.toContain("(base64)");
  });
});

describe("switching imposters", () => {
  // The pager offset used to survive the switch, so an imposter with traffic rendered as an empty
  // table paged past its end — the same lie the unknown/empty split exists to prevent.
  it("resets the page when the imposter changes", async () => {
    const many = Array.from({ length: 200 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: many },
      "/imposters/4546/requests": { json: [recorded({ path: "/only-one" })] },
    });
    // Switches port from inside the provider tree, the way the router does — `rerender` would drop
    // the providers `renderInApp` wraps around the screen.
    function Switcher(): ReactNode {
      const [port, setPort] = useState(PORT);
      return (
        <>
          <button type="button" onClick={() => setPort(4546)}>
            switch imposter
          </button>
          <RequestLog port={port} />
        </>
      );
    }
    renderInApp(<Switcher />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–100"),
    );

    await userEvent.setup().click(screen.getByRole("button", { name: /switch imposter/i }));
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    expect(screen.getByTestId("request-total").textContent).toContain("1–1 of 1");
  });
});

describe("paging through a long log", () => {
  it("moves between pages when the pager is used", async () => {
    const many = Array.from({ length: 120 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    expect(screen.getByTestId("request-total").textContent).toContain("1–50");

    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–100"),
    );

    await userEvent.setup().click(screen.getByRole("button", { name: /previous/i }));
    await waitFor(() => expect(screen.getByTestId("request-total").textContent).toContain("1–50"));
  });

  // Retention truncates the journal under the 2s poll, and `DELETE …/requests` empties it outright.
  // An offset left pointing past the new end would render an empty table for an imposter that has
  // traffic — the unknown-vs-empty lie arriving by a different route.
  it("clamps to a valid page when the journal shrinks underneath it", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let rows = Array.from({ length: 200 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        const body =
          path === REQUESTS
            ? rows
            : path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("101–150"),
    );

    rows = rows.slice(0, 60);
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);

    // Clamped to the last valid page, showing real rows rather than an empty table.
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–60 of 60"),
    );
    expect(screen.getAllByTestId("request-row").length).toBe(10);
    vi.useRealTimers();
  });

  it("shows the last partial page without over-reading", async () => {
    const many = Array.from({ length: 60 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(10));
    expect(screen.getByTestId("request-total").textContent).toContain("51–60 of 60");
  });
});

describe("polling (RFC-006 §6)", () => {
  it("refetches on the 2s request-log interval while the tab is visible", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    await screen.findByTestId("request-scope-label");

    const before = calls.filter((path) => path === REQUESTS).length;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 3 + 100);
    expect(calls.filter((path) => path === REQUESTS).length).toBeGreaterThan(before);
  });

  it("stops polling while the tab is hidden and resumes when it is shown", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    await screen.findByTestId("request-scope-label");

    setTabVisibility("hidden");
    const whileHidden = calls.filter((path) => path === REQUESTS).length;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 6 + 100);
    expect(calls.filter((path) => path === REQUESTS).length).toBe(whileHidden);

    setTabVisibility("visible");
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 100);
    await waitFor(() =>
      expect(calls.filter((path) => path === REQUESTS).length).toBeGreaterThan(whileHidden),
    );
  });
});

describe("#250 — turning a request into a stub", () => {
  const IMPOSTER = `/imposters/${PORT}`;

  function imposterWith(stubs: unknown[]): Record<string, unknown> {
    return {
      port: PORT,
      host: "0.0.0.0",
      protocol: "http",
      name: "billing",
      recordRequests: true,
      enabled: true,
      stubs,
    };
  }

  /** Expand the row's detail panel, where the action lives. */
  async function openRow(): Promise<void> {
    await userEvent.setup().click(await screen.findByTestId("request-open"));
  }

  it("offers to stub an unmatched request", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-stub-this")).toBeTruthy();
    expect(screen.queryByTestId("request-open-stub")).toBeNull();
  });

  it("offers to open the stub that answered a matched request", async () => {
    // The useful verb on a matched row is not "make a new stub" — one already answered it.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-1" } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-open-stub")).toBeTruthy();
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
  });

  it("offers no edit action when the winning stub declares no id, and says why", async () => {
    // By-index editing is the documented lost-update window, so the console declines rather than
    // offering something unsafe — and explains the refusal instead of rendering a dead row.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubIndex: 2 } })] },
      [IMPOSTER]: { json: imposterWith([{ predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-no-stub-action")).toBeTruthy();
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
    expect(screen.queryByTestId("request-open-stub")).toBeNull();
  });

  it("offers nothing at all to a reader who cannot write stubs", async () => {
    // The screen itself stays readable at `imposter.read`; only the actions are gated.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("viewer") });
    await openRow();

    expect(screen.queryByTestId("request-stub-this")).toBeNull();
    expect(screen.queryByTestId("request-no-stub-action")).toBeNull();
    expect(await screen.findByTestId("request-row")).toBeTruthy();
  });

  it("opens the editor seeded from the request, matching method and path by default", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            method: "POST",
            path: "/v1/payments",
            matchOutcome: { matched: false, tried: [] },
          }),
        ],
      },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
    expect(JSON.parse(editor.value)).toEqual({
      predicates: [{ equals: { method: "POST", path: "/v1/payments" } }],
      responses: [
        { is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" } },
      ],
    });
  });

  it("honours the field selection chosen before the editor opens", async () => {
    // The selection is made up-front precisely so nothing re-derives over a hand-edited draft.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            query: { page: "2" },
            matchOutcome: { matched: false, tried: [] },
          }),
        ],
      },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    const user = userEvent.setup();
    // Query is ON by default (it agrees with the recording flow), so exercise the selection in the
    // direction that actually changes something: turn it off, and opt a header IN.
    await user.click(await screen.findByRole("checkbox", { name: "Match on query" }));
    await user.click(screen.getByRole("checkbox", { name: "Match on header user-agent" }));
    await user.click(screen.getByTestId("request-stub-this"));

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
    const stub = JSON.parse(editor.value) as { predicates: { equals: Record<string, unknown> }[] };
    expect(stub.predicates[0]?.equals.query).toBeUndefined();
    expect(stub.predicates[0]?.equals.headers).toEqual({ "user-agent": "curl/8" });
  });

  it("refuses to re-seed over an open draft, rather than discarding it silently", async () => {
    /*
     * Clicking "Stub this" again remounts the editor with a fresh seed — which would throw away
     * whatever the operator had typed, with no warning. The selection is chosen BEFORE opening
     * precisely so nothing re-derives afterwards; this is the other half of that rule.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));
    await screen.findByTestId("code-editor-fallback");

    expect((screen.getByTestId("request-stub-this") as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByTestId("stub-this-busy").textContent).toMatch(/discard the draft/i);
  });

  it("does not open an editor over a stub the imposter no longer carries", async () => {
    /*
     * A journal entry outlives the stub that served it: the id can be gone by the time the row is
     * clicked. Opening an editor over `{}` would show an empty document titled with the missing id,
     * and a save would then PUT `{}` over whatever the fleet actually has. `ImposterDetail` refuses
     * to mount in this case; so must this screen.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-gone" } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-open-stub"));

    expect((await screen.findByTestId("stub-gone")).textContent).toMatch(/no longer in this/i);
    expect(screen.queryByTestId("code-editor-fallback")).toBeNull();
  });

  it("does not claim a catch-all shadows an EXISTING stub being edited", async () => {
    /*
     * The warning is about appends — "new stubs are appended, first-match-wins". An existing stub
     * may sit above the catch-all and fire perfectly well, so saying it will never fire is simply
     * false, and a wrong claim on this screen is the failure mode the module set exists to avoid.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-1" } })] },
      [IMPOSTER]: {
        json: imposterWith([
          { id: "s-1", predicates: [{ equals: { path: "/x" } }] },
          { id: "catch", responses: [{ is: { statusCode: 200 } }] },
        ]),
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-open-stub"));

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });

  it("offers no action, and says why, when the match diagnostics cannot be read", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: "yes" } })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect((await screen.findByTestId("request-no-stub-action")).textContent).toMatch(/unreadable/i);
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
  });

  it("keeps rendering when a row's match outcome is null", async () => {
    // `diagnostics.ts` folds null into absence; reading `.matched` off it would throw and unmount
    // the screen an operator opened precisely because something was already wrong.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: null })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-stub-this")).toBeTruthy();
  });

  it("keeps rendering when the imposter's stub list is not an array", async () => {
    /*
     * The list comes off the wire, and this is the screen an operator opens when something is
     * already wrong. A `stubs: null` reaching `hasCatchAll(...).some` would throw during render and
     * take the log down at the worst possible moment.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: {
        json: {
          port: PORT,
          host: "0.0.0.0",
          protocol: "http",
          name: "billing",
          recordRequests: true,
          enabled: true,
          stubs: null,
        },
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    expect(await screen.findByTestId("code-editor-fallback")).toBeTruthy();
    // No catch-all can be proven from a list that is not a list, so it must not claim one.
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });

  it("warns that a catch-all stub will shadow the one being added", async () => {
    /*
     * Stubs append and matching is first-match-wins, so a stub added below a catch-all never fires.
     * Without this the operator saves, sees no change, and has nothing on screen explaining why.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "catch-all", responses: [{ is: { statusCode: 200 } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    expect((await screen.findByTestId("stub-shadow-warning")).textContent).toMatch(
      /every request|first match|never fire/i,
    );
  });

  it("does not warn when the imposter has no catch-all", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });
});
