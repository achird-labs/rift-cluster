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
