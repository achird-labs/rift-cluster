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

  it("labels a base64 body rather than showing it as text", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ body: "3q2+7w==", _mode: "base64" })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    expect((await screen.findByTestId("request-detail")).textContent).toContain("Body (base64)");
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
