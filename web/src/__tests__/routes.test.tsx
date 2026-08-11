/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { POLL_INTERVAL_MS } from "../app/query.ts";
import { RouteTableScreen } from "../screens/Routes.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const ROUTES = "/front-door/routes";
const ROUTE_HITS = "/front-door/route-hits";

/** Every id in `TABLE`, installed and counted — the shape the screen sees in the ordinary case. */
const HITS = { installed: true, hits: { alpha: 0, beta: 0 } };

function route(id: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id,
    priority: 0,
    match: { path_prefix: `/${id}` },
    target: { port: 4545, strip_prefix: false },
    enabled: true,
    ...overrides,
  };
}

const TABLE = { routes: [route("alpha"), route("beta", { priority: 5 })] };

/** The calls a test needs to assert against: method, path and parsed body. */
type Call = { method: string; path: string; body: unknown };

/**
 * A fetch double that can change its answer between calls — the only way to model a second editor
 * committing underneath the first, which a static route→reply map cannot express.
 */
function stubSequence(replies: {
  get: () => unknown;
  put?: () => { status: number; json: unknown };
  del?: () => { status: number; json: unknown };
}): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      const method = init?.method ?? "GET";
      const body = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
      calls.push({ method, path, body });
      if (method === "GET") {
        // The Hits column reads its own endpoint (#368). These tests are about the editor, not
        // the counts, so it answers a constant — but it has to answer, since an unstubbed path is
        // a hard failure in this harness.
        const json = path.startsWith(ROUTE_HITS) ? HITS : replies.get();
        return Promise.resolve(new Response(JSON.stringify(json), { status: 200 }));
      }
      if (method === "PUT") {
        const reply = replies.put?.() ?? { status: 200, json: replies.get() };
        return Promise.resolve(new Response(JSON.stringify(reply.json), { status: reply.status }));
      }
      const reply = replies.del?.() ?? { status: 200, json: {} };
      return Promise.resolve(new Response(JSON.stringify(reply.json), { status: reply.status }));
    }),
  );
  return calls;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("effective order on screen", () => {
  it("lists routes in the order the front door evaluates them, not authoring order", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: HITS } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    const ids = screen.getAllByTestId("route-id").map((n) => n.textContent);
    // `beta` is authored second but has the higher priority.
    expect(ids).toEqual(["beta", "alpha"]);
  });

  it("shows a disabled route with no rank, because it is never dispatched", async () => {
    stubFetch({
      [ROUTES]: { json: { routes: [route("on"), route("off", { enabled: false })] } },
      [ROUTE_HITS]: { json: { installed: true, hits: { on: 0, off: 0 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    const ranks = screen.getAllByTestId("route-rank").map((n) => n.textContent);
    expect(ranks).toContain("—");
  });
});

describe("route CRUD round-trip", () => {
  it("replaces the whole table with a PUT", async () => {
    const calls = stubSequence({ get: () => TABLE });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe(ROUTES);
    const routes = (put?.body as { routes: { id: string; enabled: boolean }[] }).routes;
    expect(routes).toHaveLength(2);
    expect(routes.find((r) => r.id === "alpha")?.enabled).toBe(false);
  });

  it("removes one route with a DELETE by id rather than a whole-table replace", async () => {
    const calls = stubSequence({ get: () => TABLE });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /delete alpha/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "DELETE")).toBe(true));
    expect(calls.find((c) => c.method === "DELETE")?.path).toBe(`${ROUTES}/alpha`);
    expect(calls.some((c) => c.method === "PUT")).toBe(false);
  });
});

describe("two concurrent editors", () => {
  // `If-Match` does not apply to the route table (`admin_front.rs:1811` — single-imposter
  // operations only), so the editor re-reads before replacing and refuses to overwrite a table
  // that moved underneath it.
  it("refuses to clobber a table a second editor changed, and offers refresh-and-reapply", async () => {
    let committedByOther = false;
    const calls = stubSequence({
      get: () =>
        committedByOther ? { routes: [route("alpha"), route("gamma", { priority: 9 })] } : TABLE,
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    // The second editor commits between this editor's load and its save.
    committedByOther = true;
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));

    expect(await screen.findByTestId("route-conflict")).toBeTruthy();
    // The whole point: the other editor's table is still standing.
    expect(calls.some((c) => c.method === "PUT")).toBe(false);
  });

  it("saves normally when nothing changed underneath", async () => {
    const calls = stubSequence({ get: () => TABLE });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    expect(screen.queryByTestId("route-conflict")).toBeNull();
  });
});

describe("the 5s poll versus an in-progress edit", () => {
  // The other half of "do not silently clobber": the background refetch can destroy an operator's
  // draft just as effectively as a concurrent PUT can destroy their table. Without this test,
  // simplifying the adopt-only-while-clean guard would reintroduce silent data loss and every
  // other test would still pass.
  it("leaves a dirty draft alone when a poll brings a newer table", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let serverTable: unknown = TABLE;
    stubSequence({ get: () => serverTable });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    // Someone else's route lands while this operator is mid-edit.
    serverTable = { routes: [route("alpha"), route("beta", { priority: 5 }), route("gamma")] };
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 2 + 100);

    // The edit survives: alpha is still disabled, so the control still offers to re-enable it.
    expect(screen.getByRole("button", { name: /enable alpha/i })).toBeTruthy();
    expect(screen.getAllByTestId("route-row").length).toBe(2);
    vi.useRealTimers();
  });

  it("adopts a newer table when the draft is clean", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let serverTable: unknown = TABLE;
    stubSequence({ get: () => serverTable });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    serverTable = { routes: [route("alpha"), route("beta", { priority: 5 }), route("gamma")] };
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 2 + 100);

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(3));
    vi.useRealTimers();
  });
});

describe("recovering from a conflict", () => {
  it("reapplies the operator's edits on top of the current table and then saves", async () => {
    let committedByOther = false;
    const calls = stubSequence({
      get: () =>
        committedByOther ? { routes: [route("alpha"), route("gamma", { priority: 9 })] } : TABLE,
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    committedByOther = true;
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await screen.findByTestId("route-conflict");

    await userEvent.setup().click(screen.getByRole("button", { name: /reapply my edits/i }));
    expect(screen.queryByTestId("route-conflict")).toBeNull();

    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    // The operator's own edit is what gets sent, rebased rather than auto-merged.
    const put = calls.find((c) => c.method === "PUT");
    const routes = (put?.body as { routes: { id: string; enabled: boolean }[] }).routes;
    expect(routes.find((r) => r.id === "alpha")?.enabled).toBe(false);
  });

  it("discards the operator's edits back to the table the fleet now holds", async () => {
    let committedByOther = false;
    stubSequence({
      get: () =>
        committedByOther ? { routes: [route("alpha"), route("gamma", { priority: 9 })] } : TABLE,
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    committedByOther = true;
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await screen.findByTestId("route-conflict");

    await userEvent.setup().click(screen.getByRole("button", { name: /discard my edits/i }));
    expect(screen.queryByTestId("route-conflict")).toBeNull();
    await waitFor(() =>
      expect(screen.getAllByTestId("route-id").map((n) => n.textContent)).toEqual([
        "gamma",
        "alpha",
      ]),
    );
  });
});

describe("validation", () => {
  it("refuses to send a table the server would reject as a whole", async () => {
    const calls = stubSequence({
      get: () => ({ routes: [route("bad", { match: {}, target: { port: 1, strip_prefix: true } })] }),
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(1));

    expect(await screen.findByTestId("route-validation")).toBeTruthy();
    expect(screen.getByTestId("route-validation").textContent).toMatch(/strip/i);
    // Disabled rather than a live button that silently does nothing.
    expect((screen.getByRole("button", { name: /save table/i }) as HTMLButtonElement).disabled).toBe(
      true,
    );
    expect(calls.some((c) => c.method === "PUT")).toBe(false);
  });

  // Two routes distinguished only by their path prefix are a perfectly ordinary table. While the
  // console read the wrong field name they looked identical, were reported ambiguous, and could not
  // be saved at all.
  it("accepts a table whose routes differ only by path prefix", async () => {
    const calls = stubSequence({
      get: () => ({
        routes: [
          route("short", { match: { path_prefix: "/api" } }),
          route("long", { match: { path_prefix: "/api/v1/payments" } }),
        ],
      }),
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    expect(screen.queryByTestId("route-validation")).toBeNull();
    // The longer prefix is more specific, so the front door evaluates it first.
    expect(screen.getAllByTestId("route-id").map((n) => n.textContent)).toEqual(["long", "short"]);
    expect(screen.getByText(/path \/api\/v1\/payments/)).toBeTruthy();

    await userEvent.setup().click(screen.getByRole("button", { name: /disable short/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));

    // The wire field the fleet actually reads, with no camelCase twin riding along.
    const put = calls.find((c) => c.method === "PUT");
    const sent = (put?.body as { routes: Record<string, never>[] }).routes;
    expect(JSON.stringify(sent)).toContain("path_prefix");
    expect(JSON.stringify(sent)).not.toContain("pathPrefix");
    expect(JSON.stringify(sent)).not.toContain("stripPrefix");
  });

  // The client mirror is advisory; the fleet's refusal is the authority and must be readable
  // verbatim when the two disagree.
  it("surfaces the server's own refusal verbatim", async () => {
    stubSequence({
      get: () => TABLE,
      put: () => ({
        status: 400,
        json: { message: "routes 'alpha' and 'beta' are both enabled and match exactly the same requests" },
      }),
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));

    const error = await screen.findByTestId("route-server-error");
    expect(error.textContent).toContain("match exactly the same requests");
  });
});

describe("read-only principals", () => {
  it("offers no write control to a viewer", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: HITS } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("viewer") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    expect(screen.queryByRole("button", { name: /save table/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /delete alpha/i })).toBeNull();
  });
});

describe("a parked table write must not be undone by the next poll (#211)", () => {
  /*
   * The sequence this guards, which is a data-loss path and not merely a display glitch:
   *
   *   1. the PUT is parked (202) and this principal cannot read `/_fleet/ops/*`, so the outcome is
   *      `unobservable`;
   *   2. if the screen treated that as saved it would advance `base` to the draft, leaving the
   *      editor clean;
   *   3. the invalidation refetch then returns the table as it was BEFORE the parked write, and
   *      adopt-when-clean would pull it into both `draft` and `base`;
   *   4. the operator's edits vanish, and the next save would send that stale table with a matching
   *      base — sailing through the concurrency re-read and undoing the parked write on landing.
   */
  function stubParkedPut(): Call[] {
    const calls: Call[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === "string" ? input : input.toString();
        const method = init?.method ?? "GET";
        const body = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
        calls.push({ method, path, body });
        if (path.startsWith("/_fleet/ops/")) {
          // Fleet-scoped; an ordinary editor gets 404 whatever the write actually did.
          return Promise.resolve(new Response("", { status: 404 }));
        }
        if (method === "PUT") {
          return Promise.resolve(
            new Response(JSON.stringify({ opId: "op-parked" }), { status: 202 }),
          );
        }
        // The table never changes: the parked write has not committed.
        return Promise.resolve(new Response(JSON.stringify(TABLE), { status: 200 }));
      }),
    );
    return calls;
  }

  it("keeps the operator's edit on screen and says the write is unconfirmed", async () => {
    const calls = stubParkedPut();
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));

    const note = await screen.findByTestId("write-unconfirmed");
    expect(note.textContent).toContain("Accepted, not yet confirmed");

    // The refetch has returned the pre-write table by now. The edit must still be on screen.
    await waitFor(() => expect(calls.filter((c) => c.method === "GET").length).toBeGreaterThan(1));
    expect(screen.getByRole("button", { name: /enable alpha/i })).toBeDefined();
  });

  it("does not let a second save send the pre-write table back", async () => {
    const calls = stubParkedPut();
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });
    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable alpha/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await screen.findByTestId("write-unconfirmed");
    await waitFor(() => expect(calls.filter((c) => c.method === "GET").length).toBeGreaterThan(1));

    await userEvent.setup().click(screen.getByRole("button", { name: /save table/i }));
    await waitFor(() => expect(calls.filter((c) => c.method === "PUT").length).toBe(2));

    // Every PUT must still carry the operator's intent. A PUT re-enabling alpha would be the
    // parked write being undone.
    for (const put of calls.filter((c) => c.method === "PUT")) {
      const routes = (put.body as { routes: { id: string; enabled: boolean }[] }).routes;
      expect(routes.find((r) => r.id === "alpha")?.enabled).toBe(false);
    }
  });
});

describe("the HITS column (#368)", () => {
  it("shows the fleet count for a route that has taken traffic", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { installed: true, hits: { alpha: 12, beta: 4 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    // Rendered in effective order, so `beta` (priority 5) is first. Awaited because the counts are
    // a second, independent read — the table renders before they land.
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["4", "12"]),
    );
  });

  // The zero is the whole reason the column exists: a route that has never taken a request is
  // either wrong or dead. It must read as a number, not as an empty cell or a dash.
  it("shows an explicit zero for a route that has taken nothing", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { installed: true, hits: { alpha: 0, beta: 7 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["7", "0"]),
    );
    // Flagged, not merely printed — an operator scanning the table should see the dead route.
    const cells = screen.getAllByTestId("route-hits");
    expect(cells[1]?.className).toContain("warn");
    expect(cells[0]?.className).not.toContain("warn");
  });

  // A non-default tenant's routes are stored but never compiled into the shared front door, so a
  // `0` would assert "took no traffic" where the truth is "cannot take traffic".
  it("says not installed, never zero, for a tenant whose routes are not compiled in", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { installed: false, hits: null } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual([
        "not installed",
        "not installed",
      ]),
    );
    for (const cell of screen.getAllByTestId("route-hits")) {
      expect(cell.textContent).not.toContain("0");
    }
  });

  it("labels the counts as a floor when the fleet fan-out was partial", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: {
        json: { installed: true, hits: { alpha: 12, beta: 4 } },
        headers: { "Rift-Cluster-Partial": "true" },
      },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    expect(await screen.findByTestId("route-hits-partial")).toBeTruthy();
  });

  it("does not claim a floor when every node answered", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { installed: true, hits: { alpha: 12, beta: 4 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    expect(screen.queryByTestId("route-hits-partial")).toBeNull();
  });

  // The counts are a separate read from the table. If they fail, the operator still needs the
  // table — and an unknown count must not render as a zero, which is a claim about traffic.
  it("falls back to a dash, never a zero, when the hits read fails", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { status: 503, json: { message: "cluster node is shutting down" } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["—", "—"]),
    );
  });
});

describe("the HITS column refuses to guess", () => {
  // A route the server did not report a count for is unknown, not idle. The server keys the map
  // by every id in the table it read, so this means the table moved between the two reads.
  it("dashes a route the hits response does not mention, rather than calling it zero", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { installed: true, hits: { beta: 9 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["9", "—"]),
    );
  });

  // `installed` is required by the contract. Defaulting a missing one to `false` would render the
  // confident claim "not installed" — that this tenant's routes can never take a dispatch — off
  // the back of a body the console simply could not read.
  it("does not report not-installed when the body carried no installed flag", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { hits: { alpha: 1, beta: 2 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["—", "—"]),
    );
    for (const cell of screen.getAllByTestId("route-hits")) {
      expect(cell.textContent).not.toContain("not installed");
    }
  });
});

describe("a disabled route's zero is explained, not alarming", () => {
  // `RouteTable::effective_order` filters disabled routes out of the dispatch chain upstream, so a
  // disabled route is structurally incapable of claiming a request — the same "cannot" versus
  // "did not" distinction the not-installed state exists for, one level down. Flagging its zero
  // would tell an operator their route is broken seconds after they switched it off themselves.
  it("does not flag the zero of a route that is switched off", async () => {
    stubFetch({
      [ROUTES]: { json: { routes: [route("on"), route("off", { enabled: false })] } },
      [ROUTE_HITS]: { json: { installed: true, hits: { on: 0, off: 0 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["0", "0"]),
    );
    const [enabled, disabled] = screen.getAllByTestId("route-hits");
    expect(enabled?.className).toContain("warn");
    expect(disabled?.className).not.toContain("warn");
  });

  // The count is a fact about traffic already taken, so switching a route off does not erase it.
  it("still shows what a route took before it was switched off", async () => {
    stubFetch({
      [ROUTES]: { json: { routes: [route("off", { enabled: false })] } },
      [ROUTE_HITS]: { json: { installed: true, hits: { off: 40 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(1));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["40"]),
    );
  });
});
