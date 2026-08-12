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

/**
 * A non-default tenant's routes are stored and read back, but `desired_routes` compiles only the
 * default tenant's into the shared front door — so this table is structurally incapable of taking
 * a request. #404 fixed the Hits *cell*; every other column on the screen still described a live
 * dispatch chain. These tests pin the rest of the screen to the same fact.
 */
describe("a tenant whose table is never installed (#400)", () => {
  const NOT_INSTALLED = { installed: false, hits: null };

  it("states once that the table is stored but never compiled into the front door", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    const banner = await screen.findByTestId("routes-not-installed");
    expect(banner.getAttribute("role")).toBe("status");
    expect(banner.textContent).toMatch(/stored/i);
    expect(banner.textContent).toMatch(/not compiled|never compiled/i);
    // The reason, not just the fact: one shared listener with no tenant discriminator.
    expect(banner.textContent).toMatch(/default tenant/i);
  });

  // A rank is a claim about position within a chain. There is no chain here.
  it("gives no route a rank, because there is no dispatch chain to rank within", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-rank").map((n) => n.textContent)).toEqual(["—", "—"]),
    );
    // The dash alone is not the requirement: it has to be muted like a disabled route's, and it
    // has to explain itself. Asserting only `textContent` let both of those be deleted silently.
    for (const cell of screen.getAllByTestId("route-rank")) {
      expect(cell.getAttribute("title")).toMatch(/not in any dispatch chain/i);
      expect(cell.querySelector("span")?.className).toBe("order-rank off");
    }
  });

  // The header says "listed in stored order", so the rows have to actually be in stored order.
  // `effectiveOrder` ranks by priority, which would put `beta` (priority 5) first — a fabricated
  // order under a label promising the stored one.
  it("lists the rows in stored order, which is what the header now claims", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-id").map((n) => n.textContent)).toEqual([
        "alpha",
        "beta",
      ]),
    );
  });

  it("states the fact above the table rather than after it", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    const { container } = renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    const banner = await screen.findByTestId("routes-not-installed");
    const card = container.querySelector("section.card");
    expect(card).not.toBeNull();
    expect(banner.compareDocumentPosition(card as Node) & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
  });

  // Otherwise the rail names a winning route on a table the banner has just called inert.
  it("stops the route tester presenting a dispatch this tenant can never get", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getByTestId("probe-hint").textContent).toMatch(/never installed/i),
    );
  });

  // `orderReason` prose ("priority 5 → no host clause → id beta") is exactly the live-chain
  // implication being removed, so the assertion is that it is absent, not merely overridden.
  it("does not explain an evaluation order that does not exist", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-why").map((n) => n.textContent)).toEqual([
        "not installed",
        "not installed",
      ]),
    );
    for (const cell of screen.getAllByTestId("route-why")) {
      expect(cell.textContent).not.toMatch(/priority|host clause/i);
    }
  });

  it("stops the screen's header claiming the front door evaluates this table", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() => expect(screen.queryByTestId("routes-not-installed")).not.toBeNull());
    expect(screen.queryByText(/the order the front door evaluates them/i)).toBeNull();
    expect(screen.getByText(/stored order/i)).toBeTruthy();
  });

  // The stored table is real replicated state and writes to it are legitimate. Muting the *chain*
  // must not read as a read-only screen.
  it("still offers every write control, because the stored table is real", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() => expect(screen.queryByTestId("routes-not-installed")).not.toBeNull());
    expect(screen.getByTestId("add-route")).toBeTruthy();
    expect(screen.getByRole("button", { name: /save table/i })).toBeTruthy();
    expect(screen.getByRole("button", { name: /disable alpha/i })).toBeTruthy();
    expect(screen.getByRole("button", { name: /delete alpha/i })).toBeTruthy();
  });

  // Not-installed is the stronger and structural statement: the whole table is inert, so "disabled"
  // would explain a route's absence from a chain that does not exist in the first place.
  it("says not installed for a disabled route too, rather than calling it disabled", async () => {
    stubFetch({
      [ROUTES]: { json: { routes: [route("on"), route("off", { enabled: false })] } },
      [ROUTE_HITS]: { json: NOT_INSTALLED },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-why").map((n) => n.textContent)).toEqual([
        "not installed",
        "not installed",
      ]),
    );
  });

  // The fact is about the tenant's table, not about the rows in it.
  it("names the fact even for a tenant that has stored no routes at all", async () => {
    stubFetch({ [ROUTES]: { json: { routes: [] } }, [ROUTE_HITS]: { json: NOT_INSTALLED } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    expect(await screen.findByTestId("routes-not-installed")).toBeTruthy();
  });

  /*
   * `/front-door/route-hits` is a cluster-wide fan-out. Two components needing the flag is not a
   * licence to observe it twice: `Editor` mounts only after the table resolves, so at `staleTime: 0`
   * a second observer refetches on mount rather than reading the cache — one screen load, two
   * fan-outs. The flag is read once at the top and passed down, and this counts it.
   */
  it("issues one route-hits fan-out per load, however many components need the flag", async () => {
    const { calls } = stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: HITS } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() => expect(screen.getAllByTestId("route-hits")[0]?.textContent).toBe("0"));
    expect(calls.filter((path) => path.startsWith(ROUTE_HITS)).length).toBe(1);
  });

  // The default-tenant path is the one that must not regress: its chain is real.
  it("leaves the installed tenant's chain exactly as it was", async () => {
    stubFetch({ [ROUTES]: { json: TABLE }, [ROUTE_HITS]: { json: HITS } });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    expect(screen.queryByTestId("routes-not-installed")).toBeNull();
    // Evaluation order, not stored order: `beta` outranks `alpha` on priority.
    expect(screen.getAllByTestId("route-id").map((n) => n.textContent)).toEqual(["beta", "alpha"]);
    expect(screen.getAllByTestId("route-rank").map((n) => n.textContent)).toEqual(["1", "2"]);
    for (const cell of screen.getAllByTestId("route-rank")) {
      expect(cell.getAttribute("title")).toBeNull();
      expect(cell.querySelector("span")?.className).toBe("order-rank");
    }
    expect(screen.getAllByTestId("route-why")[0]?.textContent).toMatch(/priority 5/);
    expect(screen.getByText(/the order the front door evaluates them/i)).toBeTruthy();
    expect(screen.getByTestId("probe-hint").textContent).not.toMatch(/never installed/i);
  });
});

/**
 * The same bound-versus-unknown rule #369 established, one level up: a flag the console could not
 * read is not a flag that came back false. Folding the two together would put a confident
 * structural claim — "this table can never take a request" — behind a failed HTTP call.
 */
describe("an unreadable installed flag is not a not-installed table (#400)", () => {
  /*
   * A genuinely in-flight read, not a failed one. The 503 and missing-flag cases below both reach
   * `data === undefined` through `isError`; this is the third route to the same undefined, and it
   * is the one a future `hits.isPending` special-case would break without any other test noticing.
   */
  it("does not banner or mute while the hits read is still in flight", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path.startsWith(ROUTE_HITS)) return new Promise<Response>(() => {});
        if (path.startsWith(ROUTES)) {
          return Promise.resolve(new Response(JSON.stringify(TABLE), { status: 200 }));
        }
        return Promise.reject(new Error(`test stub has no reply for ${path}`));
      }),
    );
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    expect(screen.queryByTestId("routes-not-installed")).toBeNull();
    expect(screen.getAllByTestId("route-id").map((n) => n.textContent)).toEqual(["beta", "alpha"]);
    expect(screen.getAllByTestId("route-rank").map((n) => n.textContent)).toEqual(["1", "2"]);
    expect(screen.getAllByTestId("route-why")[0]?.textContent).toMatch(/priority 5/);
    expect(screen.getByText(/the order the front door evaluates them/i)).toBeTruthy();
    // Unknown, and said as unknown — not as a zero and not as "not installed".
    expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["—", "—"]);
  });

  it("does not banner or mute when the hits read failed outright", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { status: 503, json: { message: "cluster node is shutting down" } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["—", "—"]),
    );
    expect(screen.queryByTestId("routes-not-installed")).toBeNull();
    expect(screen.getAllByTestId("route-rank").map((n) => n.textContent)).toEqual(["1", "2"]);
    expect(screen.getAllByTestId("route-why")[0]?.textContent).toMatch(/priority 5/);
    expect(screen.getByText(/the order the front door evaluates them/i)).toBeTruthy();
  });

  it("does not banner or mute when the body carried no installed flag", async () => {
    stubFetch({
      [ROUTES]: { json: TABLE },
      [ROUTE_HITS]: { json: { hits: { alpha: 1, beta: 2 } } },
    });
    renderInApp(<RouteTableScreen />, { whoami: whoamiWith("editor") });

    await waitFor(() => expect(screen.getAllByTestId("route-row").length).toBe(2));
    await waitFor(() =>
      expect(screen.getAllByTestId("route-hits").map((n) => n.textContent)).toEqual(["—", "—"]),
    );
    expect(screen.queryByTestId("routes-not-installed")).toBeNull();
    expect(screen.getAllByTestId("route-rank").map((n) => n.textContent)).toEqual(["1", "2"]);
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
