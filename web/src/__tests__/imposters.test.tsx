/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CSRF_HEADER, TENANT_HEADER } from "../api/client.ts";
import { IMPOSTER_COLUMNS } from "../app/contract.ts";
import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const TWO = {
  imposters: [
    { port: 4545, protocol: "http", name: "billing", recordRequests: true, enabled: true, stubs: [{}, {}] },
    { port: 4546, protocol: "https", name: "shipping", recordRequests: false, enabled: false, stubs: [] },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("imposter list", () => {
  it("renders only fields the Imposter schema declares", async () => {
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    expect(await screen.findByText("billing")).toBeTruthy();
    expect(screen.getByText("4545")).toBeTruthy();
    expect(screen.getByText("shipping")).toBeTruthy();
    expect(screen.getAllByText("https").length).toBeGreaterThan(0);
  });

  it("says whose view this is, and does not claim the tenant is empty", async () => {
    // A console that says "no imposters" is asserting a fleet-wide fact from one node's answer.
    stubFetch({ "/imposters": { json: { imposters: [] } }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    const empty = await screen.findByTestId("imposters-empty");
    expect(empty.textContent).toMatch(/this node/i);
    expect(empty.textContent).not.toMatch(/^No imposters\.?$/);
  });

  it("warns that an empty list cannot be confirmed when this node is degraded", async () => {
    stubFetch({
      "/imposters": { json: { imposters: [] } },
      "/_fleet/members": { json: { node_id: 1, is_leader: false, current_leader: null, last_applied: 5, voters: [1, 2, 3] } },
      "/_fleet/health": { json: { ready: true, state: "ready", pending_gates: [], isolated: true, ring: { m_idx: 3, members: [1, 2, 3] } } },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    const empty = await screen.findByTestId("imposters-empty");
    await waitFor(() => expect(empty.textContent).toMatch(/cannot confirm/i));
  });

  it("says the empty list is unqualified when the fleet reading itself failed", async () => {
    // A FleetAdmin whose `/_fleet/*` read 500s has lost the signal that would say whether an empty
    // list can be trusted. Folding that into "nothing to report" would present the gap as a clean
    // reading — the same mistake as saying "no imposters" from a degraded node.
    stubFetch({
      "/imposters": { json: { imposters: [] } },
      "/_fleet/members": { status: 500, json: { message: "boom" } },
      "/_fleet/health": { status: 500, json: { message: "boom" } },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    const empty = await screen.findByTestId("imposters-empty");
    // Longer than the default: a 500 earns one retry under the shared policy, so the caveat only
    // appears once that backoff has elapsed. Waiting it out is the point — a shorter window would
    // pass by observing the *pending* state rather than the failed one.
    await waitFor(() => expect(empty.textContent).toMatch(/cannot confirm/i), { timeout: 5_000 });
    expect((await screen.findByTestId("imposters-scope-label")).textContent).toMatch(
      /could not be obtained/i,
    );
  });

  it("does not caveat the list for a principal who never had the fleet scope", async () => {
    // The other side of the same distinction: a viewer is *refused* the projection, which is not
    // evidence of anything. A permanent warning here is one operators would learn to ignore.
    stubFetch({ "/imposters": { json: { imposters: [] } } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    const empty = await screen.findByTestId("imposters-empty");
    expect(empty.textContent).not.toMatch(/cannot confirm/i);
    expect((await screen.findByTestId("imposters-scope-label")).textContent).not.toMatch(
      /could not be obtained/i,
    );
  });
});

describe("lifecycle toggle (the one mutation in C4)", () => {
  it("posts to the lifecycle route with the CSRF header and refetches the list", async () => {
    const { calls } = stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { json: { message: "disabled" } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /disable billing/i }));

    await waitFor(() => expect(calls).toContain("/imposters/4545/disable"));
    const call = (globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } }).mock.calls.find(
      ([path]) => path === "/imposters/4545/disable",
    );
    expect(call?.[1]?.method).toBe("POST");
    expect((call?.[1]?.headers as Record<string, string>)[CSRF_HEADER]).toBe("1");

    // The list must re-read after the write, or the row keeps showing the pre-toggle state until
    // the next poll tick — which is the "reflects it within one poll interval" criterion.
    await waitFor(() => expect(calls.filter((p) => p === "/imposters").length).toBeGreaterThan(1));
  });

  it("enables a disabled imposter through the enable route", async () => {
    const { calls } = stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4546/enable": { json: { message: "enabled" } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("shipping");

    await userEvent.setup().click(screen.getByRole("button", { name: /enable shipping/i }));
    await waitFor(() => expect(calls).toContain("/imposters/4546/enable"));
  });

  it("carries the tenant in view on both the read and the write", async () => {
    stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { json: { message: "disabled" } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator", ["acme", "globex"]), tenant: "globex", tenants: ["acme", "globex"] });
    await screen.findByText("billing");
    await userEvent.setup().click(screen.getByRole("button", { name: /disable billing/i }));

    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    await waitFor(() => {
      const write = mock.mock.calls.find(([path]) => path === "/imposters/4545/disable");
      expect((write?.[1]?.headers as Record<string, string>)[TENANT_HEADER]).toBe("globex");
    });
    const read = mock.mock.calls.find(([path]) => path === "/imposters");
    expect((read?.[1]?.headers as Record<string, string>)[TENANT_HEADER]).toBe("globex");
  });

  it("surfaces a refused toggle instead of leaving the row looking changed", async () => {
    // The API is the boundary (RFC-006 §3 rule 3). When it refuses, the screen must say so rather
    // than optimistically render the toggle as applied.
    stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { status: 403, json: { message: "forbidden" } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");
    await userEvent.setup().click(screen.getByRole("button", { name: /disable billing/i }));

    expect((await screen.findByRole("alert")).textContent).toMatch(/403/);
  });
});

describe("RBAC-correct visibility", () => {
  it("shows a viewer no lifecycle affordance at all", async () => {
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    expect(screen.queryByRole("button", { name: /disable/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /enable/i })).toBeNull();
  });

  it("shows an operator the lifecycle affordance", async () => {
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");

    expect(screen.getByRole("button", { name: /disable billing/i })).toBeTruthy();
  });
});

describe("every rendered cell comes from the declared column table", () => {
  it("renders exactly the declared columns, in order, and no others", async () => {
    // The compile-time half of RFC-006 §11 lives in `contract.ts` (`keyof` the schema type with its
    // index signature stripped). It is bypassable: a screen can cast and hand-write a `<td>` for a
    // field the contract never declared, and the type system never sees it. This asserts against
    // the rendered DOM instead, so an extra column is a failure wherever it was added.
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    const headers = [...screen.getAllByRole("columnheader")].map((cell) => cell.textContent?.trim());
    expect(headers).toEqual(IMPOSTER_COLUMNS.map((column) => column.label));

    // A viewer gets no lifecycle column, so the cell count is exactly the declared columns.
    const cells = within(screen.getByTestId("imposter-row-4545")).getAllByRole("cell");
    expect(cells.length).toBe(IMPOSTER_COLUMNS.length);
  });

  it("adds exactly one column for the lifecycle control, and only for a role that holds it", async () => {
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");

    const cells = within(screen.getByTestId("imposter-row-4545")).getAllByRole("cell");
    expect(cells.length).toBe(IMPOSTER_COLUMNS.length + 1);
  });
});

describe("dense tables (200 imposters, 40-character names, narrow window)", () => {
  it("truncates a long name for display but keeps the whole value reachable", async () => {
    const long = "checkout-service-integration-sandbox-eu-1";
    stubFetch({
      "/imposters": { json: { imposters: [{ port: 4545, protocol: "http", name: long, recordRequests: false, enabled: true }] } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    const cell = await screen.findByTestId("imposter-name-4545");
    expect(cell.textContent!.length).toBeLessThan(long.length);
    expect(cell.getAttribute("title")).toBe(long);
  });

  it("renders 200 rows without dropping any", async () => {
    const many = Array.from({ length: 200 }, (_, i) => ({
      port: 4000 + i,
      protocol: "http",
      name: `imposter-${i}`,
      recordRequests: false,
      enabled: true,
    }));
    stubFetch({ "/imposters": { json: { imposters: many } }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    await screen.findByText("4000");
    expect(screen.getAllByTestId(/^imposter-row-/).length).toBe(200);
  });
});
describe("a parked write is not reported as saved (#211)", () => {
  /*
   * Under `--cluster-admin-async` the lifecycle routes answer `202` the moment the write is parked,
   * before it has committed. The console used to render that identically to a `200`, so the row
   * settled and the operator was told the change had landed while it was still in flight.
   */
  it("follows the op id a 202 hands back instead of settling immediately", async () => {
    const { calls } = stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { json: { opId: "op-7" }, status: 202 },
      "/_fleet/ops/op-7": { json: { state: "applied", revision: 12 } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });
    await screen.findByText("billing");

    await userEvent.setup().click(screen.getByRole("button", { name: /disable billing/i }));

    await waitFor(() => expect(calls).toContain("/_fleet/ops/op-7"));
    // Applied is the ordinary outcome, so there is nothing to caveat on screen.
    await waitFor(() => expect(screen.queryByTestId("write-unconfirmed")).toBeNull());
  });

  it("surfaces the fleet's own reason when the parked write is refused", async () => {
    stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { json: { opId: "op-8" }, status: 202 },
      "/_fleet/ops/op-8": {
        json: { state: "failed", revision: 3, detail: "port claimed by another tenant" },
      },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });
    await screen.findByText("billing");

    await userEvent.setup().click(screen.getByRole("button", { name: /disable billing/i }));

    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("port claimed by another tenant");
  });

  it("says accepted-not-confirmed when this principal cannot read the op status", async () => {
    /*
     * The case that makes the three-valued outcome load-bearing. `/_fleet/ops/*` is fleet-scoped and
     * answers 404 to everyone else, so an ordinary operator's write is accepted, almost certainly
     * commits, and simply cannot be watched. Rendering that as failure would be the filed bug
     * inverted — an outcome asserted without observing it.
     */
    stubFetch({
      "/imposters": { json: TWO },
      "/imposters/4545/disable": { json: { opId: "op-9" }, status: 202 },
      "/_fleet/ops/op-9": { status: 404 },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");

    await userEvent.setup().click(screen.getByRole("button", { name: /disable billing/i }));

    const note = await screen.findByTestId("write-unconfirmed");
    expect(note.textContent).toContain("Accepted, not yet confirmed");
    // Not an error: nothing was refused, and role=alert would say otherwise to a screen reader.
    expect(note.getAttribute("role")).toBe("status");
  });
});

describe("#251 — import", () => {
  const FLEET = { "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } };

  /** Render the list and open the import panel, which lives behind a toggle. */
  async function openImport(role: "editor" | "viewer" | "fleet-admin" = "editor"): Promise<void> {
    renderInApp(<Imposters />, { whoami: whoamiWith(role) });
    await screen.findByText("billing");
    const toggle = screen.queryByTestId("open-import");
    if (toggle !== null) await userEvent.setup().click(toggle);
  }

  async function paste(text: string): Promise<void> {
    const user = userEvent.setup();
    const box = screen.getByLabelText("Imposter JSON to import");
    await user.clear(box);
    await user.click(box);
    await user.paste(text);
  }

  it("says what an import would do before anything is written", async () => {
    /*
     * The pre-flight is the whole point: `Add` is refused per-imposter by the port check while
     * `Replace all` succeeds and destroys what was there, and neither outcome is visible from the
     * document alone. Naming the overlap first is what makes the choice informed.
     */
    stubFetch({ "/imposters": { json: TWO }, ...FLEET });
    await openImport();
    await paste(JSON.stringify({ imposters: [{ port: 4545, protocol: "http" }, { port: 9000, protocol: "http" }] }));

    const preflight = await screen.findByTestId("import-preflight");
    expect(preflight.textContent).toContain("9000");
    // 4545 is already on screen, so it must be called out as a collision.
    expect(preflight.textContent).toContain("4545");
    expect(preflight.textContent).toMatch(/already|exist|collision/i);
  });

  it("names the JSON error and refuses to apply a document it could not read", async () => {
    stubFetch({ "/imposters": { json: TWO }, ...FLEET });
    await openImport();
    await paste("{not json");

    expect((await screen.findByTestId("import-error")).textContent).toMatch(/not valid JSON/i);
    expect((screen.getByTestId("import-add") as HTMLButtonElement).disabled).toBe(true);
  });

  it("reports each imposter's outcome individually when one of a batch fails", async () => {
    /*
     * The worst outcome here is a half-applied batch that does not say which half. A port collision
     * refuses one imposter; the rest must still be attempted, and the panel must name both sides.
     */
    stubFetch({
      "/imposters": { json: TWO },
      ...FLEET,
    });
    // `stubFetch` answers by path, so both POSTs hit `/imposters`. Override with a fetch double that
    // fails the first write and accepts the second, which is the case this test exists for.
    const realFetch = globalThis.fetch as unknown as (...args: unknown[]) => Promise<Response>;
    let writes = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        if ((init?.method ?? "GET") !== "POST") return realFetch(input, init);
        writes += 1;
        return writes === 1
          ? Promise.resolve(new Response("port 4545 is already claimed", { status: 400 }))
          : Promise.resolve(new Response("{}", { status: 201 }));
      }),
    );

    await openImport();
    await paste(JSON.stringify({ imposters: [{ port: 4545, protocol: "http" }, { port: 9000, protocol: "http" }] }));
    await userEvent.setup().click(screen.getByTestId("import-add"));

    const results = await screen.findByTestId("import-results");
    await waitFor(() => expect(results.textContent).toContain("9000"));
    expect(results.textContent).toContain("4545");
    // The server's own words, not a rewrite of them.
    expect(results.textContent).toContain("already claimed");
    expect(writes).toBe(2);
  });

  it("offers replace-all to a role holding imposter.delete", async () => {
    /*
     * The previous version of this test could not fail: it rendered the default role (which holds
     * everything), then asserted `expect(x).toBeTruthy()` inside `if (x !== null)`. Both branches
     * passed unconditionally, so it read as coverage of the authorization rule while checking
     * nothing. Asserted directly now, in both directions — see the viewer case below.
     */
    stubFetch({ "/imposters": { json: TWO }, ...FLEET });
    await openImport("editor");
    await paste(JSON.stringify({ port: 9000, protocol: "http" }));

    expect(await screen.findByTestId("import-add")).toBeTruthy();
    expect(screen.getByTestId("import-replace")).toBeTruthy();
  });

  it("names the count it is about to destroy before replacing everything", async () => {
    // Replace-all is the destructive one, and the number it destroys is not visible from the
    // document being imported — it is a fact about the fleet.
    stubFetch({ "/imposters": { json: TWO }, ...FLEET });
    await openImport("editor");
    await paste(JSON.stringify({ port: 9000, protocol: "http" }));
    await userEvent.setup().click(screen.getByTestId("import-replace"));

    const confirm = await screen.findByTestId("confirm-replace-imposters");
    // Two imposters are on screen; the confirm must say so rather than just "are you sure".
    expect(confirm.textContent).toContain("2");
  });

  it("offers a viewer no import at all", async () => {
    stubFetch({ "/imposters": { json: TWO }, ...FLEET });
    await openImport("viewer");
    // No toggle for a viewer, so no panel — and none reachable by any other route either.
    expect(screen.queryByTestId("open-import")).toBeNull();
    expect(screen.queryByTestId("import-panel")).toBeNull();
    // Export is a read affordance and stays.
    expect(screen.getByTestId("export-imposters")).toBeTruthy();
  });
});
