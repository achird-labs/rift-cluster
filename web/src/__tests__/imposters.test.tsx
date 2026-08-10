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

  /*
   * The list projection, as the fleet actually sends it: `stubCount`, no `stubs` array.
   *
   * Every other fixture in this file hands the list a `stubs` array, which no real
   * `GET /imposters` response carries — which is exactly why a Stubs column that read only
   * `stubs` passed its tests and rendered `—` for every row against a live cluster.
   */
  const LIST_SHAPE = {
    imposters: [
      { port: 4545, protocol: "http", name: "billing", recordRequests: false, enabled: true, stubCount: 3 },
      { port: 4546, protocol: "http", name: "empty", recordRequests: false, enabled: true, stubCount: 0 },
      { port: 4547, protocol: "http", name: "silent", recordRequests: false, enabled: true },
    ],
  };

  it("counts stubs from stubCount on the list, where there is no stubs array", async () => {
    stubFetch({
      "/imposters": { json: LIST_SHAPE },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    const billing = within(await screen.findByTestId("imposter-row-4545"));
    expect(billing.getByText("3")).toBeTruthy();
  });

  it("shows zero stubs as 0, not as unknown", async () => {
    stubFetch({
      "/imposters": { json: LIST_SHAPE },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    // The absent-vs-zero distinction this column already cared about, now reachable: `0 ?? UNKNOWN`
    // must stay `0`, which a truthiness check would have turned into `—`.
    const empty = within(await screen.findByTestId("imposter-row-4546"));
    expect(empty.getByText("0")).toBeTruthy();
  });

  it("still says unknown when the response carries neither stubs nor stubCount", async () => {
    stubFetch({
      "/imposters": { json: LIST_SHAPE },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });

    /*
     * Scoped to the stubs cell, not the row.
     *
     * It used to search the whole row for an em dash, which held only while the row contained
     * exactly one. `Provenance` renders a dash too — nothing declared this imposter — so a row-wide
     * search finds two and cannot say which column was unknown. Naming the cell asserts the thing
     * the test is actually about.
     */
    expect((await screen.findByTestId("imposter-cell-stubs-4547")).textContent).toBe("—");
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

/**
 * Columns the table renders that are NOT imposter fields, enumerated so they cannot grow silently.
 *
 * `contract.ts` governs columns whose value comes out of the imposter document — that is what its
 * `keyof Declared<Imposter>` key type enforces. This one does not:
 *
 * - `Provenance` is a **join** against `/admin/sources` — `ports` and `drifted` are that endpoint's
 *   declared fields, read through `sourceOwnedPorts` and `driftedPorts`, never off the imposter.
 *
 * There was an `Owner` column here too, pending on #359. It is gone: an imposter has no owner.
 * Config and stubs are replicated to every node, so every node serves them; only a *flow* is owned,
 * and a port has as many owners as it has flows. A column that can never be filled is a promise
 * rather than a roadmap, so it was removed rather than left pending.
 *
 * Listing them here keeps the property the test below exists for: a column added without a source
 * still fails, because it would have to be added to this array first and that is a sentence someone
 * has to justify.
 */
const DERIVED_COLUMNS = ["Provenance"] as const;

describe("every rendered cell comes from the declared column table", () => {
  it("renders exactly the declared columns, in order, and no others", async () => {
    // The compile-time half of RFC-006 §11 lives in `contract.ts` (`keyof` the schema type with its
    // index signature stripped). It is bypassable: a screen can cast and hand-write a `<td>` for a
    // field the contract never declared, and the type system never sees it. This asserts against
    // the rendered DOM instead, so an extra column is a failure wherever it was added.
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    // The sort affordance (#252) appends an `aria-hidden` arrow to whichever column is sorted, so
    // the label is compared with it stripped. Stripping it is not weakening the assertion: an
    // undeclared *column* still fails, which is what this test is for.
    const headers = [...screen.getAllByRole("columnheader")].map((cell) =>
      cell.textContent?.replace(/[\u25b2\u25bc]/g, "").trim(),
    );
    expect(headers).toEqual([
      ...IMPOSTER_COLUMNS.map((column) => column.label),
      ...DERIVED_COLUMNS,
    ]);

    // A viewer gets neither the lifecycle column nor the bulk-selection one (it holds none of the
    // bulk actions), so the cell count is exactly the declared columns.
    const cells = within(screen.getByTestId("imposter-row-4545")).getAllByRole("cell");
    expect(cells.length).toBe(IMPOSTER_COLUMNS.length + DERIVED_COLUMNS.length);
  });

  it("adds exactly two control columns for a role that holds the actions, and no data column", async () => {
    /*
     * An operator holds `imposter.lifecycle` and `requests.clear`, so the row carries two columns
     * that are not data: the bulk-selection checkbox (#252) and the lifecycle control. Both are
     * named here rather than left as a bare `+2`, so a third one appearing has to be justified by
     * editing this sentence.
     */
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });
    await screen.findByText("billing");

    const cells = within(screen.getByTestId("imposter-row-4545")).getAllByRole("cell");
    expect(cells.length).toBe(IMPOSTER_COLUMNS.length + DERIVED_COLUMNS.length + 2);
    expect(screen.getByTestId("imposter-select-4545")).toBeTruthy();
  });
});

describe("an imposter with no name", () => {
  // `name` is optional on `POST /imposters`, and imported configs and imposter sources routinely
  // omit it. The name cell is the list's ONLY route to the detail screen, so a nameless imposter
  // that renders an unlinked `—` is unreachable: no stub editing, no recording panel, no export,
  // and a row that silently ignores clicks with nothing on screen to say why.
  const NAMELESS = {
    imposters: [{ port: 4545, protocol: "http", recordRequests: false, enabled: true, stubs: [{}] }],
  };

  it("still links through to the detail screen", async () => {
    stubFetch({
      "/imposters": { json: NAMELESS },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const cell = await screen.findByTestId("imposter-name-4545");
    const link = cell.closest("a");
    expect(link).not.toBeNull();
    expect(link!.getAttribute("href")).toBe("#/imposters/4545");
  });

  it("labels the cell as unnamed rather than unknown, and names the port for a screen reader", async () => {
    stubFetch({
      "/imposters": { json: NAMELESS },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const cell = await screen.findByTestId("imposter-name-4545");
    // `—` is "the response did not tell us"; this imposter genuinely has no name, and a link
    // labelled `—` announces as nothing.
    expect(cell.textContent).toBe("(unnamed)");
    expect(cell.closest("a")!.getAttribute("aria-label")).toBe(
      "Open unnamed imposter on port 4545",
    );
  });

  it("leaves a named imposter's link unlabelled, since its text already names it", async () => {
    stubFetch({ "/imposters": { json: TWO }, "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const cell = await screen.findByTestId("imposter-name-4545");
    expect(cell.closest("a")!.getAttribute("aria-label")).toBeNull();
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

describe("the fleet-sum request tile (#363)", () => {
  const FLEET = { "/_fleet/members": { status: 404 }, "/_fleet/health": { status: 404 } };

  function withCounts(a: number | undefined, b: number | undefined): Record<string, unknown> {
    return {
      imposters: TWO.imposters.map((imposter, i) => ({
        ...imposter,
        ...(i === 0 ? (a === undefined ? {} : { numberOfRequests: a }) : b === undefined ? {} : { numberOfRequests: b }),
      })),
    };
  }

  it("sums every imposter's count and says the sum spans the fleet", async () => {
    stubFetch({ ...FLEET, "/imposters": { json: withCounts(7, 5) } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    expect((await screen.findByTestId("tile-requests")).textContent).toBe("12");
    expect(screen.getByText(/summed across every node/i)).toBeTruthy();
  });

  /*
   * The reason this issue needed the partial header at all. The fan-out stamps
   * `Rift-Cluster-Partial` when a node did not answer in time, and the sum is then a floor. Showing
   * `12` under a label reading "fleet sum" would report a total the fleet never confirmed.
   */
  it("says the sum is a floor when a node did not answer", async () => {
    stubFetch({
      ...FLEET,
      "/imposters": { json: withCounts(7, 5), headers: { "rift-cluster-partial": "true" } },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    expect((await screen.findByTestId("tile-requests")).textContent).toBe("12");
    expect(screen.getByText(/at least this many/i)).toBeTruthy();
    expect(screen.queryByText(/summed across every node/i)).toBeNull();
  });

  // A complete merge says nothing — a caveat that is always on is one nobody reads on the day it
  // means something, which is the rule the request log's scope strip already follows.
  it("carries no caveat when the merge reached every node", async () => {
    stubFetch({ ...FLEET, "/imposters": { json: withCounts(7, 5) } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    await screen.findByTestId("tile-requests");
    expect(screen.queryByText(/at least this many/i)).toBeNull();
  });

  /*
   * `numberOfRequests` is optional in the contract, so a row without one has an *unknown* count.
   * Summing it as zero would understate the fleet total while looking like an answer — the same
   * trap the stub-count tile beside it documents.
   */
  it("shows no total when a row carried no count at all", async () => {
    stubFetch({ ...FLEET, "/imposters": { json: withCounts(7, undefined) } });
    renderInApp(<Imposters />, { whoami: whoamiWith("viewer") });
    await screen.findByText("billing");

    const tile = await screen.findByTestId("tile-requests");
    expect(tile.textContent).not.toBe("7");
    expect(screen.getByText(/not every imposter in this response carried a count/i)).toBeTruthy();
  });
});

describe("the control-plane panel's per-voter applied indices (#361)", () => {
  const MEMBERS = {
    node_id: "2",
    is_leader: false,
    current_leader: "1",
    last_applied: 412,
    voters: ["1", "2", "3"],
    members: [
      { node_id: "1", last_applied: 415, is_leader: true, reachable: true },
      { node_id: "2", last_applied: 412, is_leader: false, reachable: true },
      { node_id: "3", last_applied: null, is_leader: null, reachable: false },
    ],
  };
  const HEALTH = {
    ready: true,
    state: "ready",
    pending_gates: [],
    isolated: false,
    ring: { m_idx: 7, members: ["1", "2", "3"] },
  };

  it("shows each voter's own applied index, not just this node's", async () => {
    stubFetch({
      "/imposters": { json: TWO },
      "/_fleet/members": { json: MEMBERS },
      "/_fleet/health": { json: HEALTH },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    // The peer's index is the point: before #361 only this node's row carried a number.
    expect((await screen.findByTestId("applied-1")).textContent).toBe("415");
    expect((await screen.findByTestId("applied-2")).textContent).toBe("412");
  });

  /*
   * A voter that did not answer has an unknown index. Rendering `0` would say "that node has
   * applied nothing" — an alarm about the fleet raised by a fan-out that merely timed out.
   */
  it("renders an unreachable voter as unknown, never as zero", async () => {
    stubFetch({
      "/imposters": { json: TWO },
      "/_fleet/members": { json: MEMBERS },
      "/_fleet/health": { json: HEALTH },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    const cell = await screen.findByTestId("applied-3");
    expect(cell.textContent).toBe("—");
    expect(cell.textContent).not.toBe("0");
    // And it says which of the two unknowns it is.
    expect(cell.getAttribute("title")).toMatch(/did not answer/i);
  });

  // The list is the membership. A voter whose row is missing must still appear, or the panel
  // reports a fleet that shrank when the truth is one node was slow.
  it("still lists a voter the projection carried no row for", async () => {
    stubFetch({
      "/imposters": { json: TWO },
      "/_fleet/members": { json: { ...MEMBERS, members: [MEMBERS.members[0]] } },
      "/_fleet/health": { json: HEALTH },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("applied-1")).textContent).toBe("415");
    expect((await screen.findByTestId("applied-3")).textContent).toBe("—");
  });
});
