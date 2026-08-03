/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { Imposters } from "../screens/Imposters.tsx";
import { createQueryClient } from "../app/query.ts";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/**
 * The list screen's search, sort and bulk behaviour.
 *
 * `features/imposters/list.ts` and `bulk.ts` already own — and unit-test — what a filter *means* and
 * what a batch *does*. What can only be tested here is the wiring: that the URL carries the filter,
 * that select-all means "what is shown", and that a partially-refused batch reaches the operator as
 * a per-item report rather than as a green summary.
 */

const PROXY = { responses: [{ proxy: { to: "https://upstream.example" } }] };
const IS = { responses: [{ is: { statusCode: 200 } }] };

const THREE = {
  imposters: [
    { port: 4547, protocol: "http", name: "mid-shipping", recordRequests: false, enabled: true, stubs: [IS] },
    { port: 4545, protocol: "http", name: "zeta-billing", recordRequests: true, enabled: true, stubs: [PROXY, IS] },
    { port: 4546, protocol: "https", name: "alpha-checkout", recordRequests: false, enabled: false, stubs: [] },
  ],
};

function base(): Parameters<typeof stubFetch>[0] {
  return {
    "/imposters": { json: THREE },
    "/_fleet/members": { status: 404 },
    "/_fleet/health": { status: 404 },
  };
}

beforeEach(() => {
  window.location.hash = "#/imposters";
});

afterEach(() => {
  vi.unstubAllGlobals();
  window.location.hash = "";
});

async function rendered(role: "editor" | "viewer" | "fleet-admin" = "fleet-admin"): Promise<void> {
  renderInApp(<Imposters />, { whoami: whoamiWith(role) });
  await screen.findByTestId("imposter-row-4545");
}

function visiblePorts(): number[] {
  return screen
    .getAllByTestId(/^imposter-row-/)
    .map((row) => Number(row.getAttribute("data-testid")?.replace("imposter-row-", "")));
}

describe("filtering", () => {
  it("narrows the list as you type and reflects the filter in the URL", async () => {
    stubFetch(base());
    await rendered();

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "bill");

    await waitFor(() => expect(visiblePorts()).toEqual([4545]));
    // Linkable: the filter is in the hash, not only in component state.
    expect(window.location.hash).toContain("q=bill");
  });

  it("restores a filter from the URL on first render", async () => {
    // The half that a state-only filter fails: reload the page and the view comes back.
    window.location.hash = "#/imposters?q=checkout";
    stubFetch(base());
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(visiblePorts()).toEqual([4546]));
    expect((screen.getByTestId("imposter-filter-text") as HTMLInputElement).value).toBe("checkout");
  });

  it("clears back to a URL with no query string at all", async () => {
    window.location.hash = "#/imposters?q=bill&sort=name";
    stubFetch(base());
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });
    await screen.findByTestId("imposter-row-4545");

    await userEvent.click(screen.getByTestId("imposter-filter-reset"));

    await waitFor(() => expect(visiblePorts()).toHaveLength(3));
    expect(window.location.hash).toBe("#/imposters");
  });

  it("renders rather than throwing for a hand-edited filter", async () => {
    window.location.hash = "#/imposters?sort=colour&dir=widdershins&rec=maybe";
    stubFetch(base());
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(visiblePorts()).toHaveLength(3));
  });

  it("says so when a filter matches nothing, instead of showing an empty table", async () => {
    stubFetch(base());
    await rendered();

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "nothing-here");

    expect(await screen.findByTestId("imposters-no-matches")).toBeTruthy();
  });

  it("names the rows a recording filter could not classify", async () => {
    // An imposter whose stubs were not in the list response cannot be classified either way.
    // Dropping it silently is the failure mode; saying so is the requirement.
    stubFetch({
      ...base(),
      "/imposters": {
        json: {
          imposters: [
            { port: 4545, protocol: "http", name: "billing", recordRequests: true, enabled: true, stubs: [PROXY] },
            { port: 4546, protocol: "http", name: "checkout", recordRequests: true, enabled: true },
          ],
        },
      },
    });
    await rendered();

    await userEvent.selectOptions(screen.getByTestId("imposter-filter-recording"), "has");

    await waitFor(() => expect(visiblePorts()).toEqual([4545]));
    expect((await screen.findByTestId("imposter-filter-unclassified")).textContent).toMatch(
      /did not include their stubs/i,
    );
  });
});

describe("source provenance", () => {
  it("offers the origin filter and narrows by it when sources are readable", async () => {
    stubFetch({
      ...base(),
      "/admin/sources": {
        json: { sources: [{ id: "mocks", uri: "git+https://x/y", mode: "pinned", onDrift: "skip", drifted: false, ports: [4545], revision: 1 }], nodeLocal: {} },
      },
    });
    await rendered();

    await userEvent.selectOptions(await screen.findByTestId("imposter-filter-owner"), "source");
    await waitFor(() => expect(visiblePorts()).toEqual([4545]));

    await userEvent.selectOptions(screen.getByTestId("imposter-filter-owner"), "hand");
    await waitFor(() => expect(visiblePorts()).toEqual([4546, 4547]));
  });

  it("does not offer the origin filter to a principal who cannot read sources", async () => {
    // Hidden, not broken. Without the join the filter could only ever answer "hand-created" for
    // everything, which is a wrong answer dressed as a real one.
    stubFetch(base());
    await rendered("viewer");

    expect(screen.queryByTestId("imposter-filter-owner")).toBeNull();
  });
});

describe("sorting", () => {
  it("sorts by a column and flips direction on a second click", async () => {
    stubFetch(base());
    await rendered();

    // Default is port ascending.
    expect(visiblePorts()).toEqual([4545, 4546, 4547]);

    // alpha-checkout(4546), mid-shipping(4547), zeta-billing(4545) — deliberately NOT port order.
    await userEvent.click(screen.getByTestId("imposter-sort-name"));
    await waitFor(() => expect(visiblePorts()).toEqual([4546, 4547, 4545]));

    await userEvent.click(screen.getByTestId("imposter-sort-name"));
    await waitFor(() => expect(visiblePorts()).toEqual([4545, 4547, 4546]));
    expect(window.location.hash).toContain("dir=desc");
  });

  it("announces the sorted column and its DIRECTION to assistive technology", async () => {
    // Both branches, because a header stuck on "ascending" while the arrow shows ▼ tells a
    // screen-reader user the opposite of what everyone else can see.
    stubFetch(base());
    await rendered();

    await userEvent.click(screen.getByTestId("imposter-sort-stubs"));
    const header = (): Element | null => screen.getByTestId("imposter-sort-stubs").closest("th");
    await waitFor(() => expect(header()?.getAttribute("aria-sort")).toBe("ascending"));

    await userEvent.click(screen.getByTestId("imposter-sort-stubs"));
    await waitFor(() => expect(header()?.getAttribute("aria-sort")).toBe("descending"));

    // And exactly one column claims a sort at a time.
    expect(document.querySelectorAll('th[aria-sort="ascending"], th[aria-sort="descending"]')).toHaveLength(1);
  });
});

describe("selection", () => {
  it("select-all takes exactly what the filter shows, and the count agrees", async () => {
    stubFetch(base());
    await rendered();

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "ing"); // billing, shipping
    await waitFor(() => expect(visiblePorts()).toEqual([4545, 4547]));

    await userEvent.click(screen.getByTestId("imposter-select-all"));

    expect((await screen.findByTestId("imposter-bulk-count")).textContent).toBe("2 selected");
  });

  it("narrowing the filter narrows the selection, so the count is never a lie", async () => {
    // Otherwise: select everything, narrow the filter, press Delete, and imposters you cannot
    // see are deleted. The count on the bar is the count acted on, by construction.
    stubFetch(base());
    await rendered();

    await userEvent.click(screen.getByTestId("imposter-select-all"));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("3 selected");

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "zeta");

    await waitFor(() =>
      expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("1 selected"),
    );
  });

  it("select-all does not quietly select rows the filter was hiding", async () => {
    /*
     * The intersection with the visible rows keeps the *count* honest while a filter is applied,
     * which masks this: if select-all reached past the filter, widening the filter afterwards would
     * suddenly reveal a selection the operator never made — and the next click is a bulk delete.
     * So the assertion is made after the filter is cleared, where nothing hides it.
     */
    stubFetch(base());
    await rendered();

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "zeta");
    await waitFor(() => expect(visiblePorts()).toEqual([4545]));
    await userEvent.click(screen.getByTestId("imposter-select-all"));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("1 selected");

    await userEvent.click(screen.getByTestId("imposter-filter-reset"));

    await waitFor(() => expect(visiblePorts()).toHaveLength(3));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("1 selected");
    expect((screen.getByTestId("imposter-select-4545") as HTMLInputElement).checked).toBe(true);
    expect((screen.getByTestId("imposter-select-4546") as HTMLInputElement).checked).toBe(false);
  });

  it("shows no bulk bar until something is actually ticked", async () => {
    // A principal who HOLDS the actions but has selected nothing. Distinct from the viewer case
    // below, which short-circuits earlier — nothing was asserting the zero-count guard itself.
    stubFetch(base());
    await rendered();

    expect(screen.queryByTestId("imposter-bulk-bar")).toBeNull();
    await userEvent.click(screen.getByTestId("imposter-select-4545"));
    expect(await screen.findByTestId("imposter-bulk-bar")).toBeTruthy();
  });

  it("select-all UNIONS with ticks made under a previous filter", async () => {
    // Individual ticks accumulate across filter changes; a select-all that replaced the set would
    // silently discard them. The two paths have to agree or the count lies in one of them.
    stubFetch(base());
    await rendered();

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "zeta");
    await waitFor(() => expect(visiblePorts()).toEqual([4545]));
    await userEvent.click(screen.getByTestId("imposter-select-4545"));

    await userEvent.clear(screen.getByTestId("imposter-filter-text"));
    await userEvent.type(screen.getByTestId("imposter-filter-text"), "alpha");
    await waitFor(() => expect(visiblePorts()).toEqual([4546]));
    await userEvent.click(screen.getByTestId("imposter-select-all"));

    await userEvent.click(screen.getByTestId("imposter-filter-reset"));
    await waitFor(() => expect(visiblePorts()).toHaveLength(3));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("2 selected");
  });

  it("un-checking select-all clears only what is shown, not ticks held elsewhere", async () => {
    // The other direction of the same rule. If un-checking removed nothing, the box would be a
    // one-way switch; if it cleared everything, it would silently discard ticks the current filter
    // is not even showing — the mirror of the bug the union fixes.
    stubFetch(base());
    await rendered();

    await userEvent.click(screen.getByTestId("imposter-select-all"));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("3 selected");

    await userEvent.type(screen.getByTestId("imposter-filter-text"), "zeta");
    await waitFor(() => expect(visiblePorts()).toEqual([4545]));
    await userEvent.click(screen.getByTestId("imposter-select-all"));

    // 4545 released; 4546 and 4547 untouched behind the filter.
    expect(screen.queryByTestId("imposter-bulk-bar")).toBeNull();
    await userEvent.click(screen.getByTestId("imposter-filter-reset"));
    await waitFor(() => expect(visiblePorts()).toHaveLength(3));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("2 selected");
    expect((screen.getByTestId("imposter-select-4545") as HTMLInputElement).checked).toBe(false);
  });

  it("drops a tick when its imposter leaves the fleet, so a REUSED port is not silently selected", async () => {
    /*
     * Ports are identity here and they are reused constantly — a source pull, an import, another
     * operator. Ticking 4545, watching it be deleted, and seeing a *different* imposter appear at
     * 4545 must not leave the new one ticked and one click from a bulk delete nobody asked for.
     * `effective` intersecting with the visible rows keeps the count honest and is exactly what
     * would hide this.
     */
    let listing = THREE;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path === "/imposters") return Promise.resolve(new Response(JSON.stringify(listing)));
        return Promise.resolve(new Response(null, { status: 404 }));
      }),
    );
    // The list polls every 5s, so waiting two cycles out would make this the slowest test in the
    // suite by an order of magnitude. Invalidating drives the same refetch immediately — what is
    // under test is the reconciliation, not the cadence.
    const client = createQueryClient();
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin"), client });
    await screen.findByTestId("imposter-row-4545");

    await userEvent.click(screen.getByTestId("imposter-select-4545"));
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("1 selected");

    // 4545 leaves the fleet — deleted by a single-row action, another operator, or a source pull.
    listing = { imposters: THREE.imposters.filter((i) => i.port !== 4545) };
    await client.invalidateQueries({ queryKey: ["imposters"] });
    await waitFor(() => expect(screen.queryByTestId("imposter-row-4545")).toBeNull());

    // …and a DIFFERENT imposter is created at the same port.
    listing = {
      imposters: [
        ...THREE.imposters.filter((i) => i.port !== 4545),
        { port: 4545, protocol: "http", name: "someone-elses-service", recordRequests: false, enabled: true, stubs: [] },
      ],
    };
    await client.invalidateQueries({ queryKey: ["imposters"] });
    await waitFor(() => expect(screen.queryByTestId("imposter-row-4545")).not.toBeNull());

    expect((screen.getByTestId("imposter-select-4545") as HTMLInputElement).checked).toBe(false);
    expect(screen.queryByTestId("imposter-bulk-bar")).toBeNull();
  });

  it("offers no bulk bar to a principal who holds none of the actions", async () => {
    // Hidden, not disabled — the same rule the single-row actions already follow.
    stubFetch(base());
    await rendered("viewer");

    expect(screen.queryByTestId("imposter-select-all")).toBeNull();
    expect(screen.queryByTestId("imposter-bulk-bar")).toBeNull();
  });
});

describe("bulk actions", () => {
  async function selectAllAndDelete(): Promise<void> {
    await userEvent.click(screen.getByTestId("imposter-select-all"));
    await userEvent.click(screen.getByTestId("imposter-bulk-delete"));
  }

  it("confirms with the exact count and the exact ports before deleting", async () => {
    stubFetch(base());
    await rendered();

    await selectAllAndDelete();

    const confirm = await screen.findByTestId("confirm-bulk-imposters");
    expect(within(confirm).getByTestId("confirm-bulk-ports").textContent).toBe("4545, 4546, 4547");
    expect(confirm.textContent).toMatch(/one request per imposter/i);
    expect(confirm.textContent).toMatch(/3 imposters\?/);
  });

  it("reports per item, naming each refusal and its port, without halting", async () => {
    // The acceptance criterion this whole slice turns on: mixed outcomes reach the operator
    // individually. A single green toast over a partial failure is the thing being prevented.
    const { requests } = stubFetch({
      ...base(),
      "/imposters/4545": { status: 200 },
      "/imposters/4547": { status: 200 },
      "/imposters/4546": { status: 409, json: { message: "owned by source `mocks`" } },
    });
    await rendered();

    await selectAllAndDelete();
    await userEvent.click(await screen.findByRole("button", { name: /delete 3/i }));

    const report = await screen.findByTestId("imposter-bulk-report");
    await waitFor(() =>
      expect(screen.getByTestId("imposter-bulk-summary").textContent).toBe("2 deleted, 1 refused"),
    );
    expect(within(report).getByTestId("imposter-bulk-item-4546").textContent).toMatch(/refused/);
    // Not halted: the port after the refusal was still attempted.
    expect(requests.filter((r) => r.method === "DELETE").map((r) => r.path)).toEqual([
      "/imposters/4545",
      "/imposters/4546",
      "/imposters/4547",
    ]);
  });

  it("reports a parked write as still committing, never as deleted", async () => {
    // #211. A 202 with an op id the session cannot read is "accepted, not observed".
    stubFetch({
      ...base(),
      "/imposters/4545": { status: 202, json: { opIds: ["op-1"] } },
      "/_fleet/ops/op-1": { status: 403 },
    });
    await rendered();

    await userEvent.click(screen.getByTestId("imposter-select-4545"));
    await userEvent.click(screen.getByTestId("imposter-bulk-delete"));
    await userEvent.click(await screen.findByRole("button", { name: /delete 1/i }));

    await waitFor(() =>
      expect(screen.getByTestId("imposter-bulk-summary").textContent).toBe(
        "0 deleted, 1 still committing",
      ),
    );
    expect(screen.getByTestId("imposter-bulk-item-4545").textContent).toMatch(/still committing/i);
  });

  it("leaves refused items selected so they can be retried", async () => {
    stubFetch({
      ...base(),
      "/imposters/4545": { status: 200 },
      "/imposters/4547": { status: 200 },
      "/imposters/4546": { status: 409, json: { message: "nope" } },
    });
    await rendered();

    await selectAllAndDelete();
    await userEvent.click(await screen.findByRole("button", { name: /delete 3/i }));

    await waitFor(() =>
      expect(screen.getByTestId("imposter-bulk-summary").textContent).toBe("2 deleted, 1 refused"),
    );
    expect(screen.getByTestId("imposter-bulk-count").textContent).toBe("1 selected");
  });
});
