/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TENANT_HEADER } from "../api/client.ts";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { onDetailTab, renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const IMPOSTER = {
  port: 4545,
  host: "0.0.0.0",
  protocol: "http",
  name: "billing",
  recordRequests: true,
  enabled: true,
  stubs: [
    { id: "s-1", routePattern: "/users/:id", predicates: [{}], responses: [{}, {}] },
    { scenarioName: "checkout", predicates: [], responses: [{}] },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
  // The detail screen's tab lives in the hash, so a test that opened one would otherwise leave the
  // next one on it — an order dependence that reads as an unrelated failure.
  window.location.hash = "";
});

describe("imposter detail", () => {
  it("reads the addressed imposter under the tenant in view", async () => {
    stubFetch({ "/imposters/4545": { json: IMPOSTER } });
    renderInApp(<ImposterDetail port={4545} />, {
      whoami: whoamiWith("viewer", ["acme", "globex"]),
      tenant: "globex",
      tenants: ["acme", "globex"],
    });
    await screen.findByText("billing");

    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    const read = mock.mock.calls.find(([path]) => path === "/imposters/4545");
    expect((read?.[1]?.headers as Record<string, string>)[TENANT_HEADER]).toBe("globex");
  });

  it("shows the contract's fields, including the host the list omits", async () => {
    // The fields are imposter configuration, which the detail screen keeps on its Settings tab.
    onDetailTab("settings");
    stubFetch({ "/imposters/4545": { json: IMPOSTER } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });

    expect((await screen.findByTestId("detail-name")).textContent).toBe("billing");
    expect(screen.getByTestId("detail-protocol").textContent).toBe("http");
    expect(screen.getByTestId("detail-host").textContent).toBe("0.0.0.0");
    expect(screen.getByTestId("detail-stubs").textContent).toBe("2");
  });

  it("lists stubs, and never invents an id for one that has none", async () => {
    // A stub without a Rift id is shown as having none rather than being labelled by its position,
    // which is not an address — C5 (#188) is why that distinction now has teeth: the write controls
    // are offered per id, and a stub without one gets them inert (see `stub-editor.test.tsx`).
    stubFetch({ "/imposters/4545": { json: IMPOSTER } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("editor") });

    await screen.findByTestId("stub-row-0");
    expect(screen.getByTestId("stub-row-0").textContent).toContain("s-1");
    expect(screen.getByTestId("stub-row-0").textContent).toContain("/users/:id");
    expect(screen.getByTestId("stub-row-1").textContent).toContain("—");
    expect(screen.getByTestId("stub-row-1").textContent).toContain("checkout");
  });

  it("offers a viewer no write control, while still allowing a read-only export", async () => {
    /*
     * This used to assert `queryByRole("button")` was null — a proxy for "no write controls" that
     * held only because the screen happened to have no buttons of any kind. #251 added export, which
     * is a READ affordance gated on `imposter.read`, so a viewer legitimately gets it now and the
     * blanket assertion would fail for the right reason.
     *
     * Narrowed to the write affordances by name, and the export is asserted PRESENT rather than
     * merely tolerated: `rbac.ts` makes the point repeatedly that hiding a control from a role that
     * holds the capability is the same class of bug as offering one to a role that does not.
     *
     * The two halves now sit on different tabs — the stubs a viewer may read, the export among the
     * imposter's settings — so the test walks to the second rather than asserting both at once. It
     * is deliberately one test still: "no write control, but the read affordance is offered" is a
     * single claim about a role, and splitting it would let half of it pass alone.
     */
    stubFetch({ "/imposters/4545": { json: IMPOSTER } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });

    await screen.findByTestId("stub-row-0");
    for (const name of [/add stub/i, /edit /i, /delete /i, /duplicate/i, /disable/i, /enable/i]) {
      expect([name, screen.queryByRole("button", { name })]).toEqual([name, null]);
    }
    expect(screen.queryByTestId("clone-imposter")).toBeNull();

    await userEvent.setup().click(screen.getByTestId("detail-tab-settings"));
    expect(await screen.findByTestId("export-imposter")).toBeTruthy();
  });

  it("distinguishes an imposter with no stubs from a response that carried no stub list", async () => {
    // Two different facts. `stubs` is optional in the contract, so absent is "the response did not
    // include them" — rendering that as "no stubs" would assert something the body never said.
    stubFetch({ "/imposters/4545": { json: { ...IMPOSTER, stubs: [] } } });
    const { unmount } = renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });
    // Asserted on the whole empty-state panel rather than one text node: the heading and the
    // sentence that carries the meaning are separate elements, and matching only the heading would
    // pass for a panel that had lost its explanation.
    expect((await screen.findByTestId("imposter-no-stubs")).textContent).toMatch(
      /no stubs[\s\S]*falls through/i,
    );
    unmount();

    const { stubs: _dropped, ...withoutStubs } = IMPOSTER;
    stubFetch({ "/imposters/4545": { json: withoutStubs } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });
    expect(await screen.findByText(/carried no stub list/i)).toBeTruthy();
  });

  it("reports a refused read rather than rendering an empty imposter", async () => {
    stubFetch({ "/imposters/4545": { status: 404, json: { message: "not found" } } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });

    expect((await screen.findByRole("alert")).textContent).toMatch(/could not read this imposter/i);
  });
});
