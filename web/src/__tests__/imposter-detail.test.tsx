/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TENANT_HEADER } from "../api/client.ts";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

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

  it("offers a viewer no write control at all", async () => {
    stubFetch({ "/imposters/4545": { json: IMPOSTER } });
    renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });

    await screen.findByTestId("stub-row-0");
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("distinguishes an imposter with no stubs from a response that carried no stub list", async () => {
    // Two different facts. `stubs` is optional in the contract, so absent is "the response did not
    // include them" — rendering that as "no stubs" would assert something the body never said.
    stubFetch({ "/imposters/4545": { json: { ...IMPOSTER, stubs: [] } } });
    const { unmount } = renderInApp(<ImposterDetail port={4545} />, { whoami: whoamiWith("viewer") });
    expect((await screen.findByText(/No stubs\./i)).textContent).toMatch(/falls through/i);
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
