/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TENANT_HEADER } from "../api/client.ts";
import { Shell } from "../app/Shell.tsx";
import { ISSUE_URL, plannedEntries } from "../app/nav.ts";
import { TENANT_STORAGE_KEY, initialTenant } from "../app/session.tsx";
import { preferenceStore, resetPreferenceStore } from "../app/storage.ts";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const QUIET = {
  "/imposters": { json: { imposters: [] } },
  "/_fleet/members": { status: 404 },
  "/_fleet/health": { status: 404 },
};

afterEach(() => {
  vi.unstubAllGlobals();
  preferenceStore().removeItem(TENANT_STORAGE_KEY);
  resetPreferenceStore();
  window.location.hash = "";
});

describe("nav — a visible roadmap, not a 404", () => {
  it("renders every planned screen as a disabled entry carrying its issue link", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, { whoami: whoamiWith("fleet-admin") });

    for (const entry of plannedEntries()) {
      const item = await screen.findByTestId(`nav-${entry.id}`);
      expect(item.dataset.planned).toBe("true");
      expect(item.textContent).toContain(entry.label);
      expect(item.textContent).toContain(`#${entry.issue}`);

      // Exactly one link, and it goes to the issue — never to a console route. That is the whole
      // difference between "a visible roadmap" and "a nav entry that 404s".
      const links = within(item).getAllByRole("link");
      expect(links.length).toBe(1);
      expect(links[0]?.getAttribute("href")).toBe(ISSUE_URL(entry.issue));
    }
  });

  it("navigates between the live screens without a page load", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, { whoami: whoamiWith("fleet-admin") });

    await userEvent.setup().click(await screen.findByRole("link", { name: /^cluster$/i }));
    await waitFor(() => expect(window.location.hash).toBe("#/cluster"));
  });

  it("does not offer the cluster screen to a principal the fleet projection 404s", async () => {
    // Hiding it is UX only — `/_fleet/*` still 404s the same principal — but offering a screen that
    // can only ever render an error is worse than not offering it.
    stubFetch(QUIET);
    renderInApp(<Shell />, { whoami: whoamiWith("viewer") });

    await screen.findByTestId("nav-imposters");
    expect(screen.queryByTestId("nav-cluster")).toBeNull();
  });
});

describe("tenant switcher", () => {
  it("is absent for a single-tenant principal", async () => {
    // There is nothing to switch between, and an inert control implies otherwise.
    stubFetch(QUIET);
    renderInApp(<Shell />, { whoami: whoamiWith("editor", ["acme"]), tenants: ["acme"] });

    await screen.findByTestId("nav-imposters");
    expect(screen.queryByTestId("tenant-switcher")).toBeNull();
  });

  it("is present for a multi-tenant principal and lists their bound tenants", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, {
      whoami: whoamiWith("editor", ["acme", "globex"]),
      tenants: ["acme", "globex"],
    });

    const switcher = await screen.findByTestId("tenant-switcher");
    expect(within(switcher).getByRole("option", { name: "acme" })).toBeTruthy();
    expect(within(switcher).getByRole("option", { name: "globex" })).toBeTruthy();
  });

  it("re-issues reads under the newly selected tenant", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, {
      whoami: whoamiWith("editor", ["acme", "globex"]),
      tenants: ["acme", "globex"],
      tenant: "acme",
    });
    await screen.findByTestId("nav-imposters");

    await userEvent.setup().selectOptions(await screen.findByTestId("tenant-switcher"), "globex");

    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    await waitFor(() => {
      const tenants = mock.mock.calls
        .filter(([path]) => path === "/imposters")
        .map(([, init]) => (init.headers as Record<string, string>)[TENANT_HEADER]);
      expect(tenants).toContain("globex");
    });
  });

  it("persists the selection per browser, under a namespaced key", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, {
      whoami: whoamiWith("editor", ["acme", "globex"]),
      tenants: ["acme", "globex"],
      tenant: "acme",
    });
    await userEvent.setup().selectOptions(await screen.findByTestId("tenant-switcher"), "globex");

    await waitFor(() => expect(preferenceStore().getItem(TENANT_STORAGE_KEY)).toBe("globex"));
    // The next mount is what the operator experiences as "it remembered".
    expect(initialTenant(["acme", "globex"])).toBe("globex");
    expect(TENANT_STORAGE_KEY).toBe("rift-console.tenant");
  });

  it("ignores a remembered tenant the principal is no longer bound to", () => {
    // Otherwise every read goes out under an `X-Rift-Tenant` that 404s (RFC-002 §8.4) and the
    // console looks broken rather than re-defaulted.
    preferenceStore().setItem(TENANT_STORAGE_KEY, "a-tenant-they-lost");
    expect(initialTenant(["acme", "globex"])).toBe("acme");
  });

  it("never leaves the selection unset while there is a tenant to pick", () => {
    // The bug this pins: an unset selection sends no `X-Rift-Tenant`, so the request lands in
    // `default` — while the switcher, having nothing to display, would show the first tenant in
    // the list. The label and the header would then disagree on every read, and re-selecting the
    // displayed option fires no change event, so the operator could not even correct it.
    expect(initialTenant(["acme", "globex"])).toBe("acme");
    expect(initialTenant(["zeta", "default", "acme"])).toBe("default");
    expect(initialTenant([])).toBeNull();
  });

  it("shows the tenant the requests actually carry, not merely the first in the list", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, {
      whoami: whoamiWith("editor", ["acme", "globex"]),
      tenants: ["acme", "globex"],
      tenant: "globex",
    });

    const switcher = (await screen.findByTestId("tenant-switcher")) as HTMLSelectElement;
    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    await waitFor(() => {
      const read = mock.mock.calls.find(([path]) => path === "/imposters");
      expect((read?.[1]?.headers as Record<string, string>)[TENANT_HEADER]).toBe(switcher.value);
    });
  });
});

describe("identity", () => {
  it("shows the principal and the role it holds in the tenant in view", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />, { whoami: whoamiWith("operator", ["acme"]), tenants: ["acme"], tenant: "acme" });

    const identity = await screen.findByTestId("identity");
    expect(identity.textContent).toContain("p-test");
    expect(identity.textContent).toMatch(/operator/i);
  });

  it("says plainly when the fleet enforces nothing at all", async () => {
    // `authorizationDisabled` is a distinct fact from "an authenticated principal with no
    // bindings", and rendering it as an ordinary identity would hide an unsecured admin plane.
    stubFetch(QUIET);
    renderInApp(<Shell />, {
      whoami: { principalId: null, authorizationDisabled: true, bindings: [] },
    });

    const identity = await screen.findByTestId("identity");
    expect(identity.textContent).toMatch(/authorization disabled/i);
  });
});