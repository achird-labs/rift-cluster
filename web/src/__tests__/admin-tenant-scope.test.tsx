/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Shell } from "../app/Shell.tsx";
import { Admin } from "../screens/Admin.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

afterEach(() => {
  vi.unstubAllGlobals();
  window.location.hash = "";
});

describe("a tenant-admin can reach the surface its role exists for", () => {
  it("resolves the admin screen to the session's tenant when the hash names none", async () => {
    /*
     * The lockout this pins. The nav links to `#/admin/tenants` with no tenant, and the only
     * control that set one was a row in the Tenants tab — which lists `/admin/tenants`, a
     * `ClusterAdmin` route a TenantAdmin cannot read. So Principals and Bindings answered "Choose a
     * tenant to see this" forever, and the role whose entire grant is `PrincipalCreate` /
     * `BindingPut` had no route to either.
     */
    stubFetch({ "/admin/tenants/acme/principals": { json: [] } });
    renderInApp(<Admin tab="principals" tenant={null} />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
      tenants: ["acme"],
    });

    // It asked for the tenant's principals rather than refusing to choose one.
    expect(await screen.findByTestId("admin-screen")).toBeTruthy();
    expect(screen.queryByText(/choose a tenant/i)).toBeNull();
  });

  it("lets an explicit tenant in the hash win, so cross-tenant links keep working", async () => {
    // A fleet-admin navigating to another tenant must not be silently redirected to its own.
    stubFetch({ "/admin/tenants/globex/principals": { json: [] } });
    renderInApp(<Admin tab="principals" tenant="globex" />, {
      whoami: whoamiWith("fleet-admin"),
      tenant: "acme",
      tenants: ["acme", "globex"],
    });

    await screen.findByTestId("admin-screen");
    expect(
      vi.mocked(fetch).mock.calls.some(([i]) => String(i).includes("/globex/principals")),
    ).toBe(true);
  });
});

describe("the tenant in view is always visible", () => {
  it("names the tenant even when there is only one, where no switcher is drawn", async () => {
    /*
     * Every read on every screen is tenant-scoped. The switcher used to render nothing below two
     * tenants — correct about the *control*, wrong about the *fact*: a single-tenant principal, the
     * common case for a TenantAdmin, could not see which tenant it was working in anywhere.
     */
    stubFetch({
      "/imposters": { json: { imposters: [] } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Shell />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
      tenants: ["acme"],
    });

    expect((await screen.findByTestId("tenant-current")).textContent).toContain("acme");
    // And still no control, because there is nothing to switch to.
    expect(screen.queryByTestId("tenant-switcher")).toBeNull();
  });

  it("draws the switcher, not the label, once there is a choice", async () => {
    stubFetch({
      "/imposters": { json: { imposters: [] } },
      "/_fleet/members": { status: 404 },
      "/_fleet/health": { status: 404 },
    });
    renderInApp(<Shell />, {
      whoami: whoamiWith("editor", ["acme", "globex"]),
      tenant: "acme",
      tenants: ["acme", "globex"],
    });

    expect(await screen.findByTestId("tenant-switcher")).toBeTruthy();
    expect(screen.queryByTestId("tenant-current")).toBeNull();
  });
});
