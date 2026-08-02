import { expect, fixture, goToScreen, signIn, test } from "./fixture.ts";

/**
 * Layer 1: every screen loads in a real browser, as each role, with a clean console.
 *
 * The `test` fixture fails on any browser console error, so each of these is also asserting that no
 * CSP directive was violated, no chunk failed to load and no query rejected unhandled — the things
 * that are invisible to jsdom and only appear in the shipped artifact.
 */

const SCREENS = [
  { hash: "/imposters", heading: /imposters/i },
  { hash: "/cluster", heading: /cluster & fleet/i },
  { hash: "/requests", heading: /request log/i },
  { hash: "/routes", heading: /front-door routes/i },
] as const;

test.describe("the shipped console loads", () => {
  test("serves the shell under its own CSP", async ({ page }) => {
    const response = await page.goto("/console/");
    expect(response?.status()).toBe(200);
    const csp = response?.headers()["content-security-policy"] ?? "";
    // Asserted here, not just in the Rust test, because this is the one place the policy is
    // actually *enforced* by a browser rather than merely emitted.
    expect(csp).toContain("default-src 'self'");
    expect(csp).toContain("frame-ancestors 'none'");
    await expect(page.getByLabel(/api key/i)).toBeVisible();
  });

  test("refuses a bad key without claiming a server fault", async ({ page }) => {
    await page.goto("/console/");
    await page.getByLabel(/api key/i).fill("nonsense-not-a-key");
    await page.getByRole("button", { name: /^sign in$/i }).click();
    await expect(page.getByRole("alert")).toContainText(/not accepted/i);
  });

  for (const { hash, heading } of SCREENS) {
    test(`renders ${hash} as fleet-admin`, async ({ page }) => {
      await signIn(page, "fleet-admin");
      await goToScreen(page, hash);
      await expect(page.getByRole("heading", { level: 1 })).toHaveText(heading);
    });
  }

  test("renders the administration screens as fleet-admin", async ({ page }) => {
    await signIn(page, "fleet-admin");
    for (const tab of ["tenants", "principals", "bindings", "audit", "sink"]) {
      await goToScreen(page, `/admin/${tab}/default`);
      await expect(page.getByTestId("admin-screen")).toBeVisible();
    }
  });

  test("shows an imposter's stubs and opens the editor", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/imposters/${imposters[0]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    // The editor's own surface: the form, the JSON document, and the summary that says what the
    // stub will match. Monaco loading is part of what this asserts — it is bundled, not fetched.
    await expect(page.getByTestId("stub-form")).toBeVisible();
    await expect(page.getByTestId("stub-summary")).toBeVisible();
  });
});

test.describe("roles get the console their bindings allow", () => {
  test("a viewer is offered no write control", async ({ page }) => {
    await signIn(page, "viewer");
    await goToScreen(page, "/imposters");
    await expect(page.getByTestId("new-imposter")).toHaveCount(0);
    await expect(page.getByTestId("nav-administration")).toHaveCount(0);
    // Presentation only — but the read it *can* do must still work.
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(/imposters/i);
  });

  test("an operator may disable but not create", async ({ page }) => {
    await signIn(page, "operator");
    await goToScreen(page, "/imposters");
    await expect(page.getByTestId("new-imposter")).toHaveCount(0);
    await expect(page.getByRole("button", { name: /disable/i }).first()).toBeVisible();
  });

  test("a tenant-admin reaches principals without a fleet-scoped tenant list", async ({ page }) => {
    /*
     * The lockout that shipped: the nav links to the admin screen with no tenant, and the only
     * control that set one lived in a `ClusterAdmin` tenant list. A TenantAdmin could never reach
     * the surface its role exists for.
     */
    await signIn(page, "tenant-admin");
    await page.getByTestId("nav-administration").click();
    await page.getByRole("link", { name: /^principals$/i }).click();
    await expect(page.getByText(/choose a tenant/i)).toHaveCount(0);
    await expect(page.getByTestId("admin-screen")).toBeVisible();
  });

  test("the tenant in view is named even with nothing to switch to", async ({ page }) => {
    await signIn(page, "tenant-admin");
    await expect(page.getByTestId("tenant-current")).toContainText("default");
  });

  test("a fleet-admin sees the fleet screen a viewer is refused", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await expect(page.getByTestId("nav-cluster")).toBeVisible();
    await goToScreen(page, "/cluster");
    await expect(page.getByTestId("fleet-node")).toBeVisible();
  });
});

test.describe("session lifecycle", () => {
  test("signing out returns to the login form", async ({ page }) => {
    // The bug this pins was invisible to the cache-level unit test that preceded it.
    await signIn(page, "editor");
    await page.getByTestId("sign-out").click();
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    await expect(page.getByTestId("sign-out")).toHaveCount(0);
  });

  test("a signed-out session cannot be resumed by navigating back", async ({ page }) => {
    await signIn(page, "editor");
    await page.getByTestId("sign-out").click();
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    // Straight to `goto`, not `goToScreen`: that helper waits for the shell, which is precisely
    // what must NOT appear here. The cookie is gone, so whoami 401s and login is what renders.
    await page.goto("/console/#/imposters");
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    await expect(page.getByTestId("identity")).toHaveCount(0);
  });
});
