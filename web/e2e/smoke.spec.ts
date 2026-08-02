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
  { hash: "/scenarios", heading: /scenarios & state/i },
  { hash: "/sources", heading: /sources/i },
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

  test("a viewer reads scenarios and is offered no control that disturbs them", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "viewer");
    // The entry must be *offered* — `scenario.read` is a Viewer grant, so hiding the screen would
    // withhold a surface the role is entitled to.
    await expect(page.getByTestId("nav-scenarios")).toBeVisible();
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    await expect(page.getByTestId("reset-scenarios")).toHaveCount(0);
    await expect(page.getByTestId("space-teardown")).toHaveCount(0);
    await expect(page.getByTestId("flow-state-clear-all")).toHaveCount(0);
  });

  test("an operator may reset and tear down but not redefine", async ({ page }) => {
    /*
     * The disturb/redefine split, in the shipped artifact. `ScenarioReset` and `SpaceTeardown` are
     * Operator; `ScenarioWrite` and `SpaceStubWrite` are Editor. The flow-state panel is the
     * counter-intuitive one — an operator may clear an entry but not set one, because the server
     * classifies the `PUT` as `SpaceStubWrite`.
     */
    const { imposters } = fixture();
    await signIn(page, "operator");
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    await expect(page.getByTestId("reset-scenarios")).toBeVisible();
    await expect(page.getByTestId("flow-state-clear-all")).toBeVisible();
    await expect(page.getByTestId("space-add-stub")).toHaveCount(0);
  });

  test("an editor may scope a stub into a space", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    await expect(page.getByTestId("space-add-stub")).toBeVisible();
  });

  test("a fleet-admin sees the fleet screen a viewer is refused", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await expect(page.getByTestId("nav-cluster")).toBeVisible();
    await goToScreen(page, "/cluster");
    await expect(page.getByTestId("fleet-node")).toBeVisible();
  });
});

test.describe("the destructive-confirm dialog is a modal (#236)", () => {
  /*
   * The layer that can actually judge this. jsdom parses CSS but never computes the cascade, so
   * every assertion below is invisible to the 470-test unit suite — which is exactly how three
   * missing rules turned the modal guarding every destructive act into an ordinary block in the
   * page flow, with nothing failing.
   *
   * These assert the *properties*, not an image: a baseline records what it looked like on the day
   * it was taken, and the confirm baseline had already recorded the broken rendering as correct.
   */
  async function openTheDialog(page: import("@playwright/test").Page) {
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("delete-imposter-4645").click();
    await expect(page.getByTestId("confirm-delete-imposter")).toBeVisible();
  }

  test("covers the whole viewport, fixed, above the page", async ({ page }) => {
    await openTheDialog(page);
    const scrim = page.locator(".scrim");

    const box = await scrim.boundingBox();
    /*
     * Compared against the *initial containing block*, not `page.viewportSize()`. A
     * `position: fixed; inset: 0` element is sized to the ICB, which excludes a classic scrollbar
     * gutter — and this page scrolls (`.app { min-height: 100vh }`). On a platform with overlay
     * scrollbars the two agree; on one with classic scrollbars `viewportSize()` is ~15px wider and
     * a strict equality fails for a reason nobody changed.
     */
    const icb = await page.evaluate(() => ({
      width: document.documentElement.clientWidth,
      height: document.documentElement.clientHeight,
    }));
    // The overlay must span the viewport — not merely exist. A `.scrim` laid out in the flow has
    // the width of its container and the height of its content, which is the shape of the bug.
    expect(box?.width).toBe(icb.width);
    expect(box?.height).toBe(icb.height);

    const { position, zIndex } = await scrim.evaluate((el) => {
      const s = getComputedStyle(el);
      return { position: s.position, zIndex: s.zIndex };
    });
    expect(position).toBe("fixed");
    expect(Number(zIndex)).toBeGreaterThan(0);
  });

  test("puts the page behind it out of reach", async ({ page }) => {
    /*
     * `aria-modal="true"` tells assistive technology the rest of the page is inert. Before this
     * fix nothing backed that claim — the page behind stayed clickable, so the attribute was a
     * statement the presentation contradicted.
     *
     * Probed by hit-testing rather than by attempting a click: `elementFromPoint` answers "what
     * would receive this click" without depending on Playwright's actionability timeouts.
     */
    await openTheDialog(page);
    const topLeft = await page.evaluate(() => {
      const el = document.elementFromPoint(60, 300);
      return el === null ? null : { inScrim: el.closest(".scrim") !== null, tag: el.tagName };
    });
    expect(topLeft).not.toBeNull();
    expect(topLeft?.inScrim).toBe(true);
  });

  test("centres the dialog and gives it its own surface", async ({ page }) => {
    await openTheDialog(page);
    const dialog = page.getByTestId("confirm-delete-imposter");

    const box = await dialog.boundingBox();
    // Same reasoning as the scrim test: measure against the initial containing block, so a classic
    // scrollbar gutter does not shift the centre out from under the assertion.
    const icbWidth = await page.evaluate(() => document.documentElement.clientWidth);
    const centreOffset = Math.abs((box?.x ?? 0) + (box?.width ?? 0) / 2 - icbWidth / 2);
    // Centred to within a pixel of rounding, rather than pinned to the left edge as flow layout
    // would leave it.
    expect(centreOffset).toBeLessThanOrEqual(1);
    // Bounded, so a long imposter name cannot stretch the dialog across a 1440px viewport.
    expect(box?.width ?? 0).toBeLessThan(icbWidth * 0.75);

    const surface = await dialog.evaluate((el) => {
      const s = getComputedStyle(el);
      return { background: s.backgroundColor, radius: s.borderTopLeftRadius, border: s.borderTopWidth };
    });
    // A transparent dialog would show the page through the text of a confirmation someone is about
    // to act on.
    expect(surface.background).not.toBe("rgba(0, 0, 0, 0)");
    expect(surface.radius).not.toBe("0px");
    expect(surface.border).not.toBe("0px");
  });

  test("lays Cancel and the destructive button out as one right-aligned row", async ({ page }) => {
    await openTheDialog(page);
    const dialog = page.getByTestId("confirm-delete-imposter");
    const cancel = dialog.getByRole("button", { name: /cancel/i });
    const destructive = page.getByTestId("confirm-destructive");

    const [c, d, box] = await Promise.all([
      cancel.boundingBox(),
      destructive.boundingBox(),
      dialog.boundingBox(),
    ]);
    // Same row: their vertical centres agree. Stacked buttons are what an unstyled `.acts` gives.
    expect(Math.abs((c?.y ?? 0) - (d?.y ?? 0))).toBeLessThanOrEqual(1);
    // Right-aligned: the destructive button is the rightmost thing, near the dialog's right edge.
    expect((d?.x ?? 0)).toBeGreaterThan((c?.x ?? 0));
    const rightGap = (box?.x ?? 0) + (box?.width ?? 0) - ((d?.x ?? 0) + (d?.width ?? 0));
    expect(rightGap).toBeLessThan(40);
  });

  test("still refuses to dismiss on a stray click beside it", async ({ page }) => {
    // Deliberate, and stated in `primitives.tsx`: "a stray click beside a destructive dialog should
    // do nothing at all". Now that the scrim actually covers the page it *could* have become a
    // dismiss target, so this pins that it did not.
    await openTheDialog(page);
    await page.mouse.click(60, 300);
    await expect(page.getByTestId("confirm-delete-imposter")).toBeVisible();
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
