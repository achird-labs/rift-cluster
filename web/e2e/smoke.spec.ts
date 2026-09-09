import { expect, fixture, goToScreen, signIn, test } from "./fixture.ts";

/**
 * Layer 1: every screen loads in a real browser, with a clean console.
 *
 * The `test` fixture fails on any browser console error, so each of these is also asserting that no
 * CSP directive was violated, no chunk failed to load and no query rejected unhandled — the things
 * that are invisible to jsdom and only appear in the shipped artifact.
 */

const SCREENS = [
  { hash: "/imposters", heading: /imposters/i },
  { hash: "/cluster", heading: /cluster & fleet/i },
  { hash: "/requests", heading: /request log/i },
  { hash: "/routes", heading: /^router$/i },
  { hash: "/scenarios", heading: /scenarios & state/i },
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
    test(`renders ${hash}`, async ({ page }) => {
      await signIn(page);
      await goToScreen(page, hash);
      await expect(page.getByRole("heading", { level: 1 })).toHaveText(heading);
    });
  }

  test("shows an imposter's stubs and opens the editor", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[0]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    // The editor's own surface: the form, the JSON document, and the summary that says what the
    // stub will match. Monaco loading is part of what this asserts — it is bundled, not fetched.
    await expect(page.getByTestId("stub-form")).toBeVisible();
    await expect(page.getByTestId("stub-summary")).toBeVisible();
  });

  test("opens an EXISTING stub in the form, not raw-only (#257)", async ({ page }) => {
    /*
     * The gap that let #257 ship. The test above only ever clicks "Add stub", which starts from
     * `NEW_STUB_TEXT` — a local constant carrying a NUMERIC `statusCode`. So the whole e2e suite
     * never opened a stub that had been through the engine, and never saw that
     * `IsResponseOut.status_code` serializes as a STRING: every existing stub opened raw-only,
     * while a brand-new one opened in the form and looked fine.
     *
     * Asserting the raw-only banner is ABSENT is the load-bearing half. `stub-form` is rendered for
     * the id field either way, so its presence alone would have passed on the broken build.
     */
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[0]}`);

    // `/^edit /` with the trailing space: the visible text is "Edit" but the accessible name is
    // `Edit <stubId>` (see interactions.spec.ts).
    await page.getByRole("button", { name: /^edit /i }).first().click();
    await expect(page.getByTestId("stub-editor")).toBeVisible();

    await expect(page.getByTestId("stub-raw-banner")).toHaveCount(0);
    await expect(page.getByTestId("response-builder")).toBeVisible();
    await expect(page.getByTestId("stub-summary")).toBeVisible();
  });
});

test.describe("the whole console is offered, because there is one identity", () => {
  /*
   * #550 removed roles, so what these pin is the inverse of what they used to: every authoring
   * control is drawn for whoever signed in. They still earn their place — an accidentally hidden
   * control and a control that never rendered look identical from a bug report, and only a test
   * that names each one tells them apart.
   */
  test("offers the imposter-list authoring controls", async ({ page }) => {
    await signIn(page);
    await goToScreen(page, "/imposters");
    await expect(page.getByTestId("new-imposter")).toBeVisible();
    await expect(page.getByTestId("open-import")).toBeVisible();
    await expect(page.getByRole("button", { name: /disable/i }).first()).toBeVisible();
  });

  test("offers the authoring controls on an imposter's own screen", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[0]}`);

    await expect(page.getByRole("button", { name: /add stub/i })).toBeVisible();
    await expect(page.getByTestId("clone-imposter")).toBeVisible();
    /*
     * Asserted on the heading rather than the `detail-port` field: the fields moved onto the
     * Settings tab, and the heading carries the port on every tab. That makes it the better probe
     * for "the screen loaded" anyway — it cannot pass merely because one panel happened to render.
     */
    await expect(page.getByRole("heading", { level: 1 })).toContainText(String(imposters[0]));
  });

  test("offers no administration entry, because there is nothing left to administer", async ({
    page,
  }) => {
    // Tenancy and principals were that screen's whole subject, and #550 removed both. The nav must
    // not keep an entry whose route no longer parses — it would land on the imposters fallback and
    // read as a broken link.
    await signIn(page);
    await expect(page.getByTestId("nav-administration")).toHaveCount(0);
  });

  test("offers every scenario and space control", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page);
    await expect(page.getByTestId("nav-scenarios")).toBeVisible();
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    await expect(page.getByTestId("reset-scenarios")).toBeVisible();
    await expect(page.getByTestId("flow-state-clear-all")).toBeVisible();

    // The space controls are asserted ON THE TAB THEY LIVE ON: neither renders on the scenarios
    // tab for anybody, so asserting there would pass for the wrong reason.
    await goToScreen(page, `/scenarios/${imposters[0]}?tab=spaces`);
    await expect(page.getByTestId("space-teardown")).toBeVisible();
    await expect(page.getByTestId("space-add-stub")).toBeVisible();
  });

  test("reaches the fleet screen from the nav", async ({ page }) => {
    await signIn(page);
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
    await signIn(page);
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
    await signIn(page);
    await page.getByTestId("sign-out").click();
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    await expect(page.getByTestId("sign-out")).toHaveCount(0);
  });

  test("a signed-out session cannot be resumed by navigating back", async ({ page }) => {
    await signIn(page);
    await page.getByTestId("sign-out").click();
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    // Straight to `goto`, not `goToScreen`: that helper waits for the shell, which is precisely
    // what must NOT appear here. The cookie is gone, so the session probe 401s and login renders.
    await page.goto("/console/#/imposters");
    await expect(page.getByLabel(/api key/i)).toBeVisible();
    await expect(page.getByTestId("identity")).toHaveCount(0);
  });
});
