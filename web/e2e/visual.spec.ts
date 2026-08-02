import { expect, fixture, goToScreen, signIn, test } from "./fixture.ts";

/**
 * Layer 2: visual regression, scoped to **components** rather than pages.
 *
 * Full-page snapshots are the reason visual testing has a reputation for flakiness: a row count, a
 * timestamp, a poll landing mid-capture and the whole page diffs. Every baseline below is one
 * component, captured against fixture data that does not move.
 *
 * This is the layer that catches the class of bug the 375 jsdom tests cannot see. The one that
 * motivated it: `background-color` on an `input` suppresses a checkbox's native checked indicator
 * in Blink and makes `accent-color` a no-op, so every checkbox in the console rendered as an empty
 * square. jsdom parses CSS but never computes the cascade or paints, so nothing there could fail.
 *
 * Runs in both `chromium` (light) and `chromium-dark`, because the token set swaps entirely on
 * `prefers-color-scheme` and a control that vanishes in one theme is a real defect.
 */

/** Hide anything whose content legitimately changes between runs. */
const VOLATILE = [
  // Applied index and last-error timestamps advance on their own.
  ".tile:has(.eyebrow:text-is('Applied index'))",
];

test.describe("component baselines", () => {
  test("the nav rail", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await expect(page.locator("nav.rail")).toHaveScreenshot("rail-fleet-admin.png");
  });

  test("the nav rail as a viewer, where most entries are not drawn", async ({ page }) => {
    // The rail's shape *is* the RBAC surface. A baseline here fails if a control starts being
    // offered to a role that cannot use it — the failure mode `rbac.ts` exists to prevent, caught
    // visually rather than by assertion.
    await signIn(page, "viewer");
    await expect(page.locator("nav.rail")).toHaveScreenshot("rail-viewer.png");
  });

  test("the topbar with identity and sign-out", async ({ page }) => {
    await signIn(page, "editor");
    await expect(page.locator("header.topbar")).toHaveScreenshot("topbar.png");
  });

  test("the login card", async ({ page }) => {
    await page.goto("/console/");
    await expect(page.locator("main.login")).toHaveScreenshot("login.png");
  });

  test("the new-imposter form, checkbox included", async ({ page }) => {
    /*
     * The specific baseline that would have caught the invisible checkbox. It is captured with the
     * box **checked**, because the bug was precisely that a checked box rendered identically to an
     * unchecked one.
     */
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("new-imposter").click();
    await expect(page.getByTestId("new-imposter-form")).toHaveScreenshot("new-imposter-form.png");
  });

  test("a checkbox renders differently checked and unchecked", async ({ page }) => {
    // Belt and braces alongside the baseline above, and it needs no committed image: if the two
    // states are pixel-identical the control is not rendering its state at all.
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("new-imposter").click();
    const box = page.getByTestId("new-imposter-form").getByRole("checkbox");
    const checked = await box.screenshot();
    await box.uncheck();
    const unchecked = await box.screenshot();
    expect(Buffer.compare(checked, unchecked)).not.toBe(0);
  });

  test("the imposter table", async ({ page }) => {
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await expect(page.locator(".card").first()).toHaveScreenshot("imposter-table.png");
  });

  test("the fleet stat tiles", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await goToScreen(page, "/cluster");
    await expect(page.locator(".tiles").first()).toHaveScreenshot("fleet-tiles.png", {
      mask: VOLATILE.map((selector) => page.locator(selector)),
    });
  });

  test("the request log scope strip", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/requests/${imposters[0]}`);
    await expect(page.getByTestId("request-scope-label")).toHaveScreenshot("request-scope.png");
  });

  test("the scenarios scope strip", async ({ page }) => {
    /*
     * The same reasoning as the request log's strip above: this screen's every number and control
     * acts on **one space**, and the strip is what keeps that in front of the reader. A baseline
     * fails if it stops being the most prominent thing under the title — which is the failure mode
     * that turns a per-space reset into one an operator thinks is global.
     */
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    await expect(page.getByTestId("scenarios-scope")).toHaveScreenshot("scenarios-scope.png");
  });

  test("the stub editor with its summary", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/imposters/${imposters[0]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    // The summary's warning state: a new stub carries no predicates and matches everything.
    await expect(page.getByTestId("stub-summary")).toHaveScreenshot("stub-summary-catchall.png");
    await expect(page.getByTestId("stub-presets")).toHaveScreenshot("stub-presets.png");
  });

  test("the confirm dialog for a destructive act", async ({ page }) => {
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("delete-imposter-4645").click();
    await expect(page.getByTestId("confirm-delete-imposter")).toHaveScreenshot("confirm.png");
  });
});
