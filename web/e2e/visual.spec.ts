import type { Page } from "@playwright/test";

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
 * One theme, so one run. This used to run twice — `chromium` and `chromium-dark` — because the
 * token set swapped entirely on `prefers-color-scheme` and a control that vanished in one theme was
 * a real defect. The console ships a single palette now, so the second pass photographed the same
 * pixels and doubled the baselines to regenerate.
 */

/**
 * The nav, by role and accessible name rather than by class.
 *
 * It was `nav.rail` until the bar went horizontal, and a locator that a pure restyle can break is a
 * locator asserting the wrong thing: what this baseline is about is the set of entries a role is
 * offered, which the accessible name identifies and the class name only happened to.
 */
const navBar = (page: Page) => page.getByRole("navigation", { name: "Console sections" });

/** Hide anything whose content legitimately changes between runs. */
const VOLATILE = [
  // Applied index and last-error timestamps advance on their own.
  ".tile:has(.eyebrow:text-is('Applied index'))",
];

test.describe("component baselines", () => {
  test("the nav bar", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await expect(navBar(page)).toHaveScreenshot("nav-fleet-admin.png");
  });

  test("the nav bar as a viewer, where most entries are not drawn", async ({ page }) => {
    // The bar's shape *is* the RBAC surface. A baseline here fails if a control starts being
    // offered to a role that cannot use it — the failure mode `rbac.ts` exists to prevent, caught
    // visually rather than by assertion. Under the top-bar layout it also catches a group whose
    // every entry is filtered away still drawing its separator.
    await signIn(page, "viewer");
    await expect(navBar(page)).toHaveScreenshot("nav-viewer.png");
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
    await expect(page.getByTestId("new-imposter-wizard")).toHaveScreenshot("new-imposter-form.png");
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
    /*
     * `.card:has(table.dense)`, not `.card").first()`.
     *
     * The first card on this screen stopped being the table when #251 added the tenant export
     * control above it, and the baseline has been a picture of two export buttons ever since — so
     * the table this test is named for has had no visual coverage at all, silently. A locator that
     * says which card it wants cannot drift that way again.
     *
     * Filtered to one fixture imposter before capturing, because this suite shares a live fleet
     * with the specs running beside it: `recording.spec.ts` and `oracle.spec.ts` both create
     * throwaway imposters, so an unfiltered table is however many rows happened to exist at the
     * moment of capture. That is exactly the "a row count and the whole page diffs" flakiness this
     * file's header says it exists to avoid — it only became visible once the locator started
     * capturing the real table.
     */
    const { imposters } = fixture();
    await page.getByTestId("imposter-filter-text").fill(String(imposters[0]));
    // "1 of N" once a filter is active — N moves with what the other specs have created, which is
    // precisely why the capture is filtered, so only the "1 of" prefix is asserted.
    await expect(page.getByTestId("imposter-filter-count")).toContainText(/^1 of /);
    await expect(page.locator(".card:has(table.dense)")).toHaveScreenshot("imposter-table.png");
  });

  test("the fleet stat tiles", async ({ page }) => {
    await signIn(page, "fleet-admin");
    await goToScreen(page, "/cluster");
    await expect(page.locator(".tiles").first()).toHaveScreenshot("fleet-tiles.png", {
      mask: VOLATILE.map((selector) => page.locator(selector)),
    });
  });

  /*
   * The request log's scope strip had a baseline here until #147 H. It is deliberately gone rather
   * than re-pointed: the strip is now a **partial-merge-only** element — it renders when the
   * server stamps `Rift-Cluster-Partial`, and not otherwise — and this fixture is a live
   * single-node server whose merges always reach every node. There is no state it can drive that
   * shows the strip, so any baseline taken here would be a screenshot of an element that no longer
   * exists. The strip's presence, absence and copy are asserted instead in
   * `src/__tests__/requestLog.test.tsx`, which can drive the header directly.
   */

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
