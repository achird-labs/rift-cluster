import AxeBuilder from "@axe-core/playwright";
import { expect, fixture, goToScreen, signIn, test } from "./fixture.ts";
import type { Page } from "@playwright/test";

/**
 * Layer 3: axe over every screen.
 *
 * Two things this is worth having for specifically.
 *
 * **Contrast.** The token set claims every status colour clears 4.5:1 against its own surface in
 * both themes. That was measured by hand once; axe re-measures it against what the browser actually
 * composites, in both themes, on every run. A hand-validated claim with no test is a claim with a
 * shelf life.
 *
 * **Labels and roles.** Half this console is forms. A field that loses its `<label>` association is
 * invisible to a screen reader and to `getByLabel` — which is how most of the suite finds things,
 * so the failure would show up as a confusing test break rather than as the accessibility
 * regression it is.
 *
 * Only `serious` and `critical` are failed on. `moderate` and `minor` include advisory rules
 * (landmark structure, heading order) that would turn this into a style gate reviewers learn to
 * skip — the same reasoning as the console's one-rule eslint config.
 */

async function violations(page: Page) {
  /*
   * Settle before scanning.
   *
   * The console polls every 5s, so a screen re-renders underneath axe while it walks the DOM — and
   * axe then computes a contrast pair against a background that is mid-swap. It showed up as
   * `color-contrast` failing roughly one run in three on the new-imposter form, with the same page
   * passing on the next run and a scoped probe finding nothing at all.
   *
   * This is a mitigation, not a diagnosis: it narrows the window rather than closing it. If this
   * rule starts flaking again, the honest fix is to pause polling for the scan rather than to widen
   * the wait.
   */
  await page.waitForTimeout(600);
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  return results.violations.filter((v) => v.impact === "serious" || v.impact === "critical");
}

function describeViolations(found: Awaited<ReturnType<typeof violations>>): string {
  return found
    .map((v) => `${v.id} (${v.impact}): ${v.help}\n  ${v.nodes.map((n) => n.target.join(" ")).join("\n  ")}`)
    .join("\n\n");
}

test.describe("accessibility", () => {
  test("the login form", async ({ page }) => {
    await page.goto("/console/");
    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });

  for (const hash of ["/imposters", "/cluster", "/requests", "/routes", "/scenarios", "/sources"]) {
    test(`the ${hash} screen`, async ({ page }) => {
      await signIn(page, "fleet-admin");
      await goToScreen(page, hash);
      const found = await violations(page);
      expect(found, describeViolations(found)).toEqual([]);
    });
  }

  for (const tab of ["tenants", "principals", "bindings", "audit", "sink"]) {
    test(`the ${tab} admin tab`, async ({ page }) => {
      await signIn(page, "fleet-admin");
      await goToScreen(page, `/admin/${tab}/default`);
      const found = await violations(page);
      expect(found, describeViolations(found)).toEqual([]);
    });
  }

  test("the new-imposter form", async ({ page }) => {
    // Every field must reach a label. The checkbox especially: its sentence lives in a sibling
    // span, which is exactly the arrangement that loses its association when markup is refactored.
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("new-imposter").click();
    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });

  test("the imposter list with a selection, so the bulk bar is in the scan", async ({ page }) => {
    /*
     * The `/imposters` scan above sees the list at rest: the filter selects are covered, but the
     * checkbox column and the bulk bar only exist once something is ticked, and the bar is where
     * the accessible names are easiest to lose. Each checkbox's name comes from an `aria-label`
     * rather than a visible `<label>` — a column of unlabelled checkboxes is the exact shape axe
     * exists to catch, and it is also unusable by anyone not looking at the row it sits in.
     *
     * Keyboard-operable is asserted here rather than by clicking: reaching the checkbox with Tab
     * and toggling it with Space is the interaction, and a `<div>` dressed as a checkbox would pass
     * a click test and fail this one.
     */
    const { imposters } = fixture();
    await signIn(page, "fleet-admin");
    await goToScreen(page, "/imposters");

    const first = page.getByTestId(`imposter-select-${imposters[0]}`);
    await first.focus();
    await expect(first).toBeFocused();
    await page.keyboard.press("Space");
    await expect(first).toBeChecked();
    await expect(page.getByTestId("imposter-bulk-bar")).toBeVisible();

    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });

  test("a sortable column header is a real button, reachable and operable by keyboard", async ({
    page,
  }) => {
    // A `<th>` with an onClick would sort on click and be invisible to keyboard and screen-reader
    // users entirely. `aria-sort` is the other half: without it the current order is conveyed by
    // an arrow glyph and nothing else.
    await signIn(page, "fleet-admin");
    await goToScreen(page, "/imposters");

    const header = page.getByTestId("imposter-sort-name");
    await header.focus();
    await expect(header).toBeFocused();
    await page.keyboard.press("Enter");

    await expect(page.locator('th[aria-sort="ascending"]')).toHaveCount(1);
    await expect(header.locator("xpath=ancestor::th")).toHaveAttribute("aria-sort", "ascending");
  });

  test("the scenarios screen, whose panels are three forms", async ({ page }) => {
    // The picker above covers the empty shape; this covers the populated one. Every panel here is a
    // form — a scenario state input, a stub textarea, a flow-state key and value — and a field that
    // loses its label association is invisible to a screen reader and to `getByLabel`.
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/scenarios/${imposters[0]}`);
    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });

  test("the stub editor", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page, "editor");
    await goToScreen(page, `/imposters/${imposters[0]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    await expect(page.getByTestId("stub-form")).toBeVisible();

    /*
     * Populate the predicate builder before scanning. A new stub carries no predicates, so scanning
     * straight after "add stub" sees only the builder's empty-state paragraph and its buttons — none
     * of the selects, inputs or the options panel that are the actual a11y surface. The criterion
     * asks for the *builder* to be covered, and an empty builder is the one state with no inputs in
     * it to get wrong.
     */
    await page.getByRole("button", { name: /add predicate/i }).click();
    await page.getByRole("button", { name: /more options for predicate 1/i }).click();
    await expect(page.getByLabel(/case sensitivity/i)).toBeVisible();

    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });

  test("the confirm dialog traps its own labelling", async ({ page }) => {
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");
    await page.getByTestId("delete-imposter-4645").click();
    const found = await violations(page);
    expect(found, describeViolations(found)).toEqual([]);
  });
});
