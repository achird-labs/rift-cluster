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
