import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fixture, goToScreen, signIn, test } from "./fixture.ts";

/**
 * A screenshot survey — every screen, full page.
 *
 * One pass, not one per role: since #550 there is one credential and one identity, so there is no
 * second view of any screen to capture.
 *
 * Not a gate. It asserts almost nothing and is excluded from `pnpm run e2e` (see
 * `playwright.config.ts`'s `testIgnore`); its output is a directory of images for a person to look
 * at. Visual *regression* answers "did this change?"; nothing in the suite answers "is this any
 * good?", and that question still needs eyes.
 *
 *     pnpm run e2e:survey     # writes web/survey/
 */

const OUT = fileURLToPath(new URL("../survey/", import.meta.url));
mkdirSync(OUT, { recursive: true });

const SCREENS: { hash: string; name: string }[] = [
  { hash: "/imposters", name: "imposters" },
  { hash: "/cluster", name: "cluster" },
  { hash: "/requests", name: "requests-picker" },
  { hash: "/routes", name: "routes" },
  { hash: "/scenarios", name: "scenarios-picker" },
];

test("survey: every screen", async ({ page }) => {
  await signIn(page);
  for (const { hash, name } of SCREENS) {
    await goToScreen(page, hash);
    // Let polling settle so a half-loaded table is not what gets captured.
    await page.waitForTimeout(400);
    await page.screenshot({ path: `${OUT}${name}.png`, fullPage: true });
  }
});

test("survey: detail and editor surfaces", async ({ page }) => {
  const { imposters } = fixture();
  await signIn(page);

  await goToScreen(page, `/imposters/${imposters[0]}`);
  await page.waitForTimeout(400);
  await page.screenshot({ path: `${OUT}imposter-detail.png`, fullPage: true });

  await page.getByRole("button", { name: /add stub/i }).click();
  await page.waitForTimeout(600);
  await page.screenshot({ path: `${OUT}stub-new.png`, fullPage: true });

  await goToScreen(page, `/requests/${imposters[0]}`);
  await page.waitForTimeout(400);
  await page.screenshot({ path: `${OUT}request-log.png`, fullPage: true });

  // An expanded row, which is where the match diagnostics live.
  await page.getByTestId("request-open").first().click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: `${OUT}request-expanded.png`, fullPage: true });

  await goToScreen(page, "/imposters");
  await page.getByTestId("new-imposter").click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: `${OUT}new-imposter.png`, fullPage: true });
});

test("survey: the login screen", async ({ page }) => {
  await page.goto("/console/");
  await page.waitForTimeout(300);
  await page.screenshot({ path: `${OUT}login.png`, fullPage: true });
});
