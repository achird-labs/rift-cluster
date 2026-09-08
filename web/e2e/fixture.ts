import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect, test as base } from "@playwright/test";
import type { ConsoleMessage, Page } from "@playwright/test";

type Fixture = {
  baseURL: string;
  /** The fleet's one credential (#550) — the value `scripts/e2e-console.sh` passed to `--api-key`. */
  apiKey: string;
  imposters: number[];
};

/**
 * What `scripts/e2e-console.sh` wrote about the fleet it started, read at run time.
 *
 * Not committed: the port and the key are the fixture's own, and the file is gitignored so a stale
 * one cannot silently point a run at a fleet that is no longer there.
 */
export function fixture(): Fixture {
  const path = fileURLToPath(new URL("./.fixture.json", import.meta.url));
  try {
    return JSON.parse(readFileSync(path, "utf8")) as Fixture;
  } catch (cause) {
    throw new Error(
      `no e2e fixture at ${path} — run \`scripts/e2e-console.sh up\` first (playwright.config.ts does this for you)`,
      { cause },
    );
  }
}

/**
 * A page that **fails the test on any browser console error**.
 *
 * This is most of the value of running a real browser at all. A CSP violation, a failed wasm
 * instantiation, an unhandled rejection in a query — none of them fail a jsdom test and all of them
 * are broken console. Warnings are ignored: React and Vite both emit them routinely and a suite
 * that fails on noise gets muted.
 */
/**
 * A 4xx the console asked for on purpose.
 *
 * Chrome logs every failed response as a console error, and this console provokes 4xx by design: an
 * unauthenticated visit probes `/_fleet/health` and renders the login screen from its `401`.
 * Failing on that would fail every test for behaviour that is working.
 *
 * 5xx is deliberately not excluded. Nothing in this console expects one, so a `500` reaching the
 * browser stays a failure.
 */
const EXPECTED_4XX = /Failed to load resource: the server responded with a status of 4\d\d/;

export const test = base.extend<{ page: Page }>({
  page: async ({ page }, use) => {
    const errors: string[] = [];
    page.on("console", (message: ConsoleMessage) => {
      if (message.type() !== "error") return;
      const text = message.text();
      // Everything else is kept — CSP violations, failed chunk loads and React errors all arrive
      // here, and each is broken console rather than a status the app handles.
      if (!EXPECTED_4XX.test(text)) errors.push(text);
    });
    // An uncaught exception is never expected and is never filtered.
    page.on("pageerror", (error) => errors.push(`uncaught: ${error.message}`));
    await use(page);
    expect(errors, `browser console errors:\n${errors.join("\n")}`).toEqual([]);
  },
});

export { expect };

/**
 * Sign in and land on the imposters screen.
 *
 * No role: since #550 there is one credential and one identity, so every session is the same
 * session. Drives the real login form rather than injecting a cookie — the key-for-cookie exchange
 * is the one flow every session depends on, and a helper that bypassed it would leave it untested
 * everywhere.
 */
export async function signIn(page: Page): Promise<void> {
  const { apiKey } = fixture();
  await page.goto("/console/");
  await page.getByLabel(/api key/i).fill(apiKey);
  await page.getByRole("button", { name: /^sign in$/i }).click();
  // The shell is up once the nav is rendered; every screen assertion can rely on that.
  await expect(page.getByTestId("nav-imposters")).toBeVisible();
}

/** Navigate within the SPA by hash, then wait for the shell to settle. */
export async function goToScreen(page: Page, hash: string): Promise<void> {
  await page.goto(`/console/#${hash}`);
  await expect(page.getByTestId("nav-imposters")).toBeVisible();
}
