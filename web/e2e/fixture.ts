import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect, test as base } from "@playwright/test";
import type { ConsoleMessage, Page } from "@playwright/test";

/** Roles minted by `scripts/e2e-console.sh`. */
export type RoleKey =
  | "bootstrap"
  | "viewer"
  | "operator"
  | "editor"
  | "tenant-admin"
  | "fleet-admin"
  | "acme-editor";

type Fixture = {
  baseURL: string;
  keys: Record<RoleKey, string>;
  imposters: number[];
};

/**
 * The keys the fixture minted, read at run time.
 *
 * They cannot be committed: `createPrincipal` returns the raw key once and the fleet keeps only an
 * argon2id hash, so every fixture run mints new ones. The file is gitignored for the same reason.
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
 * unauthenticated visit probes `/admin/whoami` and renders the login screen from its `401`, and
 * RFC-002 §8.4 makes `404` the *correct* answer for a resource in another tenant. Failing on those
 * would fail every test for behaviour that is working.
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
 * Sign in as `role` and land on the imposters screen.
 *
 * Drives the real login form rather than injecting a cookie: the key-for-cookie exchange is the one
 * flow every session depends on, and a helper that bypassed it would leave it untested everywhere.
 */
export async function signIn(page: Page, role: RoleKey): Promise<void> {
  const { keys } = fixture();
  await page.goto("/console/");
  await page.getByLabel(/api key/i).fill(keys[role]);
  await page.getByRole("button", { name: /^sign in$/i }).click();
  // The shell is up once the identity is rendered; every screen assertion can rely on that.
  await expect(page.getByTestId("identity")).toBeVisible();
}

/** Navigate within the SPA by hash, then wait for the shell to settle. */
export async function goToScreen(page: Page, hash: string): Promise<void> {
  await page.goto(`/console/#${hash}`);
  await expect(page.getByTestId("identity")).toBeVisible();
}
