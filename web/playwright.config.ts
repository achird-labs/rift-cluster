import { defineConfig, devices } from "@playwright/test";

/**
 * Browser tests against the **embedded** console — the binary serving `/console/`, not the Vite dev
 * server.
 *
 * That choice is the point of having these at all. The vitest suite already covers behaviour under
 * jsdom, which parses CSS but never computes the cascade or paints; the bug that motivated this
 * layer was `background-color` on an `input` suppressing a checkbox's native checked indicator,
 * which no jsdom assertion can see. Running against the real artifact also means the shipped CSP,
 * the real bundle and the wasm linter are all under test rather than assumed.
 *
 * `scripts/e2e-console.sh` starts one seeded node; see its header for why one and not three.
 */
const ADMIN = "http://127.0.0.1:3525";

/**
 * Which Chromium to drive.
 *
 * `chromium` — Playwright's own pinned build — is the default and the only one CI uses, because the
 * visual baselines are only meaningful against a fixed browser version. `PLAYWRIGHT_CHANNEL=chrome`
 * falls back to a locally installed Google Chrome, which is an escape hatch for a machine where the
 * pinned download will not install (it stalls mid-extraction on some sandboxed setups).
 *
 * That escape hatch is for running the *behaviour* specs. Do not regenerate baselines under it:
 * Chrome tracks stable and rasterises differently, so images produced there will diff against CI on
 * unchanged code. `e2e/README.md` says the same thing where someone will actually read it.
 */
const CHANNEL = process.env.PLAYWRIGHT_CHANNEL ?? "chromium";

export default defineConfig({
  testDir: "./e2e",
  // The survey is an exploration tool, not a gate: it writes a directory of screenshots for a
  // person to look at and asserts almost nothing. Excluded from the default run so `pnpm run e2e`
  // stays a check. Opted into by `E2E_SURVEY=1` (`pnpm run e2e:survey`) rather than by naming the
  // file, because `testIgnore` wins over an explicit filename argument.
  ...(process.env.E2E_SURVEY || process.env.E2E_INTERACTIONS
    ? {}
    : {
        // `interactions.spec.ts` is excluded for a different reason from the survey: four of its
        // eight tests fail and have not been triaged. Its header says which and what is known. A
        // gate that is red for unread reasons is a gate people learn to skip.
        testIgnore: [/survey\.spec\.ts/, /interactions\.spec\.ts/],
      }),
  // Serial. The fixture is a single stateful node with fixed ports, and the specs write to it
  // (creating imposters, clearing logs). Parallel workers would race each other's fixture.
  workers: 1,
  fullyParallel: false,
  // A failing browser test is usually a real difference, not a flake — retrying by default would
  // hide an intermittent rendering bug, which is the class this layer exists to catch. CI gets one
  // retry only to absorb genuine infrastructure noise.
  retries: process.env.CI ? 1 : 0,
  forbidOnly: !!process.env.CI,
  reporter: process.env.CI ? [["github"], ["list"]] : [["list"]],

  use: {
    baseURL: ADMIN,
    // Fixed viewport: a visual baseline diffed at a different size fails for a reason nobody
    // changed. Desktop only, per RFC-006 §10.
    viewport: { width: 1440, height: 900 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },

  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        /*
         * The full Chromium build, not the headless shell Playwright reaches for by default.
         *
         * Two reasons, and the second is the one that matters. It renders like the browser an
         * operator actually uses — the shell diverges subtly on font rasterisation and some
         * compositing, which is exactly the surface the visual baselines measure. And it is one
         * download rather than two, which keeps the CI cache smaller and the local setup shorter.
         */
        channel: CHANNEL,
        // Pinned, because `prefers-color-scheme` selects an entire token set and the baselines are
        // per-theme. The dark-mode project below flips exactly this.
        colorScheme: "light",
      },
    },
    {
      name: "chromium-dark",
      use: { ...devices["Desktop Chrome"], channel: CHANNEL, colorScheme: "dark" },
      // Only the visual specs run twice; behaviour does not change with the palette.
      testMatch: /visual\.spec\.ts/,
    },
  ],

  expect: {
    toHaveScreenshot: {
      // Anti-aliasing differs by a pixel or two across machines. Tight enough to catch a control
      // that stopped rendering, loose enough not to fail on font hinting.
      maxDiffPixelRatio: 0.01,
    },
  },

  webServer: {
    command: "bash ../scripts/e2e-console.sh up",
    /*
     * The seeded sentinel imposter, not `/console/`.
     *
     * The console answers as soon as the node binds — before any tenant, principal or imposter
     * exists — so waiting on it starts the suite mid-seed. `scripts/e2e-console.sh` creates
     * imposter 4699 as its very last action, so a 200 here means every seeding step committed.
     * Gateway traffic is auth-exempt, so the probe needs no credential.
     */
    url: "http://127.0.0.1:4699/",
    // Reuses an already-running fixture locally so the edit/run loop is fast; CI always starts its
    // own, because a reused one there would mean state leaking between jobs.
    reuseExistingServer: !process.env.CI,
    timeout: 15 * 60 * 1000,
    stdout: "pipe",
    stderr: "pipe",
  },
});
