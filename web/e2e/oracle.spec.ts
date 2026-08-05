import { expect, fixture, signIn, test } from "./fixture.ts";
import type { Page } from "@playwright/test";

/**
 * The imposter list, checked against the fleet's own answer rather than against a fixture.
 *
 * Every other spec in this directory asserts that a screen shows what the test *told* it to show.
 * That is the shape three shipped defects hid in (#321, #323, #324): the assertions were complete
 * with respect to what they modelled, and the model was wrong. Two concrete cases —
 *
 *   - every vitest list fixture handed the screen a `stubs` ARRAY, which no real `GET /imposters`
 *     response carries (it sends `stubCount`), so a Stubs column reading only `stubs` passed its
 *     tests and rendered `—` for every row against a live cluster;
 *   - no fixture anywhere built an imposter without a `name`, so the fact that the name cell was
 *     the only route to the detail screen — and rendered unlinked when absent — was unobservable.
 *
 * So this file asserts a RELATION instead of a rendering: for each imposter the admin API returns,
 * the row exists, its count agrees, and its detail screen is reachable. Nothing here hard-codes
 * what the fleet holds, which is what makes it keep working as the corpus below grows.
 *
 * ── The corpus ──────────────────────────────────────────────────────────────
 *
 * Created here rather than in `scripts/e2e-console.sh`, deliberately. That script's imposters are
 * snapshotted by `visual.spec.ts`'s imposter-table baseline, so seeding awkward rows there would
 * fail an unrelated baseline on every run and force a re-record. These are created in `beforeAll`,
 * torn down in `afterAll`, and Playwright runs spec FILES serially here (`workers: 1`,
 * `fullyParallel: false`) — `oracle` sorts before `visual`, so the table is back to its seeded
 * shape by the time the baseline is taken.
 *
 * Ports are 49xx to stay clear of the seeded 46xx range.
 */

type Corpus = { port: number; body: Record<string, unknown>; why: string };

const STUB = (path: string) => ({
  predicates: [{ equals: { path } }],
  responses: [{ is: { statusCode: 200, body: "ok" } }],
});

const CORPUS: Corpus[] = [
  {
    port: 4901,
    why: "no name at all — `name` is optional on POST /imposters, and the name cell is the list's only route to the detail screen (#321)",
    body: { port: 4901, protocol: "http", stubs: [STUB("/a"), STUB("/b")] },
  },
  {
    port: 4902,
    why: "zero stubs — absent-vs-zero, which must render 0 and not the `—` that means 'the response did not say' (#323)",
    body: { port: 4902, protocol: "http", name: "corpus-empty", stubs: [] },
  },
  {
    port: 4903,
    why: "a name past the 34-character truncation cut, so the cell truncates while the link still resolves",
    body: {
      port: 4903,
      protocol: "http",
      name: "corpus-checkout-service-integration-sandbox-eu-west-1",
      stubs: [STUB("/c")],
    },
  },
  {
    port: 4904,
    why: "non-ASCII name — the console is not English-only, and truncation is by code unit",
    body: { port: 4904, protocol: "http", name: "corpus-café-日本語", stubs: [STUB("/d")] },
  },
  {
    port: 4905,
    why: "stubs with no id, which cannot be addressed by id and get inert controls plus one shared note (#324)",
    body: {
      port: 4905,
      protocol: "http",
      name: "corpus-idless-stubs",
      stubs: [STUB("/e"), STUB("/f")],
    },
  },
];

/** What the admin API says the tenant holds — the oracle every assertion below is measured against. */
async function apiImposters(
  page: Page,
  key: string,
): Promise<{ port?: number; name?: string; stubCount?: number }[]> {
  const response = await page.request.get("/imposters", { headers: { Authorization: key } });
  expect(response.ok(), `GET /imposters failed: ${response.status()}`).toBeTruthy();
  return ((await response.json()) as { imposters: { port?: number; name?: string; stubCount?: number }[] })
    .imposters;
}

/**
 * The index of a column in the rendered table, by its header label.
 *
 * Not a hard-coded number: the leading select column and the trailing actions column are both
 * role-dependent, so a fixed index silently reads the wrong cell for some roles. Header and body
 * rows carry matching leading cells, so the header's own index is the honest one.
 */
async function columnIndex(page: Page, label: RegExp): Promise<number> {
  const headers = await page.locator("table.dense thead th").allTextContents();
  const index = headers.findIndex((text) => label.test(text.trim()));
  expect(index, `no column header matching ${label} in [${headers.join(", ")}]`).toBeGreaterThan(-1);
  return index;
}

/*
 * Deliberately NOT `describe.serial`. These tests share only the corpus that `beforeAll` seeds —
 * each signs in for itself and asserts an independent relation — and `serial` SKIPS every remaining
 * test in the group once one fails. That turns this file into a worse diagnostic than it should be:
 * reverting two defects at once produced one failure and seven skips, when the whole point of an
 * oracle is to report every disagreement it found in one run.
 */
test.describe("the imposter list agrees with the fleet", () => {
  test.beforeAll(async ({ browser }) => {
    const { keys } = fixture();
    const context = await browser.newContext();
    const page = await context.newPage();
    for (const entry of CORPUS) {
      const response = await page.request.post("/imposters", {
        headers: { Authorization: keys.editor, "Content-Type": "application/json" },
        data: entry.body,
      });
      expect(
        response.ok(),
        `could not seed :${entry.port} (${entry.why}) — ${response.status()} ${await response.text()}`,
      ).toBeTruthy();
    }
    await context.close();
  });

  test.afterAll(async ({ browser }) => {
    // Best-effort, but it must not be silent: a leaked corpus imposter changes the imposter-table
    // baseline `visual.spec.ts` takes later in the same run, and that failure would point at the
    // wrong file entirely.
    const { keys } = fixture();
    const context = await browser.newContext();
    const page = await context.newPage();
    const leaked: number[] = [];
    for (const entry of CORPUS) {
      const response = await page.request.delete(`/imposters/${entry.port}`, {
        headers: { Authorization: keys.editor },
      });
      if (!response.ok()) leaked.push(entry.port);
    }
    await context.close();
    expect(leaked, `corpus imposters left behind: ${leaked.join(", ")}`).toEqual([]);
  });

  test("every imposter the API returns has a row, and no row is invented", async ({ page }) => {
    const { keys } = fixture();
    await signIn(page, "editor");
    const expected = await apiImposters(page, keys.editor);
    const ports = expected.flatMap((imposter) => (imposter.port === undefined ? [] : [imposter.port]));
    expect(ports.length, "the fleet returned nothing, so nothing below would be asserted").toBeGreaterThan(
      CORPUS.length,
    );

    for (const port of ports) {
      await expect(page.getByTestId(`imposter-row-${port}`), `no row for :${port}`).toBeVisible();
    }
    // Both directions: a screen showing rows the fleet does not have is as wrong as one missing them.
    await expect(page.locator('[data-testid^="imposter-row-"]')).toHaveCount(ports.length);
  });

  test("every row's stub count is the count the API reports", async ({ page }) => {
    /*
     * #323 in one assertion. The list projection sends `stubCount` and omits the `stubs` array, so
     * a console reading only the array renders `—` here for every row while the number sits unread
     * in the same payload — and no fixture-based test can see that, because the fixture decides
     * which field it sends.
     */
    const { keys } = fixture();
    await signIn(page, "editor");
    const expected = await apiImposters(page, keys.editor);
    const stubs = await columnIndex(page, /^stubs$/i);

    for (const imposter of expected) {
      if (imposter.port === undefined || imposter.stubCount === undefined) continue;
      const cell = page.getByTestId(`imposter-row-${imposter.port}`).locator("td").nth(stubs);
      await expect(cell, `:${imposter.port} stub count`).toHaveText(String(imposter.stubCount));
    }
  });

  test("every imposter is reachable from the list, named or not", async ({ page }) => {
    /*
     * #321 in one assertion, and the reason a click-every-button crawler would never have found it:
     * the defect was a link that was never rendered, and a crawler only clicks what exists.
     * Reachability has to be asserted against the fleet's list rather than against the page's.
     */
    const { keys } = fixture();
    await signIn(page, "editor");
    const expected = await apiImposters(page, keys.editor);

    for (const imposter of expected) {
      if (imposter.port === undefined) continue;
      const link = page
        .getByTestId(`imposter-row-${imposter.port}`)
        .locator(`a[href="#/imposters/${imposter.port}"]`);
      await expect(link, `:${imposter.port} (name: ${imposter.name ?? "<none>"}) has no link to its detail screen`)
        .toHaveCount(1);
    }
  });

  test("a nameless imposter's link says which imposter it opens", async ({ page }) => {
    // Reachable is not the same as usable: the visible text no longer identifies the row, so the
    // accessible name has to carry the port or the link announces as nothing.
    await signIn(page, "editor");
    const cell = page.getByTestId("imposter-name-4901");
    await expect(cell).toHaveText("(unnamed)");
    await expect(cell.locator("xpath=ancestor::a")).toHaveAttribute(
      "aria-label",
      "Open unnamed imposter on port 4901",
    );
  });

  test("following a nameless imposter's link lands on its detail screen", async ({ page }) => {
    await signIn(page, "editor");
    await page.getByTestId("imposter-name-4901").click();
    await expect(page).toHaveURL(/#\/imposters\/4901$/);
    await expect(page.getByTestId("detail-port")).toContainText("4901");
    // The authoring controls the whole screen exists for — the thing being unreachable cost.
    await expect(page.getByRole("button", { name: /add stub/i })).toBeVisible();
  });

  test("zero stubs reads as 0, never as unknown", async ({ page }) => {
    const { keys } = fixture();
    await signIn(page, "editor");
    const stubs = await columnIndex(page, /^stubs$/i);
    const cell = page.getByTestId("imposter-row-4902").locator("td").nth(stubs);
    await expect(cell).toHaveText("0");
    // And the fleet agrees it is zero rather than absent, so the screen is not merely self-consistent.
    const api = (await apiImposters(page, keys.editor)).find((i) => i.port === 4902);
    expect(api?.stubCount).toBe(0);
  });

  test("a long name truncates for display without breaking its link", async ({ page }) => {
    await signIn(page, "editor");
    const cell = page.getByTestId("imposter-name-4903");
    const shown = (await cell.textContent()) ?? "";
    expect(shown.length).toBeLessThan("corpus-checkout-service-integration-sandbox-eu-west-1".length);
    // The whole value stays reachable on the title, and the link still resolves.
    await expect(cell).toHaveAttribute(
      "title",
      "corpus-checkout-service-integration-sandbox-eu-west-1",
    );
    await expect(cell.locator("xpath=ancestor::a")).toHaveAttribute("href", "#/imposters/4903");
  });

  test("a non-ASCII name renders and links", async ({ page }) => {
    await signIn(page, "editor");
    const cell = page.getByTestId("imposter-name-4904");
    await expect(cell).toHaveText("corpus-café-日本語");
    await expect(cell.locator("xpath=ancestor::a")).toHaveAttribute("href", "#/imposters/4904");
  });

  test("id-less stubs get inert controls and exactly one shared explanation", async ({ page }) => {
    // #324: two id-less stubs, so this also pins that the reason is not repeated per row — which is
    // what made the Actions column hold a paragraph and blew the row to ~200px.
    await signIn(page, "editor");
    await page.goto("/console/#/imposters/4905");
    await expect(page.getByTestId("stub-row-0")).toBeVisible();

    await expect(page.getByTestId("stub-not-addressable")).toHaveCount(2);
    await expect(page.getByTestId("stub-idless-note")).toHaveCount(1);

    // The layout invariant the DOM assertions above cannot express: a table row is a row, not a
    // paragraph. jsdom computes no layout at all, so this is only checkable in a real browser.
    const box = await page.getByTestId("stub-row-0").boundingBox();
    expect(box, "stub row has no box").not.toBeNull();
    expect(box!.height, "an id-less stub's row should not be paragraph-shaped").toBeLessThan(80);
  });
});
