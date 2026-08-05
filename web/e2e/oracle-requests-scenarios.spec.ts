import { expect, fixture, signIn, test } from "./fixture.ts";
import type { Page } from "@playwright/test";

/**
 * The request log and the scenarios screen, checked against the fleet's own answer.
 *
 * The companion to `oracle.spec.ts`, which does this for the imposter list. Same reasoning, and it
 * is worth restating because it is the reason three defects reached master: every other spec here
 * asserts that a screen shows what the test *told* it to show, so a fixture that disagrees with the
 * server produces a green suite and a broken screen. These assert a RELATION — what the admin API
 * returns is what the screen must say — so nothing hard-codes what the fleet holds.
 *
 * ── The corpus ──────────────────────────────────────────────────────────────
 *
 * Seeded here rather than in `scripts/e2e-console.sh`, for the reason `oracle.spec.ts` gives: that
 * script's imposters are snapshotted by `visual.spec.ts`'s imposter-table baseline, and extra rows
 * there would fail an unrelated baseline on every run. Ports are 49xx to stay clear of the seeded
 * 46xx range, and `afterAll` asserts the teardown actually happened.
 *
 * The seeded fixture cannot cover these cases. It records traffic on 4645/4646 but drives a handful
 * of requests and no scenario anywhere — so paging, an empty log, and a scenario whose state has
 * been moved off its default are all unreachable without seeding them.
 */

/** `RequestLog.tsx`'s own constant. A page shows this many rows however long the log is. */
const PAGE_SIZE = 50;

const REQUESTS_FEW = 4911;
const REQUESTS_PAGED = 4912;
const REQUESTS_EMPTY = 4913;
const SCENARIOS = 4914;

/** Matches anything, so recorded traffic is about the REQUESTS rather than about stub matching. */
const CATCH_ALL = { responses: [{ is: { statusCode: 200, body: "ok" } }] };

const CORPUS = [
  {
    port: REQUESTS_FEW,
    why: "a short log, where every row on screen should be checkable against the API one for one",
    body: { port: REQUESTS_FEW, protocol: "http", name: "corpus-log-short", recordRequests: true, stubs: [CATCH_ALL] },
  },
  {
    port: REQUESTS_PAGED,
    why: "a log longer than one page — the count and the rows shown must disagree, honestly",
    body: { port: REQUESTS_PAGED, protocol: "http", name: "corpus-log-paged", recordRequests: true, stubs: [CATCH_ALL] },
  },
  {
    port: REQUESTS_EMPTY,
    why: "recording on, nothing recorded — an empty log is not the same fact as a log that could not be read",
    body: { port: REQUESTS_EMPTY, protocol: "http", name: "corpus-log-empty", recordRequests: true, stubs: [CATCH_ALL] },
  },
  {
    port: SCENARIOS,
    why: "two scenarios, one moved off its default state, so the screen has something to get wrong",
    body: {
      port: SCENARIOS,
      protocol: "http",
      name: "corpus-scenarios",
      stubs: [
        { scenarioName: "checkout", responses: [{ is: { statusCode: 200, body: "checkout" } }] },
        { scenarioName: "refund", responses: [{ is: { statusCode: 200, body: "refund" } }] },
      ],
    },
  },
];

/** How many rows past a full page, so the paged case is unambiguous rather than exactly PAGE_SIZE. */
const PAGED_TOTAL = PAGE_SIZE + 5;

type RecordedRequest = { method?: string; path?: string };
type Scenario = { name?: string; state?: string };

async function apiRequests(page: Page, port: number, key: string): Promise<RecordedRequest[]> {
  const response = await page.request.get(`/imposters/${port}/requests`, {
    headers: { Authorization: key },
  });
  expect(response.ok(), `GET /imposters/${port}/requests → ${response.status()}`).toBeTruthy();
  return (await response.json()) as RecordedRequest[];
}

async function apiScenarios(
  page: Page,
  port: number,
  key: string,
): Promise<{ flowId: string; scenarios: Scenario[] }> {
  const response = await page.request.get(`/imposters/${port}/scenarios`, {
    headers: { Authorization: key },
  });
  expect(response.ok(), `GET /imposters/${port}/scenarios → ${response.status()}`).toBeTruthy();
  return (await response.json()) as { flowId: string; scenarios: Scenario[] };
}

/** The total the pager reports, parsed out of "1–50 of 55 on this node". */
async function shownTotal(page: Page): Promise<number> {
  const label = (await page.getByTestId("request-total").textContent()) ?? "";
  const match = /of\s+(\d+)\s+on this node/.exec(label);
  expect(match, `could not read a total out of ${JSON.stringify(label)}`).not.toBeNull();
  return Number(match![1]);
}

test.beforeAll(async ({ browser }) => {
  const { keys } = fixture();
  const context = await browser.newContext();
  const page = await context.newPage();

  for (const entry of CORPUS) {
    const created = await page.request.post("/imposters", {
      headers: { Authorization: keys.editor, "Content-Type": "application/json" },
      data: entry.body,
    });
    expect(
      created.ok(),
      `could not seed :${entry.port} (${entry.why}) — ${created.status()} ${await created.text()}`,
    ).toBeTruthy();
  }

  /*
   * Traffic against the imposter's OWN port, not the admin API — that is what a recorded request is.
   * Gateway traffic is auth-exempt (RFC-002 §7), so no credential rides along.
   *
   * Distinct paths on the short log so each row is individually identifiable; the paged log only
   * needs volume.
   */
  for (const path of ["/alpha", "/beta", "/gamma"]) {
    await page.request.get(`http://127.0.0.1:${REQUESTS_FEW}${path}`);
  }
  for (let i = 0; i < PAGED_TOTAL; i += 1) {
    await page.request.get(`http://127.0.0.1:${REQUESTS_PAGED}/r/${i}`);
  }

  // Move one scenario off its default so the screen has a value it could plausibly get wrong. The
  // other is left alone on purpose: the oracle must agree with the fleet about both.
  const moved = await page.request.put(`/imposters/${SCENARIOS}/scenarios/checkout/state`, {
    headers: { Authorization: keys.editor, "Content-Type": "application/json" },
    data: { state: "awaiting-payment" },
  });
  expect(moved.ok(), `could not set a scenario state — ${moved.status()} ${await moved.text()}`).toBeTruthy();

  await context.close();
});

test.afterAll(async ({ browser }) => {
  // Asserted, not best-effort: a leaked corpus imposter changes the imposter-table baseline
  // `visual.spec.ts` takes later in the same run, and that failure would name the wrong file.
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

/*
 * Not `describe.serial` — see `oracle.spec.ts`. These share only the corpus, and serial would skip
 * every remaining test after the first disagreement, which is the opposite of what an oracle is for.
 */
test.describe("the request log agrees with the fleet", () => {
  test("the total it reports is the number of requests the API holds", async ({ page }) => {
    const { keys } = fixture();
    await signIn(page, "editor");

    for (const port of [REQUESTS_FEW, REQUESTS_PAGED]) {
      const expected = await apiRequests(page, port, keys.editor);
      await page.goto(`/console/#/requests/${port}`);
      await expect(page.getByTestId("request-total")).toBeVisible();
      expect(await shownTotal(page), `:${port} total`).toBe(expected.length);
    }
  });

  test("every row shown is a request the API returned, in the same order", async ({ page }) => {
    /*
     * The relation a fixture-based test cannot express: the rows are not merely well-formed, they
     * are THESE requests. Checked on the short log so the whole log fits one page and the comparison
     * is one for one.
     */
    const { keys } = fixture();
    await signIn(page, "editor");
    const expected = await apiRequests(page, REQUESTS_FEW, keys.editor);
    await page.goto(`/console/#/requests/${REQUESTS_FEW}`);

    const rows = page.getByTestId("request-row");
    await expect(rows).toHaveCount(expected.length);
    const shown = await page.getByTestId("request-open").allTextContents();
    expect(shown).toEqual(expected.map((request) => request.path ?? "—"));
  });

  test("a log longer than a page shows one page and says so", async ({ page }) => {
    /*
     * Paging honesty. A screen that renders 50 of 55 rows and reports "55" is right; one that
     * reports 50 is lying about the fleet, and one that renders all 55 has quietly dropped the
     * pager. Both are only visible by comparing the two numbers against the API.
     */
    const { keys } = fixture();
    await signIn(page, "editor");
    const expected = await apiRequests(page, REQUESTS_PAGED, keys.editor);
    expect(expected.length, "the paged corpus did not record enough traffic").toBeGreaterThan(PAGE_SIZE);

    await page.goto(`/console/#/requests/${REQUESTS_PAGED}`);
    await expect(page.getByTestId("request-row")).toHaveCount(PAGE_SIZE);
    expect(await shownTotal(page)).toBe(expected.length);
  });

  test("an imposter that recorded nothing says so, rather than reporting a count", async ({ page }) => {
    // Empty is not unknown. `request-log-unknown` is the banner for a read that failed, and showing
    // it here would turn "nothing happened yet" into "this node is broken".
    const { keys } = fixture();
    await signIn(page, "editor");
    expect(await apiRequests(page, REQUESTS_EMPTY, keys.editor)).toEqual([]);

    await page.goto(`/console/#/requests/${REQUESTS_EMPTY}`);
    await expect(page.getByTestId("request-log-empty")).toBeVisible();
    await expect(page.getByTestId("request-log-unknown")).toHaveCount(0);
    await expect(page.getByTestId("request-row")).toHaveCount(0);
  });
});

test.describe("the scenarios screen agrees with the fleet", () => {
  test("every scenario the API reports is on screen with exactly that state", async ({ page }) => {
    const { keys } = fixture();
    await signIn(page, "editor");
    const { scenarios } = await apiScenarios(page, SCENARIOS, keys.editor);
    expect(scenarios.length, "the corpus declared no scenarios, so nothing below is asserted").toBeGreaterThan(0);

    await page.goto(`/console/#/scenarios/${SCENARIOS}`);
    for (const scenario of scenarios) {
      if (scenario.name === undefined) continue;
      const cell = page.getByTestId(`scenario-state-${scenario.name}`);
      await expect(cell, `scenario ${scenario.name} missing from the screen`).toBeVisible();
      await expect(cell, `scenario ${scenario.name} state`).toHaveText(String(scenario.state ?? ""));
    }
  });

  test("it invents no scenario the API did not report", async ({ page }) => {
    const { keys } = fixture();
    await signIn(page, "editor");
    const { scenarios } = await apiScenarios(page, SCENARIOS, keys.editor);

    await page.goto(`/console/#/scenarios/${SCENARIOS}`);
    // Every state cell on screen, whatever its name — the count must match the fleet's list.
    await expect(page.locator('[data-testid^="scenario-state-"]')).toHaveCount(scenarios.length);
  });

  test("it names the flow the states were actually read under", async ({ page }) => {
    /*
     * Every scenario state is per-space. The contract makes the imposter echo which flow it resolved
     * rather than letting a client assume "default", and `space.ts` treats a body without `flowId`
     * as unreadable for exactly that reason — so a screen naming a different flow than the one the
     * states came from is attributing them to the wrong space.
     */
    const { keys } = fixture();
    await signIn(page, "editor");
    const { flowId } = await apiScenarios(page, SCENARIOS, keys.editor);
    expect(flowId, "the fleet echoed no flow, so this assertion would be vacuous").toBeTruthy();

    await page.goto(`/console/#/scenarios/${SCENARIOS}`);
    await expect(page.getByTestId("resolved-flow")).toHaveText(flowId);
  });
});
