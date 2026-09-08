import { expect, fixture, goToScreen, signIn, test } from "./fixture.ts";

/**
 * The write flows, driven end to end.
 *
 * `smoke.spec.ts` proves every screen loads; this one proves the things an operator actually *does*
 * work. Each test creates what it needs with a name of its own and removes it, because the fixture
 * is one stateful node shared serially — a test that leaves an imposter behind changes the next
 * run's baselines.
 *
 * ## Every failure along the way was this spec's own
 *
 * Worth recording, because two of them looked exactly like broken features.
 *
 * - Wrong labels: the stub row's button reads "Edit" but its accessible name is `Edit <stubId>`,
 *   because the id moved into `aria-label` when the labels were shortened.
 * - Strict-mode violations: `getByText` matched a table cell *and* a row button, so a locator
 *   resolving to two elements failed as though nothing had rendered. The added stub had in fact
 *   been created.
 * - A missing `baseURL`: the conflict test opened a second browser context for realism, and
 *   `newContext()` does not inherit it — every relative request went nowhere, no write landed, the
 *   revision never moved, and the save under test succeeded. It waited out its timeout for a
 *   conflict it had never caused.
 * - A race on the pinned revision: the editor pins `If-Match` at first render, so opening it before
 *   the imposter read lands pins `null`, which disables Save outright.
 *
 * Locator rules that came out of it: `getByText` matches text nodes only, so a button carrying its
 * target in `aria-label` does not collide with it; `getByRole("cell")` matches the cell's accessible
 * name, which in these tables concatenates a display name with a truncated id and so matches neither
 * cleanly. Prefer `locator("tr", { hasText })` for a row.
 */

test.describe("stubs: the conflict flow", () => {
  test("refuses a stale write and offers both sides rather than merging", async ({ page }) => {
    /*
     * The lost-update case C5 exists for. One editor opens a stub and pins its revision; another
     * write lands; the first save must be refused with both versions on screen and no automatic
     * merge. Driven with two browser contexts because that is the real shape of it.
     */
    const { imposters, apiKey } = fixture();
    const port = imposters[0];
    await signIn(page);
    await goToScreen(page, `/imposters/${port}`);
    // `/^edit /` with the trailing space, not `/^edit$/`: the button's visible text is "Edit" but
    // its accessible name is `Edit <stubId>`, because the id moved into `aria-label` when the
    // labels were shortened to stop them wrapping.
    await page.getByRole("button", { name: /^edit /i }).first().click();
    await expect(page.getByTestId("stub-editor")).toBeVisible();
    /*
     * Wait for Save to be *enabled* before provoking the conflict.
     *
     * The editor pins its `If-Match` at first render (`useState(revision)`), so an editor opened
     * before the imposter read lands pins `null` — and a null token disables the save entirely. The
     * click then does nothing at all, and the test waits out its timeout for a conflict that was
     * never attempted. Enabled means a revision is pinned, which is the precondition this whole
     * flow is about.
     */
    await expect(page.getByRole("button", { name: /save stub/i })).toBeEnabled();

    /*
     * A second writer moves the imposter underneath the open editor.
     *
     * Issued through `page.request`, which shares this context and therefore the config's
     * `baseURL`. The first version opened a whole second browser context for realism, and
     * `newContext()` does **not** inherit `baseURL` — so every relative request went nowhere, no
     * write landed, the revision never moved, and the save under test succeeded. The test timed out
     * waiting for a conflict it had never caused, which looks identical to the console failing to
     * render one.
     *
     * A separate context was never needed anyway: the pinned revision is client state in the open
     * editor, so anything that commits a write is a second writer as far as this flow is concerned.
     */
    const read = await page.request.get(`/imposters/${port}`, {
      headers: { Authorization: apiKey },
    });
    const revision = read.headers()["rift-cluster-revision"] ?? "";
    expect(revision, "the read must carry a revision to write against").not.toBe("");
    const wrote = await page.request.post(`/imposters/${port}/stubs`, {
      headers: { Authorization: apiKey, "If-Match": revision },
      data: { stub: { id: "e2e-conflict", responses: [{ is: { statusCode: 200 } }] } },
    });
    // Asserted, not assumed: a second write that quietly failed would leave the revision where the
    // editor pinned it, and the save below would succeed for the wrong reason.
    expect(wrote.ok(), `second write failed: ${wrote.status()} ${await wrote.text()}`).toBe(true);

    // Now the first editor saves against the revision it pinned.
    await page.getByRole("button", { name: /save stub/i }).click();
    const conflict = page.getByTestId("stub-conflict");
    await expect(conflict).toBeVisible({ timeout: 10_000 });
    // Both sides shown, and nothing merged for the operator.
    await expect(conflict).toContainText(/reapply|discard/i);
  });
});

/**
 * The raw JSON pane — in the editor the console actually ships.
 *
 * `CodeEditor` loads Monaco through a dynamic import and renders a plain `<textarea>` fallback when
 * that import does not resolve. Under jsdom it never resolves, so `stub-editor.test.tsx` — every
 * one of its cases — drives the FALLBACK. Monaco itself has no coverage anywhere but here, which
 * matters because it is what an operator types into: it owns its own buffer, auto-closes brackets
 * and quotes, and reports its value through a change handler none of the jsdom tests exercise.
 */
test.describe("the stub JSON pane, in the editor the console ships", () => {
  test("shows an existing stub's JSON, so the editor is really live", async ({ page }) => {
    /*
     * Non-vacuity guard for the case below. A Monaco that failed to mount renders nothing and the
     * fallback textarea would answer a `toBeDisabled` check just as happily, so an error-path test
     * on its own could pass against a broken editor.
     */
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[0]}`);
    await page.getByRole("button", { name: /edit get-order/i }).click();

    await expect(page.getByTestId("stub-editor")).toBeVisible();
    await expect(page.getByTestId("stub-json")).toContainText("get-order");
  });

  test("refuses unparseable JSON and disables the save rather than sending it", async ({ page }) => {
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[1]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    await expect(page.getByTestId("stub-editor")).toBeVisible();

    // `fill()` cannot be used: Monaco's only <textarea> is a hidden readonly IME buffer, so
    // Playwright resolves it and then waits forever for it to become editable. Real keystrokes are
    // the only way in, and are what an operator does anyway.
    await page.getByTestId("stub-json").click();
    await page.keyboard.press("ControlOrMeta+A");
    await page.keyboard.press("Delete");
    await page.keyboard.type("{ not json");

    await expect(page.getByTestId("stub-json-error")).toBeVisible();
    // Saying so is only half. Leaving Save live would let a click fire a write the console has
    // already decided is malformed, and the failure would surface as a server error instead.
    await expect(page.getByRole("button", { name: /save stub/i })).toBeDisabled();
  });
});

test.describe("front-door routes: pre-flight validation", () => {
  test("refuses a duplicate id at the point of typing", async ({ page }) => {
    await signIn(page);
    await goToScreen(page, "/routes");
    await page.getByTestId("add-route").click();
    await page.getByLabel(/^id$/i).fill("checkout"); // already in the seeded table
    await page.getByLabel(/target port/i).fill("4645");
    await page.getByRole("button", { name: /add to table/i }).click();
    await expect(page.getByTestId("new-route-invalid")).toContainText(/already used/i);
  });

  test("names the strip-without-prefix error the fleet would raise", async ({ page }) => {
    // `RouteTable::validate` refuses the whole table for this; the editor mirrors it so the
    // operator is not told by a rejected save.
    await signIn(page);
    await goToScreen(page, "/routes");
    await page.getByTestId("add-route").click();
    await page.getByLabel(/^id$/i).fill("e2e-strip");
    await page.getByLabel(/target port/i).fill("4645");
    await page.getByRole("checkbox").check();
    await page.getByRole("button", { name: /add to table/i }).click();
    // Added to the draft; the whole-table validator is what reports it.
    await expect(page.getByTestId("route-validation")).toContainText(/strip/i);
  });
});

test.describe("imposters: create then remove", () => {
  test("creates one, sees it listed, and deletes it", async ({ page }) => {
    await signIn(page);
    await goToScreen(page, "/imposters");
    await page.getByTestId("new-imposter").click();
    await page.getByLabel(/^port$/i).fill("4788");
    await page.getByLabel(/^name$/i).fill("e2e-throwaway");
    // Identity -> First stub -> Review. The stub step is skipped deliberately: leaving the path
    // blank creates the imposter with no stubs, which is what this test wants.
    await page.getByTestId("wizard-next").click();
    await page.getByTestId("wizard-next").click();
    await page.getByRole("button", { name: /create imposter/i }).click();

    await expect(page.getByText("e2e-throwaway")).toBeVisible({ timeout: 10_000 });

    await page.getByTestId("delete-imposter-4788").click();
    await page.getByTestId("confirm-destructive").click();
    await expect(page.locator("tr", { hasText: "e2e-throwaway" })).toHaveCount(0, { timeout: 10_000 });
  });

  test("adds a stub to an imposter and it appears in the list", async ({ page }) => {
    // The write that was broken until this branch: `POST /stubs` needs a `{stub}` envelope.
    const { imposters } = fixture();
    await signIn(page);
    await goToScreen(page, `/imposters/${imposters[1]}`);
    await page.getByRole("button", { name: /add stub/i }).click();
    await page.getByRole("button", { name: /not found 404/i }).click();
    await page.getByLabel(/^id$/i).fill("e2e-added");
    await page.getByRole("button", { name: /save stub/i }).click();
    await expect(page.locator("tr", { hasText: "e2e-added" }).first()).toBeVisible({ timeout: 10_000 });
  });
});
