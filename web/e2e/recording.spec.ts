import { expect, goToScreen, signIn, test } from "./fixture.ts";

/**
 * The recording workflow end to end, against a real engine (issue #246).
 *
 * jsdom can prove the console sends the right document; only this can prove the engine accepts it,
 * records against it, and answers the `?replayable=true&removeProxies=true` projection the review
 * table reads. The proxy target is the fixture's own `checkout-api` on 4645 — a second imposter on
 * the same node is a real upstream, and using one keeps the test free of any outbound network.
 *
 * The imposter is created and deleted by this spec rather than reusing a fixture one: promoting
 * *replaces the whole stub list*, so pointing this at a shared imposter would destroy the stubs the
 * other specs and the visual baselines depend on.
 */
const RECORDING_PORT = 4791;

test.describe("recording: start, review, promote", () => {
  test("records a proxied request and promotes it into a static stub", async ({ page, request }) => {
    await signIn(page, "editor");
    await goToScreen(page, "/imposters");

    // --- create a throwaway imposter to record on -----------------------------
    await page.getByTestId("new-imposter").click();
    await page.getByLabel(/^port$/i).fill(String(RECORDING_PORT));
    await page.getByLabel(/^name$/i).fill("e2e-recorder");
    await page.getByRole("button", { name: /create imposter/i }).click();
    await expect(page.getByText("e2e-recorder")).toBeVisible({ timeout: 10_000 });

    await goToScreen(page, `/imposters/${RECORDING_PORT}`);

    // --- start: write one proxy stub through the form -------------------------
    await page.getByRole("button", { name: /start recording/i }).click();
    await page.getByLabel(/proxy target/i).fill("http://127.0.0.1:4645");
    // The preview is the document that will be sent — assert it before sending, which is the whole
    // point of showing it.
    await expect(page.getByTestId("recording-json-preview")).toContainText("proxyOnce");
    await page.getByRole("button", { name: /^start recording$/i }).click();

    // Derived state: the panel reads Recording because a proxy stub is now in the list.
    await expect(page.getByTestId("recorded-none")).toBeVisible({ timeout: 10_000 });

    // --- drive one request through the proxy ----------------------------------
    // Matches `get-order` upstream, so the recorded response is a real 200 with a JSON body.
    const proxied = await request.get(`http://127.0.0.1:${RECORDING_PORT}/orders/42`);
    expect(proxied.status()).toBe(200);

    // --- review: the capture appears, rendered from the removeProxies projection
    await page.reload();
    await expect(page.getByTestId("recorded-row-0")).toBeVisible({ timeout: 15_000 });
    await expect(page.getByTestId("recorded-row-0")).toContainText("/orders/42");
    await expect(page.getByTestId("recorded-body-0")).toContainText('"id"');

    // --- promote: the capture replaces the proxy stub --------------------------
    await page.getByRole("button", { name: /stop & promote/i }).click();
    await page.getByTestId("confirm-destructive").click();

    // The stub table now carries the promoted stub, and the review table is gone because the
    // imposter is no longer recording — the state is the stubs, so promoting changed it.
    await expect(page.locator("tr", { hasText: "/orders/42" }).first()).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByRole("button", { name: /start recording/i })).toBeVisible({
      timeout: 15_000,
    });

    // --- clean up so a re-run starts from the same place ----------------------
    await goToScreen(page, "/imposters");
    await page.getByTestId(`delete-imposter-${RECORDING_PORT}`).click();
    await page.getByTestId("confirm-destructive").click();
    await expect(page.locator("tr", { hasText: "e2e-recorder" })).toHaveCount(0, {
      timeout: 10_000,
    });
  });
});
