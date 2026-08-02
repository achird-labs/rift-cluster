/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const IMPOSTER = { port: 4545, protocol: "http", name: "checkout-api", enabled: true, stubs: [] };

const LISTED = {
  "/imposters": { json: { imposters: [IMPOSTER] } },
  "/_fleet/members": { status: 404 },
  "/_fleet/health": { status: 404 },
};

afterEach(() => vi.unstubAllGlobals());

describe("creating an imposter", () => {
  it("is not offered to a role that cannot write", async () => {
    // Presentation only — the admin front refuses the same principal either way — but a button that
    // can only ever answer 403 is worse than no button.
    stubFetch(LISTED);
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });

    await screen.findByTestId("imposters-scope-label");
    expect(screen.queryByTestId("new-imposter")).toBeNull();
  });

  it("sends the port explicitly, because the fleet cannot assign one", async () => {
    /*
     * `createImposter` requires the port in the body: an auto-assigned port cannot replicate, since
     * each node would pick its own. So the form collects it and this asserts it is actually sent —
     * a console that omitted it would get a 400 the operator could do nothing about.
     */
    const { calls } = stubFetch({ ...LISTED, "/imposters ": { json: IMPOSTER } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("new-imposter"));
    await user.type(screen.getByLabelText(/^port$/i), "4600");
    await user.type(screen.getByLabelText(/^name$/i), "billing-api");
    await user.click(screen.getByRole("button", { name: /create imposter/i }));

    await waitFor(() => expect(calls.filter((c) => c === "/imposters").length).toBeGreaterThan(1));
    const post = vi.mocked(fetch).mock.calls.find(([, init]) => init?.method === "POST");
    expect(post).toBeDefined();
    const body = JSON.parse(String(post?.[1]?.body)) as Record<string, unknown>;
    expect(body).toMatchObject({ port: 4600, protocol: "http", name: "billing-api", enabled: true });
  });

  it("refuses a port outside 1–65535 without a round trip", async () => {
    // Named next to the field rather than returned as a 400 with nothing to point at.
    stubFetch(LISTED);
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("new-imposter"));
    await user.type(screen.getByLabelText(/^port$/i), "70000");
    await user.click(screen.getByRole("button", { name: /create imposter/i }));

    expect((await screen.findByTestId("new-imposter-invalid")).textContent).toMatch(/65535/);
    expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
  });

  it("refuses https with no certificate, which the engine would reject at creation anyway", async () => {
    // Upstream fails loudly rather than silently serving cleartext, so a form that let this through
    // would only relay that error later.
    stubFetch(LISTED);
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("new-imposter"));
    await user.type(screen.getByLabelText(/^port$/i), "4600");
    await user.selectOptions(screen.getByLabelText(/protocol/i), "https");
    await user.click(screen.getByRole("button", { name: /create imposter/i }));

    expect((await screen.findByTestId("new-imposter-invalid")).textContent).toMatch(/certificate/i);
    expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
  });
});

describe("deleting an imposter", () => {
  it("is gated on imposter.delete, not imposter.write", async () => {
    // The two are granted together today. This asserts the console asks the question the server
    // actually answers, so the day `authz.rs` moves one the table is what fails.
    stubFetch(LISTED);
    renderInApp(<Imposters />, { whoami: whoamiWith("operator") });

    await screen.findByTestId("imposters-scope-label");
    expect(screen.queryByTestId("delete-imposter-4545")).toBeNull();
  });

  it("asks before it deletes, and names what goes", async () => {
    stubFetch(LISTED);
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(await screen.findByTestId("delete-imposter-4545"));

    const dialog = await screen.findByTestId("confirm-delete-imposter");
    // Stubs, recorded requests and flow state go with it, and nothing undoes that — the dialog says
    // so rather than asking "are you sure?".
    expect(dialog.textContent).toMatch(/stubs/i);
    expect(dialog.textContent).toMatch(/nothing undoes it/i);
    // Nothing sent yet.
    expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "DELETE")).toBe(false);
  });

  it("sends the delete once confirmed", async () => {
    stubFetch({ ...LISTED, "/imposters/4545": { status: 204 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("delete-imposter-4545"));
    await user.click(within(await screen.findByTestId("confirm-delete-imposter")).getByTestId("confirm-destructive"));

    await waitFor(() =>
      expect(
        vi.mocked(fetch).mock.calls.some(
          ([input, init]) => String(input) === "/imposters/4545" && init?.method === "DELETE",
        ),
      ).toBe(true),
    );
  });
});
