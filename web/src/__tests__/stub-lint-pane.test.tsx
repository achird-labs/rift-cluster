/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { REVISION_HEADER } from "../api/client.ts";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/**
 * The lint pane renders the findings it is given (RFC-006 §12 Q1, and #248's AC8).
 *
 * This lives in its own file because it has to `vi.mock` the lint module, and that mock is
 * file-scoped: the sibling `stub-editor.test.tsx` deliberately exercises the *other* path, where no
 * wasm artifact exists and `lintStub` resolves `"unavailable"`. Both paths are real — every dev
 * checkout and CI run hits the second — but only this one proves a finding ever reaches the DOM.
 *
 * Without it, `lintStub` returning findings and `LintPane` rendering them were separately tested and
 * never joined up, so the whole pane could stop displaying findings with the suite still green.
 */
vi.mock("../features/stubs/lint.ts", () => ({
  lintStub: vi.fn(() =>
    Promise.resolve([
      { severity: "error", code: "R001", message: "a stub with no predicates matches everything", location: "stub[0]" },
      { severity: "warning", code: "R014", message: "response body is not valid JSON for its Content-Type" },
    ]),
  ),
}));

const PORT = 4545;

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AC8 — rift-lint findings render for the stub being edited", () => {
  it("shows every finding, with its severity and code, once the editor is open", async () => {
    stubFetch({
      [`/imposters/${PORT}`]: {
        json: {
          port: PORT,
          host: "0.0.0.0",
          protocol: "http",
          name: "billing",
          recordRequests: false,
          enabled: true,
          stubs: [{ id: "s-1", responses: [{ is: { statusCode: 200 } }] }],
        },
        headers: { [REVISION_HEADER]: "default:4545@7" },
      },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("editor") });

    await userEvent.click(await screen.findByRole("button", { name: /edit/i }));

    const pane = await screen.findByTestId("stub-lint");
    // The findings themselves, not merely that the pane exists — a pane rendering "No findings."
    // over a non-empty list is exactly the silent regression this guards.
    expect(pane.textContent).toContain("R001");
    expect(pane.textContent).toContain("a stub with no predicates matches everything");
    expect(pane.textContent).toContain("stub[0]");
    expect(pane.textContent).toContain("R014");
    expect(pane.textContent).not.toMatch(/No findings/i);
  });
});
