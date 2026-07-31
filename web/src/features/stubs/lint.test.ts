import { afterEach, describe, expect, it, vi } from "vitest";

import { LINT_MODULE_URL, decodeFindings, lintStub, resetLintModule } from "./lint.ts";

afterEach(() => {
  resetLintModule();
  vi.unstubAllGlobals();
});

describe("the advisory linter degrades to unavailable rather than to silence", () => {
  it("resolves unavailable when the wasm artifact is not on this build", async () => {
    // `web/public/lint/` is populated by the release lane, so in dev and under test the import
    // fails. That is the honest degraded mode: the pane says the server is still the authority,
    // rather than showing an empty finding list that reads as "your stub is fine".
    resetLintModule();
    expect(await lintStub('{"id":"s-1"}')).toBe("unavailable");
  });

  it("loads the artifact at most once across repeated lints", async () => {
    const loads: string[] = [];
    resetLintModule();
    const load = vi.fn(async (url: string) => {
      loads.push(url);
      throw new Error("no artifact on this build");
    });
    await lintStub("{}", load);
    await lintStub("{}", load);
    expect(loads).toEqual([LINT_MODULE_URL]);
  });

  it("returns the findings the wasm export hands back as JSON text", async () => {
    resetLintModule();
    const load = async (): Promise<{ default: () => Promise<void>; lint_stub: (json: string) => string }> => ({
      default: () => Promise.resolve(),
      lint_stub: () =>
        JSON.stringify([
          { severity: "error", code: "E010", message: "responses must not be empty", location: "stubs[0]" },
        ]),
    });
    expect(await lintStub("{}", load)).toEqual([
      { severity: "error", code: "E010", message: "responses must not be empty", location: "stubs[0]" },
    ]);
  });

  it("treats a boundary that answers unparseable text as unavailable, not as a clean bill", async () => {
    // A classifier that cannot read what it is classifying must not answer "no findings" — that is
    // the safe class, and this one cannot prove it.
    resetLintModule();
    const load = async (): Promise<{ default: () => Promise<void>; lint_stub: (json: string) => string }> => ({
      default: () => Promise.resolve(),
      lint_stub: () => "not json",
    });
    expect(await lintStub("{}", load)).toBe("unavailable");
  });
});

describe("decoding one finding", () => {
  it("keeps the fields the pane renders and drops nothing it needs", () => {
    const decoded = decodeFindings(
      JSON.stringify([
        { severity: "warning", code: "W001", message: "m", location: "l", suggestion: "s" },
        { severity: "info", code: "I1", message: "m2" },
      ]),
    );
    expect(decoded).toEqual([
      { severity: "warning", code: "W001", message: "m", location: "l", suggestion: "s" },
      { severity: "info", code: "I1", message: "m2" },
    ]);
  });

  it("answers null for anything that is not an array of findings", () => {
    for (const text of ["{}", "null", "[1]", "[{}]", "oops"]) {
      expect([text, decodeFindings(text)]).toEqual([text, null]);
    }
  });
});
