/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { POLL_INTERVAL_MS } from "../app/query.ts";
import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, setTabVisibility, stubFetch, whoamiWith } from "./harness.tsx";

const IMPOSTERS = {
  "/imposters": { json: { imposters: [{ port: 4545, protocol: "http", recordRequests: false, enabled: true }] } },
};

function pollsSoFar(calls: string[]): number {
  return calls.filter((path) => path === "/imposters").length;
}

beforeEach(() => {
  setTabVisibility("visible");
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  setTabVisibility("visible");
});

describe("polling (RFC-006 §6 — polling, not realtime)", () => {
  it("refetches the imposter list on the 5s interval while the tab is visible", async () => {
    // Fake timers are installed *before* the render: the poll interval is scheduled during mount,
    // so swapping the clock afterwards would leave it on the real one and this test would measure
    // nothing.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch(IMPOSTERS);
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });
    await screen.findByText("4545");

    const before = pollsSoFar(calls);
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 3 + 100);
    expect(pollsSoFar(calls)).toBeGreaterThan(before);
  });

  it("stops polling while the tab is hidden, and resumes when it is shown again", async () => {
    // The criterion the issue asks to *verify, not assume*: an abandoned tab that keeps polling is
    // one request every 5s per tab, forever. Asserting the option is set would not catch a later
    // per-query override; counting fetches across a real visibilitychange does.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch(IMPOSTERS);
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });
    await screen.findByText("4545");

    setTabVisibility("hidden");
    const whileHidden = pollsSoFar(calls);
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 6 + 100);
    expect(pollsSoFar(calls)).toBe(whileHidden);

    setTabVisibility("visible");
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS + 100);
    await waitFor(() => expect(pollsSoFar(calls)).toBeGreaterThan(whileHidden));
  });
});