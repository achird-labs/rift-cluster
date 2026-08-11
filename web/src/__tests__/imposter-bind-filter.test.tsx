/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/**
 * The console filter half of issue #369 (bind status per node), AC4 and E9: "Bind failures" must
 * show exactly the imposters actually failing somewhere, and must never fold "could not check" into
 * either "failing" or "healthy" — the defect class RFC-006 exists to prevent, reproduced at the
 * filter layer (blocker B3).
 *
 * Modelled on `imposters.test.tsx`'s harness conventions (`stubFetch`, `renderInApp`, `whoamiWith`).
 */

afterEach(() => {
  window.location.hash = "";
  vi.unstubAllGlobals();
});

const TWO_IMPOSTERS = {
  imposters: [
    { port: 4545, protocol: "http", name: "flaky", recordRequests: false, enabled: true, stubs: [] },
    { port: 4546, protocol: "http", name: "steady", recordRequests: false, enabled: true, stubs: [] },
  ],
};

const ONE_IMPOSTER = {
  imposters: [
    { port: 4545, protocol: "http", name: "flaky", recordRequests: false, enabled: true, stubs: [] },
  ],
};

describe("the imposter list's bind-failures filter", () => {
  it("shows only the imposters that failed to bind somewhere", async () => {
    stubFetch({
      "/imposters": { json: TWO_IMPOSTERS },
      "/_fleet/members": {
        json: {
          node_id: "n1",
          is_leader: true,
          current_leader: "n1",
          last_applied: 10,
          voters: ["n1", "n2"],
          members: [
            {
              node_id: "n1",
              last_applied: 10,
              is_leader: true,
              reachable: true,
              bound_ports: [4546],
              bind_failures: { "4545": "Address already in use" },
              bind_status_unavailable: false,
            },
            {
              node_id: "n2",
              last_applied: 10,
              is_leader: false,
              reachable: true,
              bound_ports: [4545, 4546],
              bind_failures: {},
              bind_status_unavailable: false,
            },
          ],
        },
      },
      "/_fleet/health": {
        json: {
          ready: true,
          state: "ready",
          pending_gates: [],
          isolated: false,
          ring: { m_idx: 2, members: ["n1", "n2"] },
        },
      },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("imposter-row-4545")).toBeTruthy();
    expect(await screen.findByTestId("imposter-row-4546")).toBeTruthy();

    await userEvent.setup().click(await screen.findByTestId("quick-bind-failures"));

    expect(await screen.findByTestId("imposter-row-4545")).toBeTruthy();
    expect(screen.queryByTestId("imposter-row-4546")).toBeNull();
  });

  it("does not offer the filter to a principal that cannot read the fleet", async () => {
    stubFetch({ "/imposters": { json: TWO_IMPOSTERS } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    await screen.findByTestId("imposter-row-4545");

    expect(screen.queryByTestId("quick-bind-failures")).toBeNull();
    expect(screen.getByTitle(/tracked as issue #369/)).toBeTruthy();
  });

  it("says how many rows it could not classify rather than calling them healthy", async () => {
    // B3's exact scenario: `?bind=failed` arrives from a shared link, this session cannot read the
    // fleet (`fleet.read` withheld), so `fleetForBind` is `null` while the filter is already active.
    window.location.hash = "#/imposters?bind=failed";
    stubFetch({ "/imposters": { json: TWO_IMPOSTERS } });
    renderInApp(<Imposters />, { whoami: whoamiWith("editor") });

    await screen.findByTestId("imposter-filters");

    const note = await screen.findByTestId("imposter-filter-bind-unclassified");
    expect(note.textContent).toMatch(/2 not shown/);
    expect(note.textContent).toMatch(/bind status could not be confirmed/i);
  });

  it("counts a node that did not answer as unconfirmed, not as bound", async () => {
    stubFetch({
      "/imposters": { json: ONE_IMPOSTER },
      "/_fleet/members": {
        json: {
          node_id: "n1",
          is_leader: true,
          current_leader: "n1",
          last_applied: 10,
          voters: ["n1", "n2"],
          members: [
            {
              node_id: "n1",
              last_applied: 10,
              is_leader: true,
              reachable: true,
              bound_ports: [4545],
              bind_failures: {},
              bind_status_unavailable: false,
            },
            {
              node_id: "n2",
              last_applied: null,
              is_leader: null,
              reachable: false,
              bound_ports: null,
              bind_failures: null,
              bind_status_unavailable: null,
            },
          ],
        },
      },
      "/_fleet/health": {
        json: {
          ready: true,
          state: "ready",
          pending_gates: [],
          isolated: false,
          ring: { m_idx: 2, members: ["n1", "n2"] },
        },
      },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    await userEvent.setup().click(await screen.findByTestId("quick-bind-failures"));

    await waitFor(() => {
      expect(screen.queryByTestId("imposter-row-4545")).toBeNull();
    });
    const note = await screen.findByTestId("imposter-filter-bind-unclassified");
    expect(note.textContent).toMatch(/1 not shown/);
  });
});
