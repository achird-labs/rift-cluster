/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Shell } from "../app/Shell.tsx";
import { NAV } from "../app/nav.ts";
import { renderInApp, stubFetch } from "./harness.tsx";

const QUIET = {
  "/imposters": { json: { imposters: [] } },
  "/_fleet/members": { status: 404 },
  "/_fleet/health": { status: 404 },
};

afterEach(() => {
  vi.unstubAllGlobals();
  window.location.hash = "";
});

describe("nav — the five screens, in two runs", () => {
  it("draws every nav entry as a link, named by its label, and nothing greyed (#553)", async () => {
    // The roadmap chips RFC-006 §4 asked for are gone with the trim: every screen the reduced
    // console promises is built, so the bar is five links and no `[data-planned]` element. Asserted
    // from the rendered DOM rather than the model alone, so a chip cannot leak back in through the
    // renderer without this failing.
    stubFetch(QUIET);
    renderInApp(<Shell />);

    await screen.findByTestId("nav-imposters");
    const bar = screen.getByRole("navigation", { name: /console sections/i });
    const links = within(bar).getAllByRole("link");
    expect(links.map((link) => link.textContent?.replace(/^\S\s*/, "").trim())).toEqual(
      NAV.map((entry) => entry.label),
    );
    expect(links.map((link) => link.textContent)).toEqual(
      expect.arrayContaining([expect.stringContaining("Router")]),
    );
    expect(document.querySelectorAll('[data-planned="true"]').length).toBe(0);
    expect(within(bar).getAllByRole("group").map((g) => g.getAttribute("aria-label"))).toEqual([
      "Mocks",
      "Fleet",
    ]);
  });

  it("navigates between the live screens without a page load", async () => {
    stubFetch(QUIET);
    renderInApp(<Shell />);

    await userEvent.setup().click(await screen.findByRole("link", { name: /^cluster$/i }));
    await waitFor(() => expect(window.location.hash).toBe("#/cluster"));
  });

  it("offers the cluster screen even when the fleet projection is refusing", async () => {
    // `QUIET` answers `/_fleet/*` with 404. The entry stays: since #550 there is one identity and
    // one credential, so a 404 here is the fleet's state rather than a limit on this operator, and
    // the screen's own refusal note is what says so. Hiding the entry would leave an operator with
    // no route to the screen that explains it.
    stubFetch(QUIET);
    renderInApp(<Shell />);

    expect(await screen.findByTestId("nav-cluster")).toBeTruthy();
  });
});

/*
 * The fleet name in the top bar (#373) — the placement the issue argues actually matters, because
 * it is the only one visible on every screen. An operator with staging and production open in two
 * tabs can otherwise tell them apart only by port number, while every destructive act this console
 * offers is fleet-wide.
 */
describe("the fleet name in the top bar", () => {
  const NAMED = {
    ...QUIET,
    "/_fleet/members": {
      json: {
        node_id: 1,
        is_leader: true,
        current_leader: 1,
        last_applied: 9,
        voters: [1],
        fleet_name: "rift-prod-eu",
        fleet_name_unavailable: false,
      },
    },
    "/_fleet/health": {
      json: {
        ready: true,
        state: "ready",
        pending_gates: [],
        isolated: false,
        ring: { m_idx: 1, members: [1] },
      },
    },
  };

  it("shows the name in the top bar", async () => {
    stubFetch(NAMED);
    renderInApp(<Shell />);

    expect((await screen.findByTestId("topbar-fleet-name")).textContent).toContain("rift-prod-eu");
  });

  it("shows nothing at all when the fleet read fails", async () => {
    // `QUIET` answers `/_fleet/members` with 404. The badge must stay absent rather than render a
    // placeholder: a fleet whose name could not be read is not thereby an unnamed one, and every
    // screen would otherwise carry a permanent empty label.
    stubFetch(QUIET);
    renderInApp(<Shell />);

    await screen.findByTestId("nav-imposters");
    expect(screen.queryByTestId("topbar-fleet-name")).toBeNull();
  });

  it("shows nothing when the fleet is readable but unnamed", async () => {
    // Deliberately different from the Ring card, which says "Unnamed" — that card is the one place
    // the fact has a panel to itself. A global chrome label reading "Unnamed" on every screen,
    // before anyone has ever set a name, is noise.
    stubFetch({
      ...NAMED,
      "/_fleet/members": {
        json: { ...NAMED["/_fleet/members"].json, fleet_name: null },
      },
    });
    renderInApp(<Shell />);

    await screen.findByTestId("nav-imposters");
    expect(screen.queryByTestId("topbar-fleet-name")).toBeNull();
  });
});