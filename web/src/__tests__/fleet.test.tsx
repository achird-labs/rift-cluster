/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Fleet } from "../screens/Fleet.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const THREE_NODE = {
  "/_fleet/members": { json: { node_id: 2, is_leader: false, current_leader: 1, last_applied: 412, voters: [1, 2, 3] } },
  "/_fleet/health": { json: { ready: true, state: "ready", pending_gates: [], isolated: false, ring: { m_idx: 7, members: [1, 2, 3] } } },
};

const SINGLE_NODE = {
  "/_fleet/members": { json: { node_id: 1, is_leader: true, current_leader: 1, last_applied: 9, voters: [1] } },
  "/_fleet/health": { json: { ready: true, state: "ready", pending_gates: [], isolated: false, ring: { m_idx: 1, members: [1] } } },
};

afterEach(() => {
  vi.unstubAllGlobals();
});

/*
 * The fleet's name (#373). Three states, not two — and the third is the one worth testing, because
 * "could not read the name" and "nobody has named it" both arrive as `fleet_name: null` and would
 * otherwise render identically. They send an operator to different places: one is a fault on the
 * answering node, the other is a console task nobody has done yet.
 */
describe("the fleet's name", () => {
  const withMembers = (extra: Record<string, unknown>) => ({
    ...THREE_NODE,
    "/_fleet/members": {
      json: { ...THREE_NODE["/_fleet/members"].json, ...extra },
    },
  });

  it("renders the name the fleet was given", async () => {
    stubFetch(withMembers({ fleet_name: "rift-prod-eu", fleet_name_unavailable: false }));
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-name")).textContent).toBe("rift-prod-eu");
  });

  it("says Unnamed when nobody has named it, rather than leaving a gap", async () => {
    // The state every existing deployment is in the moment it upgrades, so it is the common case,
    // not an edge one. A blank here reads as "still loading" and invites a refresh that changes
    // nothing.
    stubFetch(withMembers({ fleet_name: null, fleet_name_unavailable: false }));
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-name")).textContent).toBe("Unnamed");
  });

  it("distinguishes a name it could not read from a fleet with no name", async () => {
    stubFetch(withMembers({ fleet_name: null, fleet_name_unavailable: true }));
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    const name = await screen.findByTestId("fleet-name");
    expect(name.textContent).toBe("Unavailable");
    expect(name.textContent).not.toBe("Unnamed");
  });

  it("treats a node that omits the flag entirely as having nothing to report", async () => {
    // Field-absent is the pre-#373 wire shape and must not read as "unavailable" — an older node
    // in a mixed-version fleet has not failed to read anything.
    stubFetch(withMembers({}));
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-name")).textContent).toBe("Unnamed");
  });
});

describe("cluster screen against a 3-node fleet", () => {
  it("names this node, the leader, the ring epoch and the voters", async () => {
    stubFetch(THREE_NODE);
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-node")).textContent).toContain("2");
    expect(screen.getByTestId("fleet-leader").textContent).toContain("1");
    expect(screen.getByTestId("fleet-ring-epoch").textContent).toContain("7");
    expect(screen.getByTestId("fleet-voters").textContent).toContain("3");
    expect(screen.getByTestId("fleet-applied").textContent).toContain("412");
  });

  describe("node ids are never split across a line break", () => {
    /*
     * A raft node id is a 19-digit number. Joined into one text node, a comma-separated list of them
     * wraps wherever it happens to fit — mid-digit on a narrow tile — and the reader sees
     * `334214098283493100` above `0`: two plausible ids that do not exist. This is the one value on
     * the screen where a line break changes what it *says*, not just how it looks.
     *
     * jsdom computes no layout, so the wrap itself is not observable here. What is observable is the
     * mechanism that permits it: whether each id is its own unbreakable element, or one run of text.
     */
    const REALISTIC = {
      "/_fleet/members": {
        json: {
          node_id: 3481475601826307600,
          is_leader: true,
          current_leader: 3481475601826307600,
          last_applied: 30,
          voters: [3342140982834931000, 3481475601826307600, 17445687154000630000],
        },
      },
      "/_fleet/health": {
        json: {
          ready: true,
          state: "ready",
          pending_gates: [],
          isolated: false,
          ring: { m_idx: 7, members: [3342140982834931000, 3481475601826307600, 17445687154000630000] },
        },
      },
    };

    it("gives every voter its own unbreakable element", async () => {
      stubFetch(REALISTIC);
      renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

      const cell = await screen.findByTestId("fleet-voters");
      expect(cell.querySelectorAll(".nobreak")).toHaveLength(3);
    });

    it("gives every ring member its own unbreakable element", async () => {
      stubFetch(REALISTIC);
      renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

      const cell = await screen.findByTestId("fleet-ring-epoch");
      expect(cell.querySelectorAll(".nobreak")).toHaveLength(3);
      // The epoch is prose around the list, not a member — it must not be wrapped as an id.
      expect(cell.textContent).toContain("epoch 7");
    });

    it("still separates them with a comma a line may break at", async () => {
      // The separators stay OUTSIDE the unbreakable spans on purpose: a list that cannot break at
      // all overflows its tile instead, which trades one rendering bug for another.
      stubFetch(REALISTIC);
      renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

      const cell = await screen.findByTestId("fleet-voters");
      expect(cell.textContent).toContain("3342140982834931000, 3481475601826307600");
      for (const span of cell.querySelectorAll(".nobreak")) {
        expect(span.textContent).not.toContain(",");
      }
    });
  });

  it("labels the reading as this node's own view, never the fleet's", async () => {
    // `/_fleet/*` is one node answering about itself; presenting it as the fleet's state is the
    // vacuous-test equivalent the issue calls out.
    stubFetch(THREE_NODE);
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-scope-label")).textContent).toMatch(/this node/i);
  });
});

describe("cluster screen against a single node", () => {
  it("renders without implying two nodes are missing", async () => {
    stubFetch(SINGLE_NODE);
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-node")).textContent).toContain("1");
    expect(screen.queryByTestId("fleet-degraded")).toBeNull();
    expect(screen.getByTestId("fleet-voters").textContent).toContain("1");
  });
});

describe("degraded and unknown states", () => {
  it("names each degradation rather than showing a bare warning colour", async () => {
    stubFetch({
      "/_fleet/members": { json: { node_id: 3, is_leader: false, current_leader: null, last_applied: null, voters: [1, 2, 3] } },
      "/_fleet/health": {
        json: { ready: false, state: "not-ready", pending_gates: ["cluster-joined"], isolated: true, ring: { m_idx: 7, members: [1, 2] } },
      },
    });
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    const degraded = await screen.findByTestId("fleet-degraded");
    expect(degraded.textContent).toMatch(/isolated/i);
    expect(degraded.textContent).toMatch(/not[- ]ready/i);
    expect(screen.getByTestId("fleet-pending-gates").textContent).toContain("cluster-joined");
  });

  it("renders an unknown applied index as an em dash, never as zero", async () => {
    // `last_applied: null` means this node has applied nothing it can name. Rendering it as 0 turns
    // "unknown" into a reassuring number — the distinction the prototype settled deliberately.
    stubFetch({
      "/_fleet/members": { json: { node_id: 3, is_leader: false, current_leader: null, last_applied: null, voters: [1, 2, 3] } },
      "/_fleet/health": { json: { ready: false, state: "not-ready", pending_gates: [], isolated: false, ring: { m_idx: 7, members: [1, 2, 3] } } },
    });
    renderInApp(<Fleet />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("fleet-applied")).textContent).toContain("—");
    expect(screen.getByTestId("fleet-applied").textContent).not.toContain("0");
    expect(screen.getByTestId("fleet-leader").textContent).toContain("—");
  });

  it("explains a 404 as insufficient scope rather than as a missing page", async () => {
    // RFC-002 §8.4 makes "you may not" and "it is not there" indistinguishable on the wire. The
    // console must not translate that into "the fleet has no cluster".
    stubFetch({ "/_fleet/members": { status: 404, json: { message: "not found" } }, "/_fleet/health": { status: 404, json: { message: "not found" } } });
    renderInApp(<Fleet />, { whoami: whoamiWith("viewer") });

    const error = await screen.findByRole("alert");
    expect(error.textContent).toMatch(/fleet-scoped|not available to this principal/i);
  });
});