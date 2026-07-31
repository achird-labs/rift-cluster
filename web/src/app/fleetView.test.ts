import { describe, expect, it } from "vitest";

import type { components } from "../api/schema.ts";
import { fleetView, viewConfidence } from "./fleetView.ts";

type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];

const THREE_NODE: FleetMembers = {
  node_id: 1,
  is_leader: true,
  current_leader: 1,
  last_applied: 412,
  voters: [1, 2, 3],
};

const HEALTHY: FleetHealth = {
  ready: true,
  state: "ready",
  pending_gates: [],
  isolated: false,
  ring: { m_idx: 7, members: [1, 2, 3] },
};

const SINGLE_NODE: FleetMembers = {
  node_id: 1,
  is_leader: true,
  current_leader: 1,
  last_applied: 9,
  voters: [1],
};

const SINGLE_HEALTHY: FleetHealth = {
  ready: true,
  state: "ready",
  pending_gates: [],
  isolated: false,
  ring: { m_idx: 1, members: [1] },
};

describe("fleetView on a healthy 3-node fleet", () => {
  const view = fleetView(THREE_NODE, HEALTHY);

  it("reports the fleet's identity from the schema'd fields", () => {
    expect(view.nodeId).toBe(1);
    expect(view.isLeader).toBe(true);
    expect(view.leader).toBe(1);
    expect(view.lastApplied).toBe(412);
    expect(view.voters).toEqual([1, 2, 3]);
    expect(view.ringEpoch).toBe(7);
    expect(view.ringMembers).toEqual([1, 2, 3]);
  });

  it("is neither single-node nor degraded", () => {
    expect(view.singleNode).toBe(false);
    expect(view.degraded).toEqual([]);
    expect(viewConfidence({ kind: "read", view }).partial).toBe(false);
  });
});

describe("fleetView on a single node", () => {
  const view = fleetView(SINGLE_NODE, SINGLE_HEALTHY);

  it("is recognised as single-node and is not degraded for having one voter", () => {
    // A one-node fleet is a supported deployment, not a fleet missing two nodes. Flagging it
    // degraded would train operators to ignore the degraded label.
    expect(view.singleNode).toBe(true);
    expect(view.degraded).toEqual([]);
    expect(viewConfidence({ kind: "read", view }).partial).toBe(false);
  });
});

describe("degraded detection", () => {
  it("flags a node that is not ready, and names the gates it is waiting on", () => {
    const view = fleetView(THREE_NODE, {
      ...HEALTHY,
      ready: false,
      state: "not-ready",
      pending_gates: ["cluster-joined", "cluster-reconciled"],
    });
    expect(view.degraded).toContain("not-ready");
    expect(view.pendingGates).toEqual(["cluster-joined", "cluster-reconciled"]);
    expect(viewConfidence({ kind: "read", view }).partial).toBe(true);
  });

  it("flags an isolated node", () => {
    const view = fleetView(THREE_NODE, { ...HEALTHY, isolated: true });
    expect(view.degraded).toContain("isolated");
    expect(viewConfidence({ kind: "read", view }).partial).toBe(true);
  });

  it("flags a node that knows of no leader", () => {
    const view = fleetView({ ...THREE_NODE, is_leader: false, current_leader: null }, HEALTHY);
    expect(view.degraded).toContain("no-leader");
    expect(viewConfidence({ kind: "read", view }).partial).toBe(true);
  });

  it("flags a node that has been evicted from the voter set", () => {
    // The degradation with no other tell: `is_isolated` is false for any node that sees a leader,
    // the readiness gates stay satisfied, and `state` stays `ready`. Without this the node reports
    // itself healthy while owning no part of the ring and receiving no further replication.
    const view = fleetView(
      { node_id: 4, is_leader: false, current_leader: 1, last_applied: 400, voters: [1, 2, 3] },
      HEALTHY,
    );
    expect(view.degraded).toContain("evicted");
    expect(viewConfidence({ kind: "read", view }).partial).toBe(true);
  });

  it("does not read a ring/voter difference as a degradation", () => {
    // `members_body` sends `membership_config.voter_ids()` and `health_body` sends `Ring::new` of
    // the same call, which only sorts and dedups — so within one snapshot they are the same set.
    // Comparing them across this view's two requests would report a sub-second read skew as a
    // persistent fleet degradation, which is a warning an operator can neither act on nor clear.
    expect(fleetView(THREE_NODE, { ...HEALTHY, ring: { m_idx: 7, members: [1, 2] } }).degraded).toEqual([]);
    expect(
      fleetView(THREE_NODE, { ...HEALTHY, ring: { m_idx: 8, members: [1, 2, 3, 4] } }).degraded,
    ).toEqual([]);
  });

  it("names a draining node without calling it unreachable", () => {
    const view = fleetView(THREE_NODE, { ...HEALTHY, ready: false, state: "draining" });
    expect(view.degraded).toContain("draining");
    expect(view.degraded).not.toContain("not-ready");
  });
});

describe("viewConfidence", () => {
  it("makes no claim when the projection was never asked for", () => {
    // Fleet-scoped, so every role below FleetAdmin is refused it and the imposter list does not
    // even try. Asserting "partial" on that absence would put a permanent warning on a healthy
    // console; asserting "complete" would be a claim nothing supports. It says neither.
    const confidence = viewConfidence({ kind: "not-asked" });
    expect(confidence).toEqual({ partial: false, reason: null, unknown: false });
  });

  it("does not fold a failed read into a principal who never asked", () => {
    // The distinction that matters: a FleetAdmin whose `/_fleet/*` read failed has *lost* the
    // signal that would say whether an empty list can be trusted. Reporting that as "not asked"
    // would present the gap as a clean reading.
    const confidence = viewConfidence({ kind: "unavailable" });
    expect(confidence.unknown).toBe(true);
    expect(confidence.partial).toBe(false);
    expect(confidence.reason).toMatch(/could not be obtained/i);
  });

  it("explains the degradation in words rather than only a flag", () => {
    const view = fleetView(THREE_NODE, { ...HEALTHY, isolated: true });
    const confidence = viewConfidence({ kind: "read", view });
    expect(confidence.partial).toBe(true);
    expect(confidence.unknown).toBe(false);
    expect(confidence.reason).toMatch(/isolated/i);
  });
});
