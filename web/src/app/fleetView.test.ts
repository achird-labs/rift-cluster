import { describe, expect, it } from "vitest";

import type { components } from "../api/schema.ts";
import { fleetView, viewConfidence } from "./fleetView.ts";

type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];

/*
 * Realistic raft ids, as strings.
 *
 * Every fixture here used to say `voters: [1, 2, 3]`. A single-digit id cannot round-trip wrong, so
 * the defect these types now prevent — a `u64` above 2^53-1 silently rounded by `JSON.parse` — was
 * unreachable from the fixtures even in principle. `A` is deliberately one that a double cannot
 * hold: `Number("3342140982834931156")` is `3342140982834931000`.
 */
const A = "3342140982834931156";
const B = "3481475601826307430";
const C = "17445687154000629855";

const THREE_NODE: FleetMembers = {
  node_id: A,
  is_leader: true,
  current_leader: A,
  last_applied: 412,
  voters: [A, B, C],
};

const HEALTHY: FleetHealth = {
  ready: true,
  state: "ready",
  pending_gates: [],
  isolated: false,
  ring: { m_idx: 7, members: [A, B, C] },
};

const SINGLE_NODE: FleetMembers = {
  node_id: A,
  is_leader: true,
  current_leader: A,
  last_applied: 9,
  voters: [A],
};

const SINGLE_HEALTHY: FleetHealth = {
  ready: true,
  state: "ready",
  pending_gates: [],
  isolated: false,
  ring: { m_idx: 1, members: [A] },
};

describe("fleetView on a healthy 3-node fleet", () => {
  const view = fleetView(THREE_NODE, HEALTHY);

  it("reports the fleet's identity from the schema'd fields", () => {
    // Compared against the string literals, not against `Number(...)` of them: a `toBe(Number(A))`
    // here would pass while carrying the very rounding this change exists to stop.
    expect(view.nodeId).toBe(A);
    expect(view.isLeader).toBe(true);
    expect(view.leader).toBe(A);
    expect(view.lastApplied).toBe(412);
    expect(view.voters).toEqual([A, B, C]);
    expect(view.ringEpoch).toBe(7);
    expect(view.ringMembers).toEqual([A, B, C]);
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
      { node_id: "9007199254740993", is_leader: false, current_leader: A, last_applied: 400, voters: [A, B, C] },
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
    expect(fleetView(THREE_NODE, { ...HEALTHY, ring: { m_idx: 7, members: [A, B] } }).degraded).toEqual([]);
    expect(
      fleetView(THREE_NODE, { ...HEALTHY, ring: { m_idx: 8, members: [A, B, C, "9007199254740993"] } }).degraded,
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

describe("per-voter applied indices (#361)", () => {
  it("carries what each voter reported about itself", () => {
    const view = fleetView(
      {
        ...THREE_NODE,
        members: [
          { node_id: A, last_applied: 412, is_leader: true, reachable: true },
          { node_id: B, last_applied: 409, is_leader: false, reachable: true },
          { node_id: C, last_applied: 411, is_leader: false, reachable: true },
        ],
      },
      HEALTHY,
    );

    expect(view.members.get(B)?.lastApplied).toBe(409);
    expect(view.members.get(C)?.lastApplied).toBe(411);
    // The ids survive as strings — a `Number(id)` anywhere in the parse would round these two.
    expect([...view.members.keys()]).toEqual([A, B, C]);
  });

  /*
   * The assertion this feature exists to protect. A voter that did not answer has an *unknown*
   * index; rendering it as `0` would report "that node has applied nothing" — an alarm about the
   * fleet raised by a fan-out that merely timed out.
   */
  it("keeps an unreachable voter's index unknown rather than zero", () => {
    const view = fleetView(
      {
        ...THREE_NODE,
        members: [
          { node_id: A, last_applied: 412, is_leader: true, reachable: true },
          { node_id: B, last_applied: null, is_leader: null, reachable: false },
          { node_id: C, last_applied: 411, is_leader: false, reachable: true },
        ],
      },
      HEALTHY,
    );

    expect(view.members.get(B)?.lastApplied).toBeNull();
    expect(view.members.get(B)?.lastApplied).not.toBe(0);
    expect(view.members.get(B)?.reachable).toBe(false);
    // The voter list is untouched: a fleet does not shrink because one node was slow.
    expect(view.voters).toEqual([A, B, C]);
  });

  // `members` is optional in the contract, so a body without it is a shape the schema permits —
  // every voter simply reads as unknown, which is what this panel showed before #361.
  it("treats an absent members array as every voter unknown, not as an empty fleet", () => {
    const view = fleetView(THREE_NODE, HEALTHY);

    expect(view.members.size).toBe(0);
    expect(view.voters).toEqual([A, B, C]);
  });
});

/*
 * The fleet's name (#373). Pinned here, in the file whose whole job is this transform, rather than
 * only through the two components that render it — a `?? null` or `?? false` flipped here would
 * otherwise be caught only indirectly, if at all.
 */
describe("the fleet name (#373)", () => {
  it("carries the name through", () => {
    const view = fleetView({ ...THREE_NODE, fleet_name: "rift-prod-eu" }, HEALTHY);

    expect(view.fleetName).toBe("rift-prod-eu");
    expect(view.fleetNameUnavailable).toBe(false);
  });

  it("reads an explicit null as unnamed, not unavailable", () => {
    const view = fleetView({ ...THREE_NODE, fleet_name: null }, HEALTHY);

    expect(view.fleetName).toBeNull();
    expect(view.fleetNameUnavailable).toBe(false);
  });

  it("keeps 'could not be read' distinct from 'nobody named it'", () => {
    // The whole reason the server sends two fields. Collapsing them here would undo that server
    // side decision silently, and both states render as `null` on this side.
    const view = fleetView(
      { ...THREE_NODE, fleet_name: null, fleet_name_unavailable: true },
      HEALTHY,
    );

    expect(view.fleetName).toBeNull();
    expect(view.fleetNameUnavailable).toBe(true);
  });

  it("treats a node that sends neither field as having nothing to report", () => {
    // The pre-#373 wire shape, which an older node in a mixed-version fleet still sends. It has not
    // failed to read anything, so it must not read as unavailable.
    const view = fleetView(THREE_NODE, HEALTHY);

    expect(view.fleetName).toBeNull();
    expect(view.fleetNameUnavailable).toBe(false);
  });
});
