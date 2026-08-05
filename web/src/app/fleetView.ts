import type { components } from "../api/schema.ts";

type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];

/**
 * A named reason this node's answers may not describe the whole fleet. Each maps to a schema'd
 * field, so the screen can say *which* fact it is reporting instead of showing a warning colour.
 */
export type Degradation = "not-ready" | "draining" | "isolated" | "no-leader" | "evicted";

export type FleetView = {
  /*
   * Node ids are STRINGS, all the way through.
   *
   * A raft id is a `u64` and the fleet screen renders it verbatim. Carried as a `number` it would
   * be an IEEE-754 double, and every id above 2^53-1 silently rounds — the console displayed
   * `3342140982834931000` for a node the fleet calls `3342140982834931156`. The wire now sends
   * strings for exactly this reason, and nothing here may convert them back: `Number(id)` would
   * reintroduce the defect one line from where it was fixed.
   *
   * `lastApplied` and `ringEpoch` stay numbers. They are magnitudes rather than identifiers, and a
   * log index reaching 2^53 is not a reachable state.
   */
  nodeId: string;
  isLeader: boolean;
  /** `null` when this node knows of no leader — an unknown, not the id `"0"`. */
  leader: string | null;
  lastApplied: number | null;
  voters: string[];
  ringEpoch: number;
  ringMembers: string[];
  ready: boolean;
  state: FleetHealth["state"];
  pendingGates: string[];
  isolated: boolean;
  /** A one-voter fleet is a supported deployment, not a fleet missing two nodes. */
  singleNode: boolean;
  degraded: Degradation[];
};

export function fleetView(members: FleetMembers, health: FleetHealth): FleetView {
  const ringMembers = health.ring.members;
  const degraded: Degradation[] = [];

  if (health.state === "draining") {
    // Draining is a deliberate, announced departure. Reporting it as "not ready" would file an
    // operator's own graceful leave under the same heading as a node that failed to come up.
    degraded.push("draining");
  } else if (!health.ready) {
    degraded.push("not-ready");
  }
  if (health.isolated) degraded.push("isolated");
  if (members.current_leader === null) degraded.push("no-leader");

  /*
   * Evicted from the effective membership while still running and still answering.
   *
   * This is the degradation with no other tell. `is_isolated` (`raft/node.rs`) returns false for
   * any node that can see a leader, the readiness gates stay satisfied, and `state` stays `ready` —
   * so without this check the node reports itself perfectly healthy while owning no part of the
   * ring and receiving no further replication. It is precisely the node whose "no imposters" must
   * not be believed.
   *
   * Deliberately NOT checked: `voters ⊄ ring.members`. Both arrive from the same
   * `membership_config.voter_ids()` — `members_body` sends it directly, `health_body` sends
   * `Ring::new` of it, which only sorts and dedups — so within one snapshot they are the same set
   * and the divergence is unrepresentable. Comparing them across the two requests this view makes
   * would report a sub-second read skew as a persistent fleet degradation. `ring.m_idx` is the
   * fencing token if that skew ever needs naming.
   */
  if (!members.voters.includes(members.node_id)) degraded.push("evicted");

  return {
    nodeId: members.node_id,
    isLeader: members.is_leader,
    leader: members.current_leader,
    lastApplied: members.last_applied,
    voters: members.voters,
    ringEpoch: health.ring.m_idx,
    ringMembers,
    ready: health.ready,
    state: health.state,
    pendingGates: health.pending_gates,
    isolated: health.isolated,
    // Exactly one, not "at most one": zero voters is a node with no applied membership at all,
    // which is a fault, not the supported single-node deployment.
    singleNode: members.voters.length === 1,
    degraded,
  };
}

const WORDING: Record<Degradation, string> = {
  "not-ready": "this node is not ready",
  draining: "this node is draining",
  isolated: "this node sees itself as isolated from the rest of the fleet",
  "no-leader": "this node knows of no leader",
  evicted: "this node is no longer in the fleet's voter set",
};

/**
 * Why the fleet reading is absent, when it is. The two are different sentences on screen and must
 * not collapse into one: not asking is a settled fact about the principal, whereas asking and
 * failing means the qualification this screen would have applied is simply missing.
 */
export type FleetReadState =
  | { kind: "read"; view: FleetView }
  | { kind: "not-asked" }
  | { kind: "unavailable" };

export type ViewConfidence = {
  /** True only on evidence. Absence of evidence is reported as neither partial nor complete. */
  partial: boolean;
  reason: string | null;
  /** True when the reading that would qualify this screen could not be obtained. */
  unknown: boolean;
};

/**
 * How much a read served by *this* node is worth right now.
 *
 * `not-asked` is the common case, not an edge one: the fleet projection is `ClusterAdmin`-gated, so
 * every role below FleetAdmin is refused it and the imposter list does not even try. Calling that
 * "partial" would put a permanent warning on a healthy console until operators stopped reading it;
 * calling it "complete" would assert something nothing supports. It says neither, and the screens
 * still label every reading as this node's own.
 *
 * `unavailable` is the case that must not be folded into it. A FleetAdmin whose `/_fleet/*` read
 * failed is not a principal without the scope — the screen has lost the very signal that would
 * have told it whether to trust an empty list, and saying nothing would present that as a clean
 * reading.
 */
export function viewConfidence(state: FleetReadState): ViewConfidence {
  if (state.kind === "unavailable") {
    return {
      partial: false,
      reason: "this node's fleet reading could not be obtained, so staleness cannot be assessed",
      unknown: true,
    };
  }
  if (state.kind === "not-asked" || state.view.degraded.length === 0) {
    return { partial: false, reason: null, unknown: false };
  }
  return {
    partial: true,
    reason: state.view.degraded.map((degradation) => WORDING[degradation]).join("; "),
    unknown: false,
  };
}
