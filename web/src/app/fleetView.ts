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
  /**
   * The fleet's operator-set name (#373), or `null` when nobody has named it yet. `null` is an
   * absence, the same as `leader` above — never rendered as an empty string, which on screen
   * would be indistinguishable from a name that failed to load.
   */
  fleetName: string | null;
  /**
   * Whether the answering node could not *read* the name, as opposed to there being none to read
   * (#373). Two different facts that would otherwise both arrive as `fleetName: null`, and they
   * want opposite reactions: "nobody has named this fleet" is a thing to go and fix in the
   * console, "this node's storage did not answer" is a thing to go and fix on the node.
   */
  fleetNameUnavailable: boolean;
  /**
   * What each voter reports about itself (#361), keyed by node id.
   *
   * The console is served under `default-src 'self'`, so the page can only ever dial the node that
   * served it — a peer's applied index is unreachable from the browser by construction and arrives
   * only through this projection, which the serving node assembles by asking each peer.
   *
   * Keyed rather than an array so `voters` stays the single source of *who is in the fleet*: a row
   * is looked up, and a voter with no row renders as unknown rather than disappearing from the
   * list. A membership that shrank because a peer was slow is the one thing this panel must not
   * show.
   */
  members: Map<string, MemberRow>;
  /**
   * Writes the fleet has accepted and not yet replayed (#360), summed across voters.
   *
   * `null` when the serving node could not read its own depth — a sum missing an unknown addend is
   * not a sum, so the field is absent rather than reported as a total that quietly excludes a node.
   */
  parkedIntents: number | null;
  /**
   * A voter did not answer the health fan-out, so `parkedIntents` is a lower bound (#360).
   *
   * Distinct from `parkedIntents === null`, which is this node failing to read its *own* depth and
   * means there is no sum at all. A floor and a missing answer are different things to tell an
   * operator, so they are different fields.
   */
  parkedIntentsPartial: boolean;
};

/**
 * What one voter reports about its own bind attempts (#369), as a discriminated union rather than
 * three loose nullable fields.
 *
 * The wire carries `bound_ports`/`bind_failures`/`bind_status_unavailable` as three independently
 * nullable fields because that is what a rolling upgrade and a fan-out timeout can each produce —
 * but nothing downstream of this module should have to re-derive "so is this node's answer usable"
 * from three nulls every time it asks. `known: false` collapses every reason that derivation can
 * fail into the one fact that matters to a caller: there is no positive answer for this node, so
 * every port on it reads as unknown. `why` is kept only because the two unusable cases want
 * different sentences on screen ("did not answer" vs "did not report").
 */
export type BindReport =
  | { known: true; boundPorts: number[]; failures: Record<string, string> }
  | { known: false; why: "unreachable" | "unreported" };

/** One voter's own report, as the members projection carries it (#361). */
export type MemberRow = {
  /** `null` when that node did not answer — an unknown index, never `0`. */
  lastApplied: number | null;
  /** `null` when that node did not answer. */
  isLeader: boolean | null;
  /** Whether the serving node got an answer from this voter inside its fan-out budget. */
  reachable: boolean;
  /** That voter's own bind report (#369). See `BindReport` for why it is a union, not three fields. */
  bind: BindReport;
};

/**
 * A voter's raw bind fields, reduced to the shape `bindStatus` actually needs.
 *
 * Unreachable is checked first and independently of the other three: an unreachable row's
 * `bound_ports`/`bind_failures`/`bind_status_unavailable` are `null` for the same reason its
 * `last_applied` is — nobody answered — and that is a different fact from a reachable node that
 * answered with nothing to report.
 */
function bindReportOf(row: {
  reachable: boolean;
  bound_ports?: number[] | null;
  bind_failures?: Record<string, string> | null;
  bind_status_unavailable?: boolean | null;
}): BindReport {
  if (!row.reachable) return { known: false, why: "unreachable" };
  /*
   * Any of the three missing, `null`, or an explicit `bind_status_unavailable: true` means this
   * node has no usable answer — a pre-#369 peer omits the keys entirely, and a node that could not
   * read its own config sends them `null` even though it *did* answer the fan-out. `?? []`/`?? {}`
   * here would turn either case into "checked, nothing failed", which is the exact silent-bound
   * defect this field exists to prevent.
   */
  if (
    row.bound_ports == null ||
    row.bind_failures == null ||
    row.bind_status_unavailable == null ||
    row.bind_status_unavailable
  ) {
    return { known: false, why: "unreported" };
  }
  return { known: true, boundPorts: row.bound_ports, failures: row.bind_failures };
}

export function fleetView(
  members: FleetMembers,
  health: FleetHealth,
  healthPartial = false,
): FleetView {
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
    fleetName: members.fleet_name ?? null,
    // `?? false` is a domain-optional read, not a swallow: the field is optional in the contract,
    // and a node that does not send it is one that had nothing to report as unreadable.
    fleetNameUnavailable: members.fleet_name_unavailable ?? false,
    /*
     * `?? []` is a domain-optional read, not a swallowed failure: `members` is optional in the
     * contract, so a response without it is a shape the schema permits and every voter simply
     * reads as unknown — which is exactly what this panel showed before #361 published the rows.
     */
    parkedIntents: health.parked_intents_fleet ?? null,
    parkedIntentsPartial: healthPartial,
    members: new Map(
      (members.members ?? []).map((row) => [
        row.node_id,
        {
          lastApplied: row.last_applied ?? null,
          isLeader: row.is_leader ?? null,
          reachable: row.reachable,
          bind: bindReportOf(row),
        },
      ]),
    ),
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

/**
 * A single voter's answer for a single port (#369).
 *
 * Three states, and deliberately no fourth: "unknown" carries `why` rather than being three
 * separate states of its own, because every screen that renders this treats all three the same
 * way (a neutral row, never a green one) and only needs the reason for the hover text. Collapsing
 * "bound" and "unknown" into one optimistic default is exactly the defect `bound_ports` being a
 * positive list exists to prevent — see `BindReport` for why.
 */
export type BindState =
  | { state: "bound" }
  | { state: "failed"; reason: string }
  | { state: "unknown"; why: "unreachable" | "unreported" | "not-applied" };

/**
 * Per-node bind status for `port`, one entry per voter, keyed by node id (#369).
 *
 * Iterates `view.voters`, not `view.members.keys()`, for the same reason `view.members` itself is
 * keyed rather than an array: a voter with no row must render as unknown, not vanish from the
 * panel. A membership that appears to have shrunk because one node was slow to answer the bind
 * fan-out is the one thing this must not show.
 */
export function bindStatus(view: FleetView, port: number): Map<string, BindState> {
  return new Map(
    view.voters.map((voter): [string, BindState] => {
      const row = view.members.get(voter);
      if (row === undefined) return [voter, { state: "unknown", why: "unreported" }];
      const { bind } = row;
      if (!bind.known) return [voter, { state: "unknown", why: bind.why }];
      const reason = bind.failures[String(port)];
      if (reason !== undefined) return [voter, { state: "failed", reason }];
      if (bind.boundPorts.includes(port)) return [voter, { state: "bound" }];
      /*
       * In neither list: this node answered and reported nothing wrong, but it also never claimed
       * the socket — raft lag (the imposter is committed but not yet applied here) and "this node
       * has never heard of this port" both look like this. `bound_ports` is positive precisely so
       * this case is representable instead of defaulting to "bound".
       */
      return [voter, { state: "unknown", why: "not-applied" }];
    }),
  );
}

/**
 * The fleet-wide answer for one port (#369): "failed" if any voter reports a failed bind, "bound"
 * only if every voter confirms it, "unknown" otherwise.
 *
 * `"bound"` requires unanimity on purpose. Two of three voters confirming the socket says nothing
 * about the third — it might be bound, failed, or not yet applied — and asserting "bound" from a
 * majority would be the same optimistic-default failure `bindStatus` exists to refuse, one level up.
 */
export function bindVerdict(view: FleetView, port: number): "bound" | "failed" | "unknown" {
  const statuses = [...bindStatus(view, port).values()];
  if (statuses.some((status) => status.state === "failed")) return "failed";
  if (statuses.length > 0 && statuses.every((status) => status.state === "bound")) return "bound";
  return "unknown";
}
