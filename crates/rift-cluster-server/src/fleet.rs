//! The read-only `/_fleet/*` admin-port projection (RFC-006 §5.2, issue #185).
//!
//! `/_cluster/*` ([`crate::cluster_api`]) answers the same three questions,
//! but rides the cluster port and is authenticated with the cluster
//! credential. An operator working the admin API has no way to reach it
//! without also holding a credential this surface was never meant to hand
//! out. `/_fleet/*` re-exposes the same bodies on the admin port instead —
//! by calling the cluster port's own builders rather than reimplementing
//! them. Two independent implementations checked by a test can still diverge
//! between test runs; one implementation called twice cannot.
//!
//! The two ports are no longer key-for-key identical, and the distinction
//! matters. This projection may **add** to the cluster port's body — the
//! per-voter `members` fan-out (#361), the fleet-wide `parked_intents_fleet`
//! sum (#360) — but may never **drop** or restate a field. Every addition is
//! a fold over the cluster port's own builder, asked once per node, so there
//! is still exactly one implementation of each fact. What the rule forbids is
//! the two ports answering the *same* question differently, and a sum of N
//! answers is not a second opinion about one of them.
//!
//! The cluster port stays node-local on purpose: it is the target of these
//! fan-outs, so making it fleet-wide would have every peer ask every other
//! peer — and it is how an operator asks *one* node what it thinks.
//! `fleet_projection_matches_the_cluster_port_shapes` enforces exactly this.
//!
//! This module is deliberately pure: it classifies a request into a
//! [`FleetRoute`] and renders a body. It does not authenticate, authorize, or
//! build an HTTP response — that stays the admin front's job, kept in one
//! place rather than spread across every route that needs it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hyper::Method;
use rift_cluster::{NodeId, RaftNode};

use crate::cluster_api::{BindFields, health_body, members_body, op_body};
use crate::readiness::Readiness;

/// A recognized `/_fleet/*` route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FleetRoute {
    Members,
    Health,
    Op(uuid::Uuid),
}

/// Classify `(method, path)` into a `/_fleet/*` route, or `None` if it names
/// none of the three this projection answers.
///
/// A malformed op id names no op the same way an unknown one does (the same
/// reasoning `/_cluster/ops/:id` already documents), so it classifies to
/// `None` here too — the caller's generic 404, not a route match that then
/// fails to render.
pub(crate) fn classify(method: &Method, path: &str) -> Option<FleetRoute> {
    if *method != Method::GET {
        return None;
    }
    match path.strip_prefix("/_fleet/")? {
        "members" => Some(FleetRoute::Members),
        "health" => Some(FleetRoute::Health),
        rest => {
            let id = rest.strip_prefix("ops/")?;
            // The id may carry a query string; only the path part names the op.
            let id = id.split('?').next().unwrap_or_default();
            id.parse::<uuid::Uuid>().ok().map(FleetRoute::Op)
        }
    }
}

/// How long the members fan-out waits for every peer before answering with what it has.
///
/// Matches the journal merge's own peer budget: this is the same trade — an operator read that
/// answers promptly with stated coverage beats one that hangs on an unreachable node.
const MEMBER_PEER_BUDGET: Duration = Duration::from_secs(2);

/// A rendered body, and whether it is complete.
pub(crate) struct FleetBody {
    pub value: serde_json::Value,
    /// A voter did not answer inside [`MEMBER_PEER_BUDGET`], so `members` carries a row it could
    /// not fill (#361). The caller stamps `Rift-Cluster-Partial`.
    pub partial: bool,
}

impl FleetBody {
    /// A body assembled without asking anyone else, so complete by construction.
    fn local(value: serde_json::Value) -> Self {
        Self {
            value,
            partial: false,
        }
    }
}

/// Render the body for `route`. `Ok(None)` is the caller's 404 — it only
/// happens for [`FleetRoute::Op`], since `classify` never produces `Members`
/// or `Health` for a request that could fail to render one.
pub(crate) async fn body(
    route: &FleetRoute,
    node: &Arc<RaftNode>,
    readiness: &Readiness,
) -> Result<Option<FleetBody>, String> {
    match route {
        FleetRoute::Members => Ok(Some(merged_members(node).await)),
        FleetRoute::Health => Ok(Some(fleet_health(node, readiness).await)),
        FleetRoute::Op(op_id) => op_body(node, op_id)
            .map(|found| found.map(FleetBody::local))
            .map_err(|e| e.to_string()),
    }
}

/// `health_body`, plus the fleet's total parked-write backlog (#360).
///
/// The tile this feeds sits beside imposter and source counts that are fleet-wide, and it means
/// "the fleet has taken work it has not finished". A single node's figure under that label would
/// read as the whole fleet's and understate it, so the depth is summed across voters — each node
/// answering for itself, exactly as the members fan-out works.
///
/// `/_cluster/health` keeps carrying only this node's `parked_intents` and does not fan out: it is
/// the target of this fan-out, so making it fleet-wide would have every peer ask every other peer,
/// and it is also how an operator asks *one* node how far behind its own replay is.
///
/// `parked_intents_fleet` is **absent** rather than partial-and-silent when this node could not
/// read its own depth — a sum missing an unknown addend is not a sum. When a *peer* fails to
/// answer, the sum is a floor and the response says so through `Rift-Cluster-Partial`.
async fn fleet_health(node: &Arc<RaftNode>, readiness: &Readiness) -> FleetBody {
    let mut value = health_body(node, readiness);
    let status = node.status();
    let me = status.node_id;

    // This node's own contribution comes from the body just built, so the two can never disagree
    // about the same number.
    let Some(mine) = value
        .get("parked_intents")
        .and_then(serde_json::Value::as_u64)
    else {
        // Already logged where it failed; the field is `null` above and the fleet sum is omitted
        // rather than reported as a total that silently excludes this node.
        return FleetBody {
            value,
            partial: true,
        };
    };

    let mut set = tokio::task::JoinSet::new();
    for peer in status.voters.iter().copied().filter(|&id| id != me) {
        let node = Arc::clone(node);
        set.spawn(async move {
            let outcome = node
                .call_member(peer, "GET", "/_cluster/health", Vec::new())
                .await
                .and_then(|bytes| {
                    serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|e| e.to_string())
                });
            (peer, outcome)
        });
    }

    let mut total = mine;
    let mut unanswered = 0usize;
    let drained = tokio::time::timeout(MEMBER_PEER_BUDGET, async {
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((peer, Ok(reply))) => {
                    let (new_total, usable) = add_peer_depth(total, Some(&reply));
                    total = new_total;
                    if !usable {
                        // The peer answered but could not read its own depth. Its contribution is
                        // unknown, which makes the sum a floor exactly as an unreachable peer does.
                        unanswered += 1;
                        tracing::warn!(peer, "health fan-out: peer reported no parked depth");
                    }
                }
                Ok((peer, Err(e))) => {
                    let (new_total, _usable) = add_peer_depth(total, None);
                    total = new_total;
                    unanswered += 1;
                    tracing::warn!(peer, error = %e, "health fan-out: peer did not answer");
                }
                Err(e) => {
                    let (new_total, _usable) = add_peer_depth(total, None);
                    total = new_total;
                    unanswered += 1;
                    tracing::warn!(error = %e, "health fan-out: task failed");
                }
            }
        }
    })
    .await;

    // A timeout leaves tasks unjoined and their nodes uncounted; dropping the set aborts them.
    let timed_out = drained.is_err();
    if let Some(map) = value.as_object_mut() {
        map.insert("parked_intents_fleet".to_owned(), serde_json::json!(total));
    }
    FleetBody {
        value,
        partial: timed_out || unanswered > 0,
    }
}

/// Fold one peer's `/_cluster/health` reply into the running fleet-wide `parked_intents` sum
/// (issue #401).
///
/// Extracted out of `fleet_health`'s fan-out so the "peer answered" and "peer did not answer"
/// arms are under direct test rather than reachable only through a real fan-out — every wire test
/// here runs a solo node, so an *answering* peer's contribution to the sum was previously
/// reachable by no test at all.
///
/// `reply` is `None` for every way a peer can fail to contribute a number to this call: it did not
/// answer, its task panicked, or (the caller passes `Some` but the value has no field) it answered
/// with a body that carries no `parked_intents`. All three collapse to the same outcome here
/// because they mean the same thing to the sum: this peer's depth is *unknown*, not `0` — folding
/// it in as `0` would let an incomplete sum read as an exact one. The returned `bool` is `true`
/// only when a real depth was added, so the caller can tell "contributed" from "did not" without
/// re-deriving it from the total.
fn add_peer_depth(total: u64, reply: Option<&serde_json::Value>) -> (u64, bool) {
    match reply
        .and_then(|r| r.get("parked_intents"))
        .and_then(serde_json::Value::as_u64)
    {
        Some(depth) => (total.saturating_add(depth), true),
        None => (total, false),
    }
}

/// `members_body`, plus a `members` row per voter carrying that voter's own applied index (#361).
///
/// # Why the fan-out lives here and not in `members_body`
///
/// The console is served under `default-src 'self'`, so the page can only ever dial the node that
/// served it: a peer's applied index is unreachable from the browser *by construction*, and can
/// only arrive through an aggregate the serving node assembles. That is this function.
///
/// It asks each peer the question the cluster port already answers — `GET /_cluster/members` — so
/// there is still exactly **one** implementation of "a node's own applied index", now folded over
/// N nodes instead of read once. This module's header rule is intact in the sense that matters:
/// the risk it guards is two rival implementations drifting, and a fold over one is not that.
///
/// `/_cluster/members` deliberately stays node-local. It is the target of this fan-out, so making
/// it fleet-wide too would have every peer fan out to every other peer on each call — and it is
/// also how an operator asks *one* node what it thinks, which is precisely the question you need
/// when isolating a node that is behind.
///
/// # Additive
///
/// Every field `members_body` produced is still there and still means what it did; `members` is
/// added beside them. An existing reader of `last_applied` (this node's) is unaffected.
async fn merged_members(node: &Arc<RaftNode>) -> FleetBody {
    let mut value = members_body(node);
    let status = node.status();
    let me = status.node_id;

    // This node's own bind fields, read back off the body just built rather than recomputed after
    // the fan-out below (issue #369, blocker B1). `local_bind_state` reads live engine state, and
    // the fan-out awaits for up to `MEMBER_PEER_BUDGET` (2s) — calling it a second time after that
    // await could observe a bind that failed (or recovered) in between, producing a response that
    // claims this node is bound at the top level and failed in its own `members[]` row, or the
    // reverse. Reading the one value both places use makes that divergence unrepresentable rather
    // than merely unlikely, the same choice already made here for `last_applied`/`is_leader` via
    // `status`.
    let local_bind = BindFields {
        bound_ports: value
            .get("bound_ports")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        bind_failures: value
            .get("bind_failures")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        bind_status_unavailable: value
            .get("bind_status_unavailable")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    };

    // Peers are asked concurrently: serially, one unreachable node would spend the whole budget
    // before the next was tried, so a single dead peer would blank every row after it.
    let mut set = tokio::task::JoinSet::new();
    for peer in status.voters.iter().copied().filter(|&id| id != me) {
        let node = Arc::clone(node);
        set.spawn(async move {
            let outcome = node
                .call_member(peer, "GET", "/_cluster/members", Vec::new())
                .await
                .and_then(|bytes| {
                    serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|e| e.to_string())
                });
            (peer, outcome)
        });
    }

    let mut answered: HashMap<NodeId, serde_json::Value> = HashMap::new();
    let drained = tokio::time::timeout(MEMBER_PEER_BUDGET, async {
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((peer, Ok(reply))) => {
                    answered.insert(peer, reply);
                }
                Ok((peer, Err(e))) => {
                    // Logged, never folded into the row as a value: "this node could not be
                    // reached" is what the row says, and the reason belongs where an operator
                    // reading logs will find it rather than in an API body.
                    tracing::warn!(peer, error = %e, "members fan-out: peer did not answer");
                }
                Err(e) => tracing::warn!(error = %e, "members fan-out: task failed"),
            }
        }
    })
    .await;
    // A timeout leaves the remaining tasks unjoined; dropping the set aborts them.
    let timed_out = drained.is_err();

    let rows: Vec<serde_json::Value> = status
        .voters
        .iter()
        .copied()
        .map(|id| {
            member_row(
                id,
                id == me,
                status.last_applied,
                status.is_leader,
                answered.get(&id),
                &local_bind,
            )
        })
        .collect();

    let partial = members_partial(timed_out, &rows);
    if let Some(map) = value.as_object_mut() {
        map.insert("members".to_owned(), serde_json::Value::Array(rows));
    }
    FleetBody { value, partial }
}

/// Build one row of `/_fleet/members`'s `members` array (#361, #369).
///
/// Extracted out of `merged_members`'s fan-out so the row shape is under test on its own — every
/// wire test that exercises it runs a solo node, so `is_me: false` was previously reachable by no
/// test at all (blocker T2).
///
/// `local_bind` is this node's own bind fields, computed once by the caller before the fan-out
/// starts (blocker B1) — passed in rather than recomputed here for the same reason.
fn member_row(
    id: NodeId,
    is_me: bool,
    status_last_applied: Option<u64>,
    status_is_leader: bool,
    answered: Option<&serde_json::Value>,
    local_bind: &BindFields,
) -> serde_json::Value {
    if is_me {
        // This node observing itself: `status_last_applied`/`status_is_leader` come from the same
        // `status()` snapshot the caller already took, and `local_bind` from the same body build —
        // both single reads reused here rather than asked again, so this row and the top-level body
        // cannot disagree about this node.
        return serde_json::json!({
            "node_id": id.to_string(),
            "last_applied": status_last_applied,
            "is_leader": status_is_leader,
            "reachable": true,
            "bound_ports": local_bind.bound_ports.clone(),
            "bind_failures": local_bind.bind_failures.clone(),
            "bind_status_unavailable": local_bind.bind_status_unavailable.clone(),
        });
    }
    match answered {
        // `last_applied` is read back from the peer's own body rather than recomputed: whatever
        // that node reports about itself is the answer, and this node has no second opinion to
        // offer. `BindFields::from_reply` applies the same rule to the bind fields, and
        // additionally folds a reply missing them to "unknown" rather than "nothing bound".
        Some(reply) => {
            let bind = BindFields::from_reply(Some(reply));
            serde_json::json!({
                "node_id": id.to_string(),
                "last_applied": reply.get("last_applied").cloned().unwrap_or(serde_json::Value::Null),
                "is_leader": reply.get("is_leader").cloned().unwrap_or(serde_json::Value::Null),
                "reachable": true,
                "bound_ports": bind.bound_ports,
                "bind_failures": bind.bind_failures,
                "bind_status_unavailable": bind.bind_status_unavailable,
            })
        }
        // `null`, never `0`. A voter that did not answer has an *unknown* applied index, and a
        // zero would render as "this node has applied nothing" — which is the alarm an operator
        // would act on, raised by the fan-out rather than by the fleet. Its bind fields are
        // equally unknown, via `BindFields::unknown()`.
        None => {
            let bind = BindFields::unknown();
            serde_json::json!({
                "node_id": id.to_string(),
                "last_applied": serde_json::Value::Null,
                "is_leader": serde_json::Value::Null,
                "reachable": false,
                "bound_ports": bind.bound_ports,
                "bind_failures": bind.bind_failures,
                "bind_status_unavailable": bind.bind_status_unavailable,
            })
        }
    }
}

/// Whether `/_fleet/members` fell short of a complete answer.
///
/// Widened by issue #369 beyond the original #361 rule (timeout, or an unreachable voter) with two
/// more ways a row can be reachable and still leave a gap:
///
/// - a voter that *answered* but could not read its own bind status
///   (`bind_status_unavailable: true`);
/// - a voter that answered with **no bind status at all** — a reply from a pre-#369 build, valid
///   `200` and everything, that simply omits the three bind keys. `BindFields::from_reply` folds
///   that (and the previous case) to `bound_ports: null`, so keying off `bound_ports` being `null`
///   on an otherwise-reachable row catches both: neither the old rule (`reachable`) nor the
///   `bind_status_unavailable` rule alone fires for this third case, since it is `reachable: true`
///   and `bind_status_unavailable: null` — a response that would otherwise claim to be complete
///   while carrying no bind status for that node.
///
/// In every case the row is reachable but the body is missing a fact it claims to carry, which is
/// exactly what `Rift-Cluster-Partial` exists to flag. Extracted to a free function so each rule is
/// under test on its own rather than only reachable through the timing of a real fan-out.
fn members_partial(timed_out: bool, rows: &[serde_json::Value]) -> bool {
    timed_out
        || rows.iter().any(|row| row["reachable"] == false)
        || rows
            .iter()
            .any(|row| row["reachable"] == true && row["bound_ports"].is_null())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_members() {
        assert_eq!(
            classify(&Method::GET, "/_fleet/members"),
            Some(FleetRoute::Members)
        );
    }

    #[test]
    fn classifies_health() {
        assert_eq!(
            classify(&Method::GET, "/_fleet/health"),
            Some(FleetRoute::Health)
        );
    }

    #[test]
    fn classifies_a_well_formed_op_id() {
        let id = uuid::Uuid::new_v4();
        assert_eq!(
            classify(&Method::GET, &format!("/_fleet/ops/{id}")),
            Some(FleetRoute::Op(id))
        );
    }

    #[test]
    fn a_malformed_op_id_names_no_route() {
        assert_eq!(classify(&Method::GET, "/_fleet/ops/not-a-uuid"), None);
    }

    #[test]
    fn an_unrecognized_path_names_no_route() {
        assert_eq!(classify(&Method::GET, "/_fleet/config"), None);
    }

    #[test]
    fn a_non_get_method_names_no_route() {
        assert_eq!(classify(&Method::POST, "/_fleet/members"), None);
    }

    /// A voter that did not answer has an **unknown** bind status, not a healthy one.
    ///
    /// The same reasoning `last_applied` already uses: `null`, never a value that reads as an
    /// answer. An empty `bound_ports` here would be worse than the `0` that rule was written for —
    /// the console renders "bound" from *absence*, so an empty-but-present list would make every
    /// port on an unreachable node render as an outage, and an empty `bind_failures` would make it
    /// render as healthy. Both are claims this node cannot make about a peer it never reached.
    #[test]
    fn an_unreachable_peer_reports_unknown_bind_status_and_never_a_healthy_one() {
        assert_eq!(BindFields::from_reply(None), BindFields::unknown());
    }

    /// A peer running a build from before this field existed must read as unknown, not as bound.
    ///
    /// This is the rolling-upgrade shape, and it is the one a `.unwrap_or_default()` would get
    /// silently wrong: the peer answers `200` with a perfectly valid body that simply has no bind
    /// fields in it, so nothing fails and the default would be "no failures anywhere".
    #[test]
    fn a_peer_that_omits_the_bind_fields_reports_unknown_not_bound() {
        let reply = serde_json::json!({ "node_id": "7", "last_applied": 12, "is_leader": false });
        assert_eq!(BindFields::from_reply(Some(&reply)), BindFields::unknown());
    }

    /// A peer's own report is echoed verbatim — this node has no second opinion about a peer's
    /// sockets, exactly as it has none about the peer's `last_applied`.
    #[test]
    fn a_peer_row_carries_the_peers_own_bind_report() {
        let reply = serde_json::json!({
            "node_id": "7",
            "bound_ports": [8080],
            "bind_failures": { "9090": "Address already in use" },
            "bind_status_unavailable": false,
        });

        assert_eq!(
            BindFields::from_reply(Some(&reply)),
            BindFields {
                bound_ports: serde_json::json!([8080]),
                bind_failures: serde_json::json!({ "9090": "Address already in use" }),
                bind_status_unavailable: serde_json::json!(false),
            }
        );
    }

    /// A complete answer from every voter is not partial.
    ///
    /// Carries `bound_ports`/`bind_failures` — not just `bind_status_unavailable: false` — because
    /// the widened rule below now keys off `bound_ports` being present, not merely off the old flag.
    /// A row missing `bound_ports` reads as unknown whatever `bind_status_unavailable` says, so a
    /// "complete" fixture has to actually carry it or this test would trip the new clause and fail
    /// for the wrong reason.
    #[test]
    fn a_fleet_that_answered_in_full_is_not_partial() {
        let rows = vec![serde_json::json!({
            "reachable": true,
            "bound_ports": [8080],
            "bind_failures": {},
            "bind_status_unavailable": false,
        })];
        assert!(!members_partial(false, &rows));
    }

    /// An unreachable voter makes the answer partial — the pre-existing rule, restated so the
    /// widening below cannot quietly drop it.
    #[test]
    fn an_unreachable_voter_makes_the_answer_partial() {
        let rows = vec![
            serde_json::json!({ "reachable": true, "bind_status_unavailable": false }),
            serde_json::json!({ "reachable": false, "bind_status_unavailable": null }),
        ];
        assert!(members_partial(false, &rows));
    }

    /// A voter that answered but could not read its own config also makes the answer partial.
    ///
    /// It is reachable, so the old rule would call this complete — but the body is missing a fact
    /// it claims to carry, which is precisely what `Rift-Cluster-Partial` means.
    #[test]
    fn a_voter_that_could_not_read_its_own_bind_status_makes_the_answer_partial() {
        let rows = vec![
            serde_json::json!({ "reachable": true, "bind_status_unavailable": false }),
            serde_json::json!({ "reachable": true, "bind_status_unavailable": true }),
        ];
        assert!(members_partial(false, &rows));
    }

    /// A fan-out that ran out of budget is partial whatever the rows say.
    #[test]
    fn a_timed_out_fan_out_is_partial() {
        let rows = vec![serde_json::json!({
            "reachable": true,
            "bind_status_unavailable": false,
        })];
        assert!(members_partial(true, &rows));
    }

    /// A peer that answered but published no bind status leaves a gap the header must declare.
    /// It is `reachable: true`, so the pre-#369 rule alone would call this body complete — while it
    /// carries no bind status at all for that node. This is the rolling-upgrade shape.
    #[test]
    fn a_reachable_voter_that_published_no_bind_status_makes_the_answer_partial() {
        let rows = vec![
            serde_json::json!({ "reachable": true, "bound_ports": [8080], "bind_failures": {}, "bind_status_unavailable": false }),
            serde_json::json!({ "reachable": true, "bound_ports": null, "bind_failures": null, "bind_status_unavailable": null }),
        ];
        assert!(members_partial(false, &rows));
    }

    /// The local row carries this node's own bind fields verbatim, and `reachable` is always `true`
    /// for it — this node always has an answer about itself.
    #[test]
    fn the_local_row_carries_this_nodes_own_bind_fields() {
        let local_bind = BindFields {
            bound_ports: serde_json::json!([8080, 8081]),
            bind_failures: serde_json::json!({ "9090": "Address already in use" }),
            bind_status_unavailable: serde_json::json!(false),
        };

        let row = member_row(7, true, Some(412), true, None, &local_bind);

        assert_eq!(row["reachable"], serde_json::json!(true));
        assert_eq!(row["bound_ports"], local_bind.bound_ports);
        assert_eq!(row["bind_failures"], local_bind.bind_failures);
        assert_eq!(
            row["bind_status_unavailable"],
            local_bind.bind_status_unavailable
        );
    }

    /// The whole row, asserted against a literal — the test that catches a swapped key, a dropped
    /// field, or the local state leaking into a peer's row. No wire test exercises this path: every
    /// one of them runs a solo node, so `is_me: false` was reachable by nothing before this test.
    #[test]
    fn a_peer_row_places_the_peers_bind_fields_under_the_right_keys() {
        let local_bind = BindFields::unknown();
        let answered = serde_json::json!({
            "last_applied": 12,
            "is_leader": false,
            "bound_ports": [8080],
            "bind_failures": { "9090": "Address already in use" },
            "bind_status_unavailable": false,
        });

        let row = member_row(9, false, Some(999), true, Some(&answered), &local_bind);

        assert_eq!(
            row,
            serde_json::json!({
                "node_id": "9",
                "last_applied": 12,
                "is_leader": false,
                "reachable": true,
                "bound_ports": [8080],
                "bind_failures": { "9090": "Address already in use" },
                "bind_status_unavailable": false,
            })
        );
    }

    /// A peer that never answered is unknown in every bind field, and `reachable: false`.
    #[test]
    fn a_silent_peer_row_is_unknown_in_every_bind_field() {
        let local_bind = BindFields::unknown();

        let row = member_row(9, false, Some(999), true, None, &local_bind);

        assert_eq!(row["reachable"], serde_json::json!(false));
        assert_eq!(row["bound_ports"], serde_json::Value::Null);
        assert_eq!(row["bind_failures"], serde_json::Value::Null);
        assert_eq!(row["bind_status_unavailable"], serde_json::Value::Null);
    }

    // -- issue #401: `add_peer_depth` (the `fleet_health` peer fold) --------

    /// A peer that answers with a real depth grows the total by exactly that much, and is
    /// reported as having contributed.
    #[test]
    fn a_peer_that_answers_with_a_depth_grows_the_total_by_exactly_that_depth() {
        let (total, usable) = add_peer_depth(10, Some(&serde_json::json!({ "parked_intents": 7 })));
        assert_eq!(total, 17);
        assert!(usable);
    }

    /// A peer that answers but carries no `parked_intents` field leaves the total unchanged, and
    /// is reported as not having contributed — the mutation this test exists to kill would let an
    /// answering-but-empty peer be counted as `+0` and still marked complete.
    #[test]
    fn a_peer_that_answers_with_no_parked_intents_field_leaves_the_total_unchanged_and_is_marked_unusable()
     {
        let (total, usable) = add_peer_depth(10, Some(&serde_json::json!({ "node_id": "9" })));
        assert_eq!(total, 10);
        assert!(!usable);
    }

    /// A peer that never answered at all leaves the total unchanged, and is reported as not
    /// having contributed — `None` must never be silently folded in as a `0` depth.
    #[test]
    fn an_unreachable_peer_leaves_the_total_unchanged_and_is_marked_unusable() {
        let (total, usable) = add_peer_depth(10, None);
        assert_eq!(total, 10);
        assert!(!usable);
    }
}
