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

use crate::cluster_api::{health_body, members_body, op_body};
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
                    match reply
                        .get("parked_intents")
                        .and_then(serde_json::Value::as_u64)
                    {
                        Some(depth) => total = total.saturating_add(depth),
                        // The peer answered but could not read its own depth. Its contribution is
                        // unknown, which makes the sum a floor exactly as an unreachable peer does.
                        None => {
                            unanswered += 1;
                            tracing::warn!(peer, "health fan-out: peer reported no parked depth");
                        }
                    }
                }
                Ok((peer, Err(e))) => {
                    unanswered += 1;
                    tracing::warn!(peer, error = %e, "health fan-out: peer did not answer");
                }
                Err(e) => {
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
            if id == me {
                return serde_json::json!({
                    "node_id": id.to_string(),
                    "last_applied": status.last_applied,
                    "is_leader": status.is_leader,
                    "reachable": true,
                });
            }
            match answered.get(&id) {
                // `last_applied` is read back from the peer's own body rather than recomputed:
                // whatever that node reports about itself is the answer, and this node has no
                // second opinion to offer.
                Some(reply) => serde_json::json!({
                    "node_id": id.to_string(),
                    "last_applied": reply.get("last_applied").cloned().unwrap_or(serde_json::Value::Null),
                    "is_leader": reply.get("is_leader").cloned().unwrap_or(serde_json::Value::Null),
                    "reachable": true,
                }),
                // `null`, never `0`. A voter that did not answer has an *unknown* applied index,
                // and a zero would render as "this node has applied nothing" — which is the alarm
                // an operator would act on, raised by the fan-out rather than by the fleet.
                None => serde_json::json!({
                    "node_id": id.to_string(),
                    "last_applied": serde_json::Value::Null,
                    "is_leader": serde_json::Value::Null,
                    "reachable": false,
                }),
            }
        })
        .collect();

    let partial = timed_out || rows.iter().any(|row| row["reachable"] == false);
    if let Some(map) = value.as_object_mut() {
        map.insert("members".to_owned(), serde_json::Value::Array(rows));
    }
    FleetBody { value, partial }
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
}
