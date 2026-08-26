//! The `/_cluster/*` operator surface (RFC-001 §11.1).
//!
//! These endpoints ride the cluster port rather than the admin API: they answer
//! questions about the fleet, they must be authenticated with the cluster
//! credential rather than the admin API key, and the cluster port already has
//! exactly that transport. Every answer comes from *this node's* applied state,
//! so comparing two nodes' answers is what tells an operator whether the fleet
//! has converged.

use std::sync::{Arc, OnceLock, Weak};

use rift_cluster::rpc::{HandlerFuture, RpcError};
use rift_cluster::{RaftNode, Router};

use crate::readiness::Readiness;
use crate::route_hits::{CLUSTER_ROUTE_HITS_PATH, RouteHitCounter};

/// Binding an already-bound [`NodeSlot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the cluster operator surface was bound to a node twice")]
pub struct NodeSlotAlreadyBound;

/// A late-filled handle to the node the endpoints report on.
///
/// The node cannot exist when its routes are built — the routes are needed to
/// bind the cluster port, whose address the node then advertises — so the
/// handlers read it through this slot, which the composition fills in
/// immediately after the node is constructed. The cluster port is already
/// accepting by then, so a request landing in that window is answered with a
/// clean "not available yet" rather than being served stale.
///
/// The handle is [`Weak`] on purpose. The node owns the task that serves these
/// very handlers, so a strong reference here would be a cycle: the node would
/// keep itself alive, its `Drop` (which aborts the accept loop and releases the
/// cluster port and the redb lock) would never run, and every startup path that
/// bails out after the node exists would leak a live node for the life of the
/// process.
#[derive(Clone, Default)]
pub struct NodeSlot(Arc<OnceLock<Weak<RaftNode>>>);

impl NodeSlot {
    /// Publish the node to the handlers.
    ///
    /// Binding twice is a composition bug — the second node would be invisible
    /// to the whole operator surface — so it is returned rather than logged,
    /// leaving the caller no way to continue as if it had worked.
    pub fn set(&self, node: &Arc<RaftNode>) -> Result<(), NodeSlotAlreadyBound> {
        self.0
            .set(Arc::downgrade(node))
            .map_err(|_| NodeSlotAlreadyBound)
    }

    fn node(&self) -> Result<Arc<RaftNode>, RpcError> {
        self.0
            .get()
            .ok_or_else(|| RpcError::Handler("cluster node is not available yet".to_owned()))?
            .upgrade()
            .ok_or_else(|| RpcError::Handler("cluster node is shutting down".to_owned()))
    }
}

/// Register the operator endpoints onto `base`.
///
/// `route_hits` is passed directly rather than through a [`NodeSlot`] because, unlike the node, it
/// exists before the router is built — it is handed to the front-door listener at the same moment.
///
/// `front_door` is this node's own listener state (issue #403), passed as a plain `bool` for the
/// same reason and resolved by the caller so that one derivation of the fact serves every consumer.
#[must_use]
pub fn routes(
    base: Router,
    slot: NodeSlot,
    readiness: Arc<Readiness>,
    route_hits: Arc<RouteHitCounter>,
    front_door: bool,
) -> Router {
    let members = slot.clone();
    let config = slot.clone();
    let imposters = slot.clone();
    let ops = slot.clone();
    let health = slot;

    base.route_prefix(
        "GET",
        "/_cluster/ops/",
        Arc::new(move |suffix: String, _body: Vec<u8>| -> HandlerFuture {
            let slot = ops.clone();
            Box::pin(async move {
                let node = slot.node()?;
                // The suffix may carry a query string; the id is the path part.
                let id = suffix.split('?').next().unwrap_or_default();
                // A malformed id names no op the same way an unknown one
                // does: 404, never the internal-failure bucket.
                let Ok(op_id) = id.parse::<uuid::Uuid>() else {
                    return Err(RpcError::UnknownRoute {
                        method: "GET".to_owned(),
                        path: format!("/_cluster/ops/{id}"),
                    });
                };
                match op_body(&node, &op_id)? {
                    Some(value) => serde_json::to_vec(&value).map_err(handler_error),
                    // Unknown ids and ops whose dedup window has lapsed are
                    // indistinguishable; both answer 404.
                    None => Err(RpcError::UnknownRoute {
                        method: "GET".to_owned(),
                        path: format!("/_cluster/ops/{id}"),
                    }),
                }
            })
        }),
    )
    .route(
        "GET",
        "/_cluster/members",
        json_handler(move || Ok(members_body(members.node()?.as_ref()))),
    )
    .route(
        "GET",
        "/_cluster/config",
        json_handler(move || {
            let node = config.node()?;
            // issue #182: resource reads went tenant-aware, and `configured_ports`
            // now answers fleet-wide — `(tenant, port)` per row instead of a bare
            // port list. This endpoint is deliberately fleet-wide (design section
            // E): it is what an operator diffs across nodes to see whether the
            // fleet has converged, and "converged" now has to mean "on the same
            // tenant's config at that port", not just "the same port". Emitting
            // `{tenant, port}` rows keeps that honest instead of flattening the
            // tenant back out.
            let ports: Vec<serde_json::Value> = node
                .configured_ports()
                .map_err(handler_error)?
                .into_iter()
                .map(|(tenant, port)| {
                    serde_json::json!({
                        "tenant": tenant,
                        "port": port,
                    })
                })
                .collect();
            // Provenance (issue #134): which imposters a source owns, and at
            // which version. Reported here rather than only under
            // `/admin/sources` because this is the endpoint an operator
            // compares across nodes to see whether the fleet has converged —
            // and "converged on the same configs from the same source version"
            // is the question a source-driven fleet actually asks. Tenant-qualified
            // for the same reason `ports` above is (issue #182).
            let provenance: Vec<serde_json::Value> = node
                .config_provenance()
                .map_err(handler_error)?
                .into_iter()
                .map(|(tenant, port, source)| {
                    serde_json::json!({
                        "tenant": tenant,
                        "port": port,
                        "sourceId": source.id,
                        "version": source.version,
                    })
                })
                .collect();
            Ok(serde_json::json!({
                "ports": ports,
                "provenance": provenance,
                "last_applied": node.status().last_applied,
            }))
        }),
    )
    .route(
        "GET",
        "/_cluster/imposters",
        json_handler(move || {
            let node = imposters.node()?;
            // issue #182: `configured_ports` now hands back `(tenant, port)` pairs
            // fleet-wide, so the owning tenant for each port is already in hand —
            // no need for a second `owning_tenant` lookup (and its documented
            // non-O(1) cost) per port.
            let ports = node.configured_ports().map_err(handler_error)?;
            let mut reported = Vec::with_capacity(ports.len());
            for (tenant, port) in ports {
                let body = node
                    .get_imposter(tenant.as_str(), port)
                    .map_err(handler_error)?;
                // A committed body that will not parse is corruption, not an
                // absent imposter: report it as such rather than hiding the port.
                let config = match body {
                    Some(body) => {
                        serde_json::from_str::<serde_json::Value>(&body).unwrap_or_else(|e| {
                            // Surfacing it in the response only helps whoever
                            // happens to poll; corruption of committed state has
                            // to reach the logs too.
                            tracing::error!(
                                port,
                                error = %e,
                                "committed imposter config will not parse"
                            );
                            serde_json::json!({ "error": e.to_string() })
                        })
                    }
                    None => serde_json::Value::Null,
                };
                // `bind_failure` is what makes this endpoint the per-`(port, node)` view RFC-001
                // §7.4.6 promises. Bind status is a node-local observation — it cannot ride the
                // deterministic raft apply — so it is reported by each node about itself, here and
                // in `rift_cluster_bind_failures`, rather than replicated. Absent (`null`) is the
                // healthy answer; present means the node holds the config and serves the imposter
                // in-process but never bound its port.
                //
                // Sourced from `bind_failure`, not the general `apply_failures` map, so a parse or
                // stub-patch failure is never mislabelled as bind divergence — see
                // `RedbStateMachine::bind_failure`.
                reported.push(serde_json::json!({
                    "tenant": tenant,
                    "port": port,
                    "config": config,
                    "bind_failure": node.bind_failure(port),
                }));
            }
            Ok(serde_json::json!({ "imposters": reported }))
        }),
    )
    .route(
        "GET",
        "/_cluster/health",
        json_handler(move || Ok(health_body(health.node()?.as_ref(), &readiness))),
    )
    // Issue #368. No `NodeSlot`: this answers from a counter, not from committed state, so it is
    // servable from the moment the port opens and has nothing to wait for the node to provide.
    .route(
        "GET",
        CLUSTER_ROUTE_HITS_PATH,
        json_handler(move || Ok(route_hits.body(front_door))),
    )
}

/// Build the `/_cluster/members` body: this node's view of raft membership.
///
/// Shared with the `/_fleet/members` admin-port projection ([`crate::fleet`],
/// RFC-006 §5.2) so the two ports answer from one code path rather than two
/// implementations that a test has to keep honest — one implementation
/// called twice cannot diverge.
/// A raft node id, rendered as a **string**.
///
/// A `NodeId` is a `u64`. JSON numbers are IEEE-754 doubles wherever the reader is JavaScript, so
/// every id above 2^53-1 is silently rounded on the way in: the console read `3342140982834931156`
/// off the wire and displayed `3342140982834931000`. Not a formatting problem — a different node,
/// presented as a complete answer, on the one screen whose job is to say which nodes are in the
/// fleet. An operator pasting that into `curl` addresses nothing.
///
/// Ids are identifiers, never magnitudes: nothing sums or orders them, so a string costs nothing
/// and is the only encoding that survives the round trip. `last_applied` and `m_idx` stay numbers
/// deliberately — they ARE magnitudes, and a log index reaching 2^53 is not a reachable state.
fn node_id(id: impl std::fmt::Display) -> serde_json::Value {
    serde_json::Value::String(id.to_string())
}

pub(crate) fn members_body(node: &RaftNode) -> serde_json::Value {
    let status = node.status();
    // `fleet_name()` is the one read here that can fail, and it is `Result<Option<String>, _>` —
    // which is why it cannot use `.ok()` the way `parked_intents` does a few lines below. That
    // read is `Result<u64, _>`: it has no legitimate `None`, so a `null` there unambiguously
    // means "could not be read". Here `Ok(None)` is the *ordinary* answer — every fleet starts
    // unnamed — so folding the error into `None` too would put "storage could not answer" and
    // "nobody has named this fleet" on the same wire byte, and the console would render both as
    // `Unnamed`. That is the wrong-but-quiet shape the error rules exist to prevent.
    //
    // The endpoint deliberately still answers: `status()` is read from in-memory raft metrics, so
    // everything else in this body is trustworthy even when redb is not, and an operator
    // debugging that failure needs the membership view rather than a 500. The ambiguity is closed
    // by reporting the failure as its own field instead (issue #373).
    let (fleet_name, fleet_name_unavailable) = match node.fleet_name() {
        Ok(name) => (name, false),
        Err(e) => {
            tracing::error!(error = %e, "fleet members body: fleet name could not be read");
            (None, true)
        }
    };
    let bind_fields = local_bind_state(node).fields();
    serde_json::json!({
        "node_id": node_id(status.node_id),
        "is_leader": status.is_leader,
        // `null` stays `null`: "this node knows of no leader" is an absence, and rendering it as
        // the string "0" would name node 0 as the leader.
        "current_leader": status.current_leader.map(node_id),
        "last_applied": status.last_applied,
        "voters": status.voters.iter().map(node_id).collect::<Vec<_>>(),
        "fleet_name": fleet_name,
        "fleet_name_unavailable": fleet_name_unavailable,
        // Issue #369: what this node knows about its own listeners. See `BindFields` and
        // `LocalBindState` for the shape and why it is a positive list, not just a failure map.
        "bound_ports": bind_fields.bound_ports,
        "bind_failures": bind_fields.bind_failures,
        "bind_status_unavailable": bind_fields.bind_status_unavailable,
    })
}

/// The three bind fields a `members` row carries (issue #369): what a node knows about its own
/// listeners, or what a caller could learn about a peer's.
///
/// One struct rather than three loose values because the three only ever travel together — every
/// producer below (`LocalBindState::fields`, `BindFields::unknown`, `BindFields::from_reply`) sets
/// all three at once, and a caller that forgot one would leave a row with two fresh facts and one
/// stale `null`.
#[derive(Debug, PartialEq)]
pub(crate) struct BindFields {
    pub(crate) bound_ports: serde_json::Value,
    pub(crate) bind_failures: serde_json::Value,
    pub(crate) bind_status_unavailable: serde_json::Value,
}

impl BindFields {
    /// All three `null`. Used when nothing at all is known: an unreachable peer, or a peer reply
    /// missing the fields.
    ///
    /// Not `{ bound_ports: [], bind_failures: {} }` — an empty `bound_ports` is the claim "this
    /// node holds no ports", and an empty `bind_failures` is the claim "nothing has failed". Both
    /// are answers this node has no basis to give about a peer it never heard from; `null` is the
    /// only encoding that says "no answer" rather than "a reassuring answer".
    pub(crate) fn unknown() -> BindFields {
        BindFields {
            bound_ports: serde_json::Value::Null,
            bind_failures: serde_json::Value::Null,
            bind_status_unavailable: serde_json::Value::Null,
        }
    }

    /// Read the three bind fields back off a peer's own `/_cluster/members` reply, exactly as
    /// `merged_members` already does for `last_applied`: a peer's own report is echoed verbatim,
    /// never recomputed here, because this node has no second opinion about a peer's sockets.
    ///
    /// `None`, or a reply missing *any* of the three keys, both fold to [`Self::unknown`] rather
    /// than `.unwrap_or_default()`-ing the missing ones in isolation. A reply from a node running a
    /// pre-#369 build omits all three keys and is otherwise a perfectly valid `200` — the exact
    /// shape `fleet_name_unavailable` (#373) already had to account for — so treating a partial
    /// read as "the missing fields are empty" would render a peer that has not upgraded yet as a
    /// peer with nothing bound.
    pub(crate) fn from_reply(reply: Option<&serde_json::Value>) -> BindFields {
        let Some(reply) = reply else {
            return BindFields::unknown();
        };
        let (Some(bound_ports), Some(bind_failures), Some(bind_status_unavailable)) = (
            reply.get("bound_ports"),
            reply.get("bind_failures"),
            reply.get("bind_status_unavailable"),
        ) else {
            return BindFields::unknown();
        };
        BindFields {
            bound_ports: bound_ports.clone(),
            bind_failures: bind_failures.clone(),
            bind_status_unavailable: bind_status_unavailable.clone(),
        }
    }
}

/// What a node can say about its own listeners (issue #369).
#[derive(Debug)]
pub(crate) enum LocalBindState {
    /// This node walked its engine's own imposter set and checked each one's socket.
    Observed {
        bound_ports: Vec<u16>,
        failures: std::collections::BTreeMap<u16, String>,
    },
    /// No local engine ([`RaftNode::local_bind_report`] returned `None`), so this node knows
    /// nothing about any port — not even that a given port is unbound, since it has no engine to
    /// check against.
    Unavailable,
}

impl LocalBindState {
    /// Render as the three wire fields.
    pub(crate) fn fields(&self) -> BindFields {
        match self {
            LocalBindState::Observed {
                bound_ports,
                failures,
            } => BindFields {
                bound_ports: serde_json::Value::Array(
                    bound_ports
                        .iter()
                        .copied()
                        .map(serde_json::Value::from)
                        .collect(),
                ),
                bind_failures: serde_json::Value::Object(
                    failures
                        .iter()
                        .map(|(port, reason)| {
                            (port.to_string(), serde_json::Value::String(reason.clone()))
                        })
                        .collect(),
                ),
                bind_status_unavailable: serde_json::Value::Bool(false),
            },
            // `null`, never `[]`/`{}`: see `BindFields::unknown` for why an empty answer would be
            // the wrong-but-quiet one. This differs from `unknown()` only in the third field —
            // `bind_status_unavailable` is `true` here because *this* node is the one with nothing
            // to report (no local engine, so no bind observations exist) and can say so, where
            // `unknown()` is for a peer this node cannot even ask.
            LocalBindState::Unavailable => BindFields {
                bound_ports: serde_json::Value::Null,
                bind_failures: serde_json::Value::Null,
                bind_status_unavailable: serde_json::Value::Bool(true),
            },
        }
    }
}

/// What `node` knows about the ports its own engine holds (issue #369, RFC-001 §7.4.6).
///
/// `bound_ports` is a **positive** list, not just "every configured port minus the failures" —
/// `bind_failure(p).is_none()` is equally true for a port this node has never applied at all, and
/// [`RaftNode::is_locally_bound`]'s own doc comment warns against exactly that inference. Without
/// the positive list, a peer that never received the imposter (or has no local engine at all) would
/// have no failure recorded for it and would render as bound.
///
/// A port that is neither locally bound nor recorded as a bind failure is in **neither** collection
/// — that means "not applied on this node", and is deliberately distinct from both "bound" and
/// "failed to bind".
///
/// Built from [`RaftNode::local_bind_report`] — an in-memory pass over the engine's own imposter
/// set — rather than from [`RaftNode::configured_ports`] (blocker B4). `configured_ports` opens a
/// redb read transaction that scans the fleet-wide `SM_CONFIGS` table, which made every 5-second
/// `/_fleet/members` poll from every open console tab, fanned out to every peer, an O(all imposters
/// in the fleet) table scan; `local_bind_report` answers from memory in one pass over the ports
/// this node's engine actually holds. `Unavailable` therefore now means "no local engine" — never
/// "the config read failed", since there is no config read here to fail.
pub(crate) fn local_bind_state(node: &RaftNode) -> LocalBindState {
    match node.local_bind_report() {
        Some((bound_ports, failures)) => LocalBindState::Observed {
            bound_ports,
            failures,
        },
        None => LocalBindState::Unavailable,
    }
}

/// Build the `/_cluster/health` body: readiness plus this node's ring view.
///
/// Shared with `/_fleet/health` for the same reason as [`members_body`].
pub(crate) fn health_body(node: &RaftNode, readiness: &Readiness) -> serde_json::Value {
    let ring = node.ring();
    serde_json::json!({
        "ready": readiness.state().is_ready(),
        "state": readiness.state().as_str(),
        "pending_gates": readiness.pending(),
        "isolated": node.is_isolated(),
        // This node's own parked-write backlog (issue #360): writes it accepted under
        // `--cluster-admin-async` and has not replayed. A magnitude, so a number.
        //
        // `null` on a storage error, never `0`. A zero is the reassuring answer — "nothing
        // outstanding" — and it is the one an operator would act on by not acting; reporting it
        // because redb could not be read is exactly the wrong-but-quiet failure the error rules
        // exist to prevent. The read itself must not fail the health probe, which is what
        // `is_ready` is for.
        "parked_intents": node
            .parked_intent_count()
            .inspect_err(|e| tracing::warn!(error = %e, "health serves without a parked-intent depth"))
            .ok(),
        // #439, D-48: non-null while this node's apply is parked on a sideloaded blob no member
        // can supply. Always present on this build, so a peer that *omits* the key is an old
        // build, not a healthy one — the fleet roll-up in `fleet.rs` relies on that. Degraded,
        // not not-ready: `ready` and `state` above are untouched, because pulling the node from
        // the load balancer would only widen whatever partition caused the stall.
        "blob_fetch_stall": node.blob_fetch_stall().map(|stall| serde_json::json!({
            "digest": stall.digest,
            "stalled_for_secs": stall.stalled_for().as_secs(),
            "origin": node_id(stall.origin),
            "tried": stall.tried.iter().map(node_id).collect::<Vec<_>>(),
            "skewed": stall.skewed.iter().map(node_id).collect::<Vec<_>>(),
            "last_error": stall.last_error,
        })),
        "ring": {
            // `m_idx` is an epoch counter — a magnitude, and small. The members are ids; see
            // `node_id` for why those are strings.
            "m_idx": ring.m_idx(),
            "members": ring.members().iter().map(node_id).collect::<Vec<_>>(),
        },
    })
}

/// Build the `/_cluster/ops/:id` body for a well-formed op id.
///
/// `Ok(None)` means the id names no op — unknown ids and ops whose dedup
/// window has lapsed are indistinguishable, and both are the caller's 404.
/// Shared with `/_fleet/ops/:id` for the same reason as [`members_body`].
pub(crate) fn op_body(
    node: &RaftNode,
    op_id: &uuid::Uuid,
) -> Result<Option<serde_json::Value>, RpcError> {
    let value = match node.read_op(op_id).map_err(handler_error)? {
        Some(response) => match response.outcome {
            rift_cluster::ControlOutcome::Applied => serde_json::json!({
                "state": "applied",
                "revision": response.revision,
            }),
            rift_cluster::ControlOutcome::Failed { reason } => serde_json::json!({
                "state": "failed",
                "revision": response.revision,
                "detail": reason,
            }),
        },
        None if node.intent_parked(op_id).map_err(handler_error)? => {
            serde_json::json!({ "state": "pending" })
        }
        None => return Ok(None),
    };
    Ok(Some(value))
}

/// Report a node-side failure as a handler error. The node's own error types
/// are already operator-readable, so the transport carries the message rather
/// than re-classifying it.
fn handler_error(e: impl std::fmt::Display) -> RpcError {
    RpcError::Handler(e.to_string())
}

/// Wrap a synchronous, body-less reporter into a cluster-port handler.
fn json_handler<F>(report: F) -> Arc<dyn rift_cluster::rpc::Handler>
where
    F: Fn() -> Result<serde_json::Value, RpcError> + Send + Sync + 'static,
{
    Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
        let reported = report().and_then(|value| serde_json::to_vec(&value).map_err(handler_error));
        Box::pin(async move { reported })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{BindFields, LocalBindState, node_id};

    /// The healthy shape, asserted against literal JSON rather than against the constructor's own
    /// output — a round trip through `LocalBindState` would pass for any encoding at all.
    #[test]
    fn an_observed_node_lists_the_ports_it_holds_and_the_ports_it_could_not_bind() {
        let state = LocalBindState::Observed {
            bound_ports: vec![8080, 8081],
            failures: BTreeMap::from([(9090, "Address already in use".to_owned())]),
        };

        assert_eq!(
            state.fields(),
            BindFields {
                bound_ports: serde_json::json!([8080, 8081]),
                bind_failures: serde_json::json!({ "9090": "Address already in use" }),
                bind_status_unavailable: serde_json::json!(false),
            }
        );
    }

    /// A node with no local engine cannot observe binds at all, and must say so.
    ///
    /// `null`, never `[]`/`{}`: an empty list is the claim "I looked and every port is fine", which
    /// is exactly the wrong-but-quiet answer — the console would render a confident green row for a
    /// node that answered nothing. Follows `fleet_name_unavailable` (#373) rather than `.ok()`.
    ///
    /// Note the trigger, because it is not a runtime failure: `local_bind_report` reads the
    /// engine's in-memory imposter set and touches no storage, so there is no config read left to
    /// fail. `Unavailable` is reached only by a node that has no engine to ask — a static property
    /// of that node's role, which is why nothing logs an error on this path.
    #[test]
    fn a_node_with_no_local_engine_reports_null_not_an_empty_healthy_answer() {
        assert_eq!(
            LocalBindState::Unavailable.fields(),
            BindFields {
                bound_ports: serde_json::Value::Null,
                bind_failures: serde_json::Value::Null,
                bind_status_unavailable: serde_json::json!(true),
            }
        );
    }

    // The "a port is never both bound and failed" invariant used to have a unit test here that
    // hand-built two disjoint `Vec`/`BTreeMap` literals and asserted them disjoint — never calling
    // `local_bind_state`, the function whose invariant it claimed to name, so it could not have
    // caught a regression in it. The real check lives in the wire test
    // `fleet_members_reports_the_port_this_node_could_not_bind`
    // (`crates/rift-cluster-server/tests/bind_divergence.rs`), which asserts the failed port is
    // absent from `bound_ports` against a live bind failure — a literal-constructed unit test
    // cannot pin this, because the invariant depends on `Imposter::is_bound()` and the engine's own
    // state, not on anything expressible as two hand-picked collections.

    /// The encoding property the whole change rests on: a `u64` id survives verbatim.
    ///
    /// `3342140982834931156` is the id that exposed this. It is not representable as an IEEE-754
    /// double, so a client reading it as a JSON number gets `3342140982834931000` — a different
    /// node, and one that does not exist. The console displayed exactly that.
    #[test]
    fn a_u64_id_beyond_two_to_the_53_survives_as_an_exact_string() {
        assert_eq!(
            node_id(3_342_140_982_834_931_156_u64),
            serde_json::json!("3342140982834931156")
        );

        // The first integer a double cannot hold, so the test names the boundary rather than
        // implying the problem starts somewhere vague.
        assert_eq!(
            node_id(9_007_199_254_740_993_u64),
            serde_json::json!("9007199254740993")
        );
    }

    /// The half a "renders as a string" assertion alone would miss: the string has to parse BACK.
    #[test]
    fn the_rendered_string_round_trips_to_the_same_u64() {
        for id in [
            0_u64,
            1,
            9_007_199_254_740_993,
            3_342_140_982_834_931_156,
            u64::MAX,
        ] {
            let rendered = node_id(id);
            let text = rendered
                .as_str()
                .expect("a node id renders as a JSON string");
            assert_eq!(text.parse::<u64>().expect("and parses back"), id);
        }
    }
}
