//! The control-plane op set (ADR-001 §4.1): what an admin mutation becomes in the
//! Raft log, and the deterministic pure logic the state machine runs before it
//! mutates anything.
//!
//! Everything here must be deterministic across nodes: the same committed
//! [`ControlRequest`] against the same state-machine state yields the same
//! [`ControlResponse`] and the same table mutation on every replica. Anything
//! that can differ per node (port binds, listener state) lives in the engine
//! drive *after* apply, never here.

use rift_ee::seams::{ImposterConfig, Stub};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Tenant scope of a control op. Fixed to `"default"` until RFC-002 (#17) —
/// [`validate`] rejects anything else, but the field is in the log format now so
/// multi-tenancy does not need a wire break.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

/// The only tenant that exists until RFC-002 (#17).
pub const DEFAULT_TENANT: &str = "default";

impl TenantId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        self.0 == DEFAULT_TENANT
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self(DEFAULT_TENANT.to_owned())
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The envelope every log entry carries: the op plus the identity needed for
/// dedup (`op_id`, from the client's `Idempotency-Key` or minted by the
/// accepting node) and audit (`principal`, populated once RFC-002 lands).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlRequest {
    pub op_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// Wall-clock seconds at the minting node when the op was accepted. This is
    /// the state machine's *only* time source: dedup TTL and GC run against the
    /// maximum `issued_at_secs` the log has carried (a replicated logical
    /// clock), never against a replica's local clock — local clocks would let
    /// replicas disagree about which dedup entries have expired, and a replay
    /// landing near the boundary would then re-apply on one replica and
    /// collapse on another, diverging their applied state.
    #[serde(default)]
    pub issued_at_secs: u64,
    /// Apply only if the addressed record's stored revision equals this;
    /// `None` = unconditional (last-writer-wins, the pre-#46 behavior).
    ///
    /// Mixed-version caveat: a replica running a pre-#46 binary ignores this
    /// field and applies unconditionally, so operators must not send
    /// `If-Match` until every node runs an upgraded binary — the feature is
    /// inert until a client opts in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub op: ControlOp,
}

/// Application-level operation carried by the Raft log (ADR-001 §4.1).
///
/// The reserved variants exist so the log format is stable before RFC-002
/// (#17) defines their payloads: their tag is fixed now, their body is opaque
/// JSON, and [`validate`] rejects them until the features land.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlOp {
    PutImposter {
        tenant: TenantId,
        // Boxed: an inline `ImposterConfig` would make every op as large as the
        // biggest one (clippy::large_enum_variant); serde is transparent to it.
        config: Box<ImposterConfig>,
    },
    PatchStubs {
        tenant: TenantId,
        port: u16,
        edit: StubEditScript,
    },
    DeleteImposter {
        tenant: TenantId,
        port: u16,
    },
    DeleteAll {
        tenant: TenantId,
    },
    /// Pause/resume serving on a port, applied in place — never a wholesale
    /// replace (upstream #817 semantics; enterprise #15).
    SetEnabled {
        tenant: TenantId,
        port: u16,
        enabled: bool,
    },
    TenantPut {
        body: serde_json::Value,
    },
    TenantDelete {
        body: serde_json::Value,
    },
    PrincipalPut {
        body: serde_json::Value,
    },
    PrincipalDelete {
        body: serde_json::Value,
    },
    BindingPut {
        body: serde_json::Value,
    },
    BindingDelete {
        body: serde_json::Value,
    },
}

/// An ordered sequence of stub edits, applied atomically to one imposter's stub
/// list — the order-aware #316 semantics, mirroring
/// `ImposterManager::{add_stub, replace_stub_by_id, delete_stub_by_id, move_stub}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StubEditScript(pub Vec<StubEdit>);

/// One step of a [`StubEditScript`]. By-id steps address explicit stub ids only
/// (the upstream #202 contract); positional steps use current indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StubEdit {
    Add {
        stub: Stub,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
    },
    ReplaceById {
        id: String,
        stub: Stub,
    },
    DeleteById {
        id: String,
    },
    Move {
        from: usize,
        to: usize,
    },
}

/// How applying a [`ControlOp`] turned out — deterministic on every replica.
/// `Failed` is a *committed* outcome: the op is in the log and deduped like any
/// other, it just changed nothing (validation refused it identically everywhere).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOutcome {
    Applied,
    Failed { reason: String },
}

/// Application-level response returned from applying a [`ControlRequest`].
/// `revision` is the applying log index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlResponse {
    pub revision: u64,
    pub outcome: ControlOutcome,
}

impl ControlResponse {
    #[must_use]
    pub fn applied(revision: u64) -> Self {
        Self {
            revision,
            outcome: ControlOutcome::Applied,
        }
    }

    #[must_use]
    pub fn failed(revision: u64, reason: impl Into<String>) -> Self {
        Self {
            revision,
            outcome: ControlOutcome::Failed {
                reason: reason.into(),
            },
        }
    }
}

/// Deterministic pre-apply validation: everything that must hold before the
/// state machine mutates its tables. Mirrors the checks of upstream's private
/// `ImposterManager::validate_config_set` for the ops it covers (protocol,
/// duplicate explicit stub ids), plus the cluster-only rules: an explicit port
/// (auto-assign cannot replicate — every node would pick a different port),
/// the single-tenant gate, and the not-yet-implemented variants.
///
/// `Err` carries the reason recorded in the `Failed` outcome. It must depend
/// only on the op itself, never on per-node state.
pub fn validate(op: &ControlOp) -> Result<(), String> {
    match op {
        ControlOp::PutImposter { tenant, config } => {
            require_default_tenant(tenant)?;
            if config.port.is_none() {
                return Err(
                    "config must carry an explicit port: an auto-assigned port cannot replicate"
                        .to_owned(),
                );
            }
            match config.protocol.as_str() {
                "http" | "https" => {}
                other => return Err(format!("unsupported protocol {other:?}")),
            }
            let mut ids = std::collections::HashSet::new();
            for stub in &config.stubs {
                if let Some(id) = stub.id.as_deref()
                    && !ids.insert(id)
                {
                    return Err(format!("duplicate stub id {id:?}"));
                }
            }
            Ok(())
        }
        ControlOp::PatchStubs { tenant, .. } | ControlOp::DeleteImposter { tenant, .. } => {
            require_default_tenant(tenant)
        }
        ControlOp::DeleteAll { tenant } => require_default_tenant(tenant),
        ControlOp::SetEnabled { tenant, .. } => require_default_tenant(tenant),
        ControlOp::TenantPut { .. }
        | ControlOp::TenantDelete { .. }
        | ControlOp::PrincipalPut { .. }
        | ControlOp::PrincipalDelete { .. }
        | ControlOp::BindingPut { .. }
        | ControlOp::BindingDelete { .. } => {
            Err("reserved op: multi-tenancy and RBAC arrive with RFC-002 (#17)".to_owned())
        }
    }
}

fn require_default_tenant(tenant: &TenantId) -> Result<(), String> {
    if tenant.is_default() {
        Ok(())
    } else {
        Err(format!(
            "unknown tenant {:?}: multi-tenancy arrives with RFC-002 (#17)",
            tenant.as_str()
        ))
    }
}

/// The single-imposter `(tenant, port)` record `op` addresses, or `None` if
/// `op` has no such target (a bulk op, or a reserved RFC-002 variant). Used by
/// the state machine's expected-revision check (#46): a precondition can only
/// ever hold against one stored record, so every op without a single target
/// refuses a precondition deterministically rather than silently ignoring it.
pub(crate) fn precondition_target(op: &ControlOp) -> Option<(&TenantId, u16)> {
    match op {
        // `config.port` is validated to be present before this ever matters,
        // but a `None` here must still yield `None`, not a bogus target.
        ControlOp::PutImposter { tenant, config } => config.port.map(|port| (tenant, port)),
        ControlOp::PatchStubs { tenant, port, .. }
        | ControlOp::DeleteImposter { tenant, port }
        | ControlOp::SetEnabled { tenant, port, .. } => Some((tenant, *port)),
        ControlOp::DeleteAll { .. }
        | ControlOp::TenantPut { .. }
        | ControlOp::TenantDelete { .. }
        | ControlOp::PrincipalPut { .. }
        | ControlOp::PrincipalDelete { .. }
        | ControlOp::BindingPut { .. }
        | ControlOp::BindingDelete { .. } => None,
    }
}

/// Apply `script` to `stubs` deterministically, mirroring the upstream stub
/// lifecycle semantics exactly: `Add` rejects a duplicate explicit id and
/// clamps `index` to the list length; `ReplaceById` keeps the slot's position
/// and forces the replacement's id to the addressed id; `DeleteById` removes
/// the addressed stub; `Move` bounds-checks both ends and carries the stub.
///
/// Any failing step fails the whole script and leaves `stubs` untouched, so a
/// committed `PatchStubs` is all-or-nothing — partial application would diverge
/// replicas from the stored config.
pub(crate) fn apply_edit(stubs: &mut Vec<Stub>, script: &StubEditScript) -> Result<(), String> {
    // Clone-for-atomicity: steps mutate a scratch copy, written back only when
    // every step succeeded.
    let mut next = stubs.clone();
    for step in &script.0 {
        match step {
            StubEdit::Add { stub, index } => {
                if let Some(id) = stub.id.as_deref()
                    && next.iter().any(|s| s.id.as_deref() == Some(id))
                {
                    return Err(format!("add: duplicate stub id {id:?}"));
                }
                let at = index.unwrap_or(next.len()).min(next.len());
                next.insert(at, stub.clone());
            }
            StubEdit::ReplaceById { id, stub } => {
                let Some(i) = next
                    .iter()
                    .position(|s| s.id.as_deref() == Some(id.as_str()))
                else {
                    return Err(format!("replace: no stub with id {id:?}"));
                };
                let mut stub = stub.clone();
                stub.id = Some(id.clone());
                next[i] = stub;
            }
            StubEdit::DeleteById { id } => {
                let Some(i) = next
                    .iter()
                    .position(|s| s.id.as_deref() == Some(id.as_str()))
                else {
                    return Err(format!("delete: no stub with id {id:?}"));
                };
                next.remove(i);
            }
            StubEdit::Move { from, to } => {
                let len = next.len();
                if *from >= len {
                    return Err(format!("move: index {from} out of bounds (len {len})"));
                }
                if *to >= len {
                    return Err(format!("move: index {to} out of bounds (len {len})"));
                }
                let stub = next.remove(*from);
                next.insert(*to, stub);
            }
        }
    }
    *stubs = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn config(port: u16) -> Box<ImposterConfig> {
        serde_json::from_value(json!({ "port": port, "protocol": "http" }))
            .expect("minimal config parses")
    }

    fn stub(id: Option<&str>) -> Stub {
        let mut v = json!({});
        if let Some(id) = id {
            v = json!({ "id": id });
        }
        serde_json::from_value(v).expect("minimal stub parses")
    }

    fn stub_ids(stubs: &[Stub]) -> Vec<Option<String>> {
        stubs.iter().map(|s| s.id.clone()).collect()
    }

    // -- log-format stability -------------------------------------------------

    /// The envelope's wire shape is the log format: field names, the external
    /// variant tag, and the transparent tenant string. Locked here so a change
    /// fails a test instead of silently orphaning committed entries.
    #[test]
    fn envelope_wire_format_is_stable() {
        let request = ControlRequest {
            op_id: uuid(1),
            principal: None,
            issued_at_secs: 42,
            expected_revision: None,
            op: ControlOp::DeleteImposter {
                tenant: TenantId::default(),
                port: 8080,
            },
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            json!({
                "op_id": "00000000-0000-0000-0000-000000000001",
                "issued_at_secs": 42,
                "op": { "DeleteImposter": { "tenant": "default", "port": 8080 } },
            })
        );
        // A pre-`issued_at_secs` entry still decodes (the field defaults to 0).
        let legacy: ControlRequest = serde_json::from_value(json!({
            "op_id": "00000000-0000-0000-0000-000000000001",
            "op": { "DeleteAll": { "tenant": "default" } },
        }))
        .expect("legacy envelope parses");
        assert_eq!(legacy.issued_at_secs, 0);
        assert_eq!(
            legacy.expected_revision, None,
            "a pre-#46 envelope decodes to an unconditional apply"
        );

        // A conditioned envelope carries the expectation as a plain integer.
        let conditioned = ControlRequest {
            expected_revision: Some(17),
            ..request
        };
        let value = serde_json::to_value(&conditioned).expect("serialize");
        assert_eq!(value["expected_revision"], json!(17));
    }

    /// Every variant tag in the log format, including the reserved ones whose
    /// payloads RFC-002 will define: the tags must never change spelling.
    #[test]
    fn every_variant_tag_is_stable() {
        let cases: Vec<(ControlOp, &str)> = vec![
            (
                ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: config(1),
                },
                "PutImposter",
            ),
            (
                ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port: 1,
                    edit: StubEditScript(vec![]),
                },
                "PatchStubs",
            ),
            (
                ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port: 1,
                },
                "DeleteImposter",
            ),
            (
                ControlOp::DeleteAll {
                    tenant: TenantId::default(),
                },
                "DeleteAll",
            ),
            (
                ControlOp::SetEnabled {
                    tenant: TenantId::default(),
                    port: 1,
                    enabled: true,
                },
                "SetEnabled",
            ),
            (ControlOp::TenantPut { body: json!({}) }, "TenantPut"),
            (ControlOp::TenantDelete { body: json!({}) }, "TenantDelete"),
            (ControlOp::PrincipalPut { body: json!({}) }, "PrincipalPut"),
            (
                ControlOp::PrincipalDelete { body: json!({}) },
                "PrincipalDelete",
            ),
            (ControlOp::BindingPut { body: json!({}) }, "BindingPut"),
            (
                ControlOp::BindingDelete { body: json!({}) },
                "BindingDelete",
            ),
        ];
        for (op, tag) in cases {
            let value = serde_json::to_value(&op).expect("serialize");
            let object = value.as_object().expect("externally tagged");
            assert_eq!(
                object.keys().collect::<Vec<_>>(),
                vec![tag],
                "variant tag drifted"
            );
            let _: ControlOp = serde_json::from_value(value).expect("round-trips");
        }
    }

    #[test]
    fn principal_is_omitted_when_absent_and_round_trips_when_present() {
        let mut request = ControlRequest {
            op_id: uuid(2),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::DeleteAll {
                tenant: TenantId::default(),
            },
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert!(value.get("principal").is_none());

        request.principal = Some("alice".to_owned());
        let value = serde_json::to_value(&request).expect("serialize");
        let back: ControlRequest = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back.principal.as_deref(), Some("alice"));
    }

    // -- validate -------------------------------------------------------------

    #[test]
    fn validate_accepts_the_real_ops_on_the_default_tenant() {
        let ok = [
            ControlOp::PutImposter {
                tenant: TenantId::default(),
                config: config(8080),
            },
            ControlOp::PatchStubs {
                tenant: TenantId::default(),
                port: 8080,
                edit: StubEditScript(vec![]),
            },
            ControlOp::DeleteImposter {
                tenant: TenantId::default(),
                port: 8080,
            },
            ControlOp::DeleteAll {
                tenant: TenantId::default(),
            },
        ];
        for op in ok {
            assert_eq!(validate(&op), Ok(()), "{op:?}");
        }
    }

    #[test]
    fn validate_rejects_a_non_default_tenant() {
        let op = ControlOp::DeleteAll {
            tenant: TenantId::new("acme"),
        };
        let err = validate(&op).expect_err("non-default tenant must be rejected");
        assert!(err.contains("tenant"), "{err}");
    }

    #[test]
    fn validate_rejects_a_config_without_an_explicit_port() {
        let op = ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(json!({ "protocol": "http" })).expect("parses"),
        };
        let err = validate(&op).expect_err("auto-assign cannot replicate");
        assert!(err.contains("port"), "{err}");
    }

    #[test]
    fn validate_rejects_an_unknown_protocol() {
        let op = ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(json!({ "port": 1, "protocol": "smtp" }))
                .expect("parses"),
        };
        let err = validate(&op).expect_err("protocol outside http/https");
        assert!(err.contains("protocol"), "{err}");
    }

    #[test]
    fn validate_rejects_duplicate_explicit_stub_ids() {
        let op = ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: serde_json::from_value(json!({
                "port": 1,
                "protocol": "http",
                "stubs": [ { "id": "a" }, { "id": "a" } ],
            }))
            .expect("parses"),
        };
        let err = validate(&op).expect_err("duplicate ids corrupt the stub-key diff");
        assert!(err.contains('a'), "{err}");
    }

    #[test]
    fn validate_accepts_set_enabled_on_the_default_tenant_only() {
        let op = ControlOp::SetEnabled {
            tenant: TenantId::default(),
            port: 1,
            enabled: false,
        };
        assert_eq!(validate(&op), Ok(()));

        let op = ControlOp::SetEnabled {
            tenant: TenantId::new("acme"),
            port: 1,
            enabled: false,
        };
        validate(&op).expect_err("non-default tenant is still refused");
    }

    #[test]
    fn validate_rejects_every_reserved_variant() {
        let reserved = [
            ControlOp::TenantPut { body: json!({}) },
            ControlOp::TenantDelete { body: json!({}) },
            ControlOp::PrincipalPut { body: json!({}) },
            ControlOp::PrincipalDelete { body: json!({}) },
            ControlOp::BindingPut { body: json!({}) },
            ControlOp::BindingDelete { body: json!({}) },
        ];
        for op in reserved {
            let err = validate(&op).expect_err("reserved until RFC-002");
            assert!(err.contains("#17"), "{err}");
        }
    }

    // -- apply_edit -----------------------------------------------------------

    #[test]
    fn add_appends_by_default_and_inserts_at_a_clamped_index() {
        let mut stubs = vec![stub(Some("a"))];
        apply_edit(
            &mut stubs,
            &StubEditScript(vec![
                StubEdit::Add {
                    stub: stub(Some("b")),
                    index: None,
                },
                StubEdit::Add {
                    stub: stub(Some("c")),
                    index: Some(0),
                },
                StubEdit::Add {
                    stub: stub(Some("d")),
                    index: Some(999),
                },
            ]),
        )
        .expect("all adds apply");
        assert_eq!(
            stub_ids(&stubs),
            [Some("c"), Some("a"), Some("b"), Some("d")].map(|s| s.map(String::from))
        );
    }

    #[test]
    fn add_rejects_a_duplicate_explicit_id() {
        let mut stubs = vec![stub(Some("a"))];
        let err = apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::Add {
                stub: stub(Some("a")),
                index: None,
            }]),
        )
        .expect_err("duplicate id must be rejected, like add_stub_unique");
        assert!(err.contains('a'), "{err}");
    }

    #[test]
    fn replace_by_id_keeps_position_and_forces_the_addressed_id() {
        let mut stubs = vec![stub(Some("a")), stub(Some("b")), stub(Some("c"))];
        let replacement: Stub = serde_json::from_value(json!({
            "id": "renamed-away",
            "routePattern": "/users/:id",
        }))
        .expect("parses");
        apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::ReplaceById {
                id: "b".to_owned(),
                stub: replacement,
            }]),
        )
        .expect("replace applies");
        assert_eq!(
            stub_ids(&stubs),
            [Some("a"), Some("b"), Some("c")].map(|s| s.map(String::from)),
            "position preserved, id forced back to the addressed id"
        );
        assert_eq!(stubs[1].route_pattern.as_deref(), Some("/users/:id"));
    }

    #[test]
    fn delete_by_id_removes_the_addressed_stub() {
        let mut stubs = vec![stub(Some("a")), stub(Some("b"))];
        apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::DeleteById { id: "a".to_owned() }]),
        )
        .expect("delete applies");
        assert_eq!(stub_ids(&stubs), [Some("b".to_owned())]);
    }

    #[test]
    fn by_id_steps_fail_on_a_missing_id() {
        let mut stubs = vec![stub(Some("a"))];
        for script in [
            StubEditScript(vec![StubEdit::DeleteById {
                id: "ghost".to_owned(),
            }]),
            StubEditScript(vec![StubEdit::ReplaceById {
                id: "ghost".to_owned(),
                stub: stub(None),
            }]),
        ] {
            let err = apply_edit(&mut stubs, &script).expect_err("missing id must fail");
            assert!(err.contains("ghost"), "{err}");
        }
    }

    #[test]
    fn move_reorders_and_bounds_checks() {
        let mut stubs = vec![stub(Some("a")), stub(Some("b")), stub(Some("c"))];
        apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::Move { from: 2, to: 0 }]),
        )
        .expect("in-bounds move applies");
        assert_eq!(
            stub_ids(&stubs),
            [Some("c"), Some("a"), Some("b")].map(|s| s.map(String::from))
        );

        let err = apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::Move { from: 0, to: 3 }]),
        )
        .expect_err("out-of-bounds destination must fail");
        assert!(err.contains('3'), "{err}");

        let err = apply_edit(
            &mut stubs,
            &StubEditScript(vec![StubEdit::Move { from: 5, to: 0 }]),
        )
        .expect_err("out-of-bounds source must fail");
        assert!(err.contains('5'), "{err}");
    }

    /// A failing step must leave the list untouched — the whole script is
    /// all-or-nothing, because it applies to a committed log entry.
    #[test]
    fn a_failing_script_mutates_nothing() {
        let mut stubs = vec![stub(Some("a"))];
        let before = serde_json::to_value(&stubs).expect("serialize");
        apply_edit(
            &mut stubs,
            &StubEditScript(vec![
                StubEdit::Add {
                    stub: stub(Some("b")),
                    index: None,
                },
                StubEdit::DeleteById {
                    id: "ghost".to_owned(),
                },
            ]),
        )
        .expect_err("second step fails");
        assert_eq!(
            serde_json::to_value(&stubs).expect("serialize"),
            before,
            "partial application would diverge replicas from the stored config"
        );
    }
}
