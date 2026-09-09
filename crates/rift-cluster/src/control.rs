//! The control-plane op set (ADR-001 §4.1): what an admin mutation becomes in the
//! Raft log, and the deterministic pure logic the state machine runs before it
//! mutates anything.
//!
//! Everything here must be deterministic across nodes: the same committed
//! [`ControlRequest`] against the same state-machine state yields the same
//! [`ControlResponse`] and the same table mutation on every replica. Anything
//! that can differ per node (port binds, listener state) lives in the engine
//! drive *after* apply, never here.

use rift_cluster_base::seams::{ImposterConfig, RecordedResponse, RouteTable, Stub};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The envelope every log entry carries: the op plus the identity needed for
/// dedup (`op_id`, from the client's `Idempotency-Key` or minted by the
/// accepting node) and attribution (`principal`, RFC-002 §6).
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
/// **Removing a variant is a log-format break, and #546, #549 and #550 each took one
/// deliberately.** This enum is externally-tagged `serde_json` with no envelope
/// version and no `#[serde(other)]` catch-all — `raft::store` writes entries with
/// `serde_json::to_vec` and reads them back with `from_slice` — so a variant that
/// is gone here cannot be decoded at all: a node replaying a log that still holds
/// an `AuditSinkPut`, `SourcePut`, `SpecPut`, `TenantPut`, `TenantDelete`,
/// `PrincipalPut`, `PrincipalCreate`, `PrincipalDelete`, `BindingPut` or
/// `BindingDelete` entry fails to start rather than skipping it. **#550 also removed
/// the `tenant` field from every surviving variant**, and that is *not* a decoding
/// break on its own: this enum sets no `deny_unknown_fields`, so an old entry's extra
/// `tenant` key is dropped and the op decodes — pinned on *this* type by
/// [`tests::an_old_entrys_tenant_field_is_ignored`], because a claim D-73 leans on
/// operationally should not rest on a test of some other wire shape, and across *every*
/// surviving variant by [`tests::every_surviving_variant_ignores_an_old_entrys_tenant_field`],
/// because `deny_unknown_fields` is per-container and one variant proves nothing about the
/// next. What actually refuses an
/// old state directory is redb: every state-machine table's key or value type lost
/// its tenant component, and redb answers `TableTypeMismatch` when a table is opened
/// under a definition whose types differ from the ones it was created with. That
/// guard exists on disk only. The wire has none — a mixed-version fleet straddling
/// #550 would exchange ops that decode on both sides and apply against different key
/// shapes — so **a mixed-version fleet across D-73 is unsupported**: every node
/// upgrades at once, from a fresh `cluster-state-dir`. Pre-release that is the right
/// trade, and clean removal is why it was taken; a *post*-release removal would have
/// to keep the variant as an ignored arm and version the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlOp {
    PutImposter {
        // Boxed: an inline `ImposterConfig` would make every op as large as the
        // biggest one (clippy::large_enum_variant); serde is transparent to it.
        config: Box<ImposterConfig>,
    },
    PatchStubs {
        port: u16,
        edit: StubEditScript,
    },
    DeleteImposter {
        port: u16,
    },
    /// Delete every imposter the fleet holds.
    DeleteAll,
    /// Pause/resume serving on a port, applied in place — never a wholesale
    /// replace (upstream #817 semantics; cluster #15).
    SetEnabled {
        port: u16,
        enabled: bool,
    },
    /// Whole-table replace of the front door's route table (issue #19 / U-11,
    /// cluster #131). Never a partial merge: [`RouteTable::validate`]
    /// checks the table as a unit (ambiguity is a property of the whole set),
    /// so admission must see — and apply must store — the whole thing.
    PutRoutes {
        table: RouteTable,
    },
    /// Remove one route by id. Idempotent at the state-machine level, like
    /// [`ControlOp::DeleteImposter`] — see `mutate_tables`'s comment for why.
    DeleteRoute {
        id: String,
    },
    /// Mint or rotate the fleet's session-signing key (RFC-006 §5.3, issue #185).
    ///
    /// One key, fleet-wide, so every node verifies a console session cookie from its own applied
    /// state without asking a peer — which is what makes a login *not* a Raft write. Only minting
    /// and rotating are; the steady state is pure local verification.
    ///
    /// **This op deliberately carries a secret into the replicated log — the only one that
    /// does.** It is admissible because of what the secret means outside the fleet: this key is
    /// fleet-internal and meaningless anywhere else. It cannot be stored hashed at all, because
    /// verifying an HMAC needs the key itself, not a one-way digest of it — a hash would make the
    /// cookie unverifiable by anyone, including us. A secret with power over a *third-party*
    /// system has no op that carries it: the credential-bearing source ops were removed with the
    /// tracking sources (#549, D-72).
    ///
    /// So it sits inside the same trust boundary as the state directory, which already holds all
    /// committed config. Rotation is the containment — and since D-73 it is the *only* one:
    /// writing a new key invalidates every outstanding session at once, and there is no
    /// per-session revocation because there is no per-session server state to revoke. Recorded in
    /// `docs/architecture/08-tenancy-security.md`.
    SessionKeyPut {
        /// 32 random bytes, hex-encoded. Hex rather than raw so the op stays printable in a log
        /// dump and survives JSON without a base64 alphabet decision.
        key: String,
    },
    /// Set or rename the fleet's operator-facing name (issue #373).
    ///
    /// Fleet-scoped and replicated rather than a per-node command-line flag: a flag lets two
    /// nodes disagree about what the fleet is called, which is exactly the confusion this
    /// feature exists to remove. One fleet has one name, agreed by consensus like the rest of
    /// the cluster's config, and every node — and every console session, regardless of which
    /// node it happens to be talking to — reads the same value back.
    FleetNamePut {
        name: String,
    },
    /// Bump a port's journal clear generation, or one space's within it (Ch.7 §"Clears are
    /// generation bumps — never timestamps", issue #224).
    ///
    /// A clear deletes nothing. It raises a counter, and every reader ignores entries stamped
    /// below it — so the clear converges by consensus rather than by a best-effort broadcast a
    /// partitioned peer can miss forever, and no node consults a clock to decide what "before the
    /// clear" means. That is what makes the result immune to skew: there is no timestamp in the
    /// path at all.
    ///
    /// **Carries no number.** Apply *increments*, rather than storing a value the submitter
    /// chose, because two clears racing from two nodes must both take effect: they commit in log
    /// order and compose to +2, which is harmlessly stronger than either alone since both mean
    /// "ignore everything before me". A submitted number would instead make the second clear
    /// silently overwrite the first with the same value.
    ///
    /// `space: None` clears the whole port; `Some(flow)` clears only that space's entries and
    /// leaves the port generation — and therefore every sibling space — untouched.
    ///
    /// D-38: a clear is a generation bump committed on the log, never a timestamped deletion.
    JournalClearGen {
        port: u16,
        space: Option<String>,
    },
    /// One `proxyOnce`/`proxyAlways` recording, as consensus fact (#226, Ch.7 §proxyOnce, D-40).
    ///
    /// Carries **both** the replayable response and — when predicate generation built one —
    /// the recorded stub, in a single op. Deliberately not two ops riding one front-door
    /// `Mutation`: the front door commits mutation ops one log entry at a time, so a two-op
    /// shape would make "recorded but stub-less" representable across a crash between them.
    /// One op, one apply transaction: the marker row and the stub mutation land together or
    /// not at all.
    ///
    /// The stub's insertion position is resolved **at apply**, against the then-current stub
    /// list (see [`RecordedStubPlacement`]), for the same reason upstream's
    /// `insert_or_append_proxy_stub` re-locates under its write lock: a position computed by
    /// the submitter can go stale between submission and commit.
    ProxyRecorded {
        port: u16,
        /// The claim key: hex `xxh64` of the request signature's canonical JSON — the same
        /// rendering the proxy store's HRW key uses, minus the port prefix (the row is
        /// already port-keyed).
        sig_hash: String,
        /// The replayable recorded response. Stored on consensus so `lookup()` answers from
        /// any node's applied state: for a stub-less proxyOnce recording this is the replay
        /// source *forever*, not just during a replication window.
        resp: RecordedResponse,
        stub: Option<RecordedStub>,
    },
    /// Delete every recorded-proxy marker for a port (#226) — the clustered half of
    /// `DELETE /imposters/:port/savedProxyResponses`. Recorded *stubs* are imposter config
    /// and are deleted through the stub-edit surfaces; this op clears the claim table so
    /// signatures record afresh.
    ProxyRecordedClear {
        port: u16,
    },
}

/// The stub half of a [`ControlOp::ProxyRecorded`]: the generated stub plus everything its
/// apply-time placement depends on. Grouped so an op cannot carry a placement without a stub
/// or vice versa.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedStub {
    pub stub: Box<Stub>,
    pub placement: RecordedStubPlacement,
    /// `proxy.to` of the proxy stub the recording came from — the anchor
    /// [`placement`](Self::placement) is resolved against at apply.
    pub proxy_to: String,
}

/// Where a recorded stub lands relative to its proxy stub — the engine's own semantics
/// (upstream `StubPlacement`, rift#911), mirrored here so the wire format is ours.
///
/// `BeforeProxy` (proxyOnce): the recording matches first next time. `AfterProxyMerging`
/// (proxyAlways): the proxy keeps running; responses merge into an existing stub with
/// structurally equal non-empty predicates (upstream #611) instead of duplicating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RecordedStubPlacement {
    BeforeProxy,
    AfterProxyMerging,
}

/// The fleet's session-signing key, as applied state (issue #185).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionKey {
    /// Hex-encoded HMAC-SHA256 key.
    pub key: String,
    /// The revision of the [`ControlOp::SessionKeyPut`] that produced this record. Bound into every
    /// token, so a rotation invalidates outstanding cookies by construction rather than by sweeping
    /// a table: a cookie minted under revision N stops verifying the moment N+1 is applied.
    pub revision: u64,
}

/// Bytes in a session-signing key. 32 is HMAC-SHA256's block-optimal size — longer buys nothing,
/// shorter weakens it.
pub const SESSION_KEY_BYTES: usize = 32;

/// The largest payload a single [`ControlOp`] may carry into the log.
///
/// A bound on what a *log entry* carries, which is a fleet-wide liability: every replica stores
/// it and every snapshot copies it. It sits under the cluster transport's own cap, so a payload
/// that passes here still has to fit on the wire.
///
/// Until #549 this also bounded a `SourcePullResult`'s config set (where it matched upstream's
/// 10 MB fetch cap); with the source ops gone, the only op that can carry a payload of any size
/// is [`ControlOp::ProxyRecorded`]'s recorded response body.
pub const MAX_LOG_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Longest a fleet name ([`ControlOp::FleetNamePut`]) may be, in `char`s. A cap exists so the
/// name stays chrome-sized wherever it renders (a top bar, a members-list column); 128
/// matches the length ceiling a chrome-sized operator-chosen name wants, not any technical
/// constraint of the field itself.
pub const MAX_FLEET_NAME_CHARS: usize = 128;

/// An ordered sequence of stub edits, applied atomically to one imposter's stub
/// list — the order-aware #316 semantics, mirroring
/// `ImposterManager::{add_stub, replace_stub_by_id, delete_stub_by_id, move_stub}`.
///
/// This is the by-id/positional level of D-5's two-level reconcile: an explicit
/// script carries a reorder as a `Move`, never as delete+add, so the moved slot
/// keeps its runtime state. The whole-config level (a `PutImposter`) is diffed
/// upstream by `apply_config` (U-6) on stable stub keys, where a pure reorder
/// costs nothing — D-5 is why order matters (it is match priority) and why a
/// set-diff was rejected.
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
    /// Every stub scoped to `space` (issue #537, D-69). Set-addressed, not id-addressed, because a
    /// space stub is not required to carry an `id` — `DeleteById` cannot express this at all.
    ///
    /// Committed by a space teardown alongside its journal clear. Once space stubs replicate, a
    /// teardown that only tore down the local engine would be undone by the next
    /// `EngineAction::Sync`, which re-renders every imposter from `sm_configs` and would
    /// resurrect them fleet-wide.
    DeleteBySpace {
        space: String,
    },
    Move {
        from: usize,
        to: usize,
    },
}

impl ControlOp {
    /// The variant's name, for the committed-write log line (`RaftNode::write`). Exhaustive
    /// with no wildcard arm so a new op cannot land in the trail as "unknown".
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            ControlOp::PutImposter { .. } => "PutImposter",
            ControlOp::PatchStubs { .. } => "PatchStubs",
            ControlOp::DeleteImposter { .. } => "DeleteImposter",
            ControlOp::DeleteAll => "DeleteAll",
            ControlOp::SetEnabled { .. } => "SetEnabled",
            ControlOp::PutRoutes { .. } => "PutRoutes",
            ControlOp::DeleteRoute { .. } => "DeleteRoute",
            ControlOp::SessionKeyPut { .. } => "SessionKeyPut",
            ControlOp::FleetNamePut { .. } => "FleetNamePut",
            ControlOp::JournalClearGen { .. } => "JournalClearGen",
            ControlOp::ProxyRecorded { .. } => "ProxyRecorded",
            ControlOp::ProxyRecordedClear { .. } => "ProxyRecordedClear",
        }
    }
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
/// duplicate explicit stub ids), plus the cluster-only rule that a replicable
/// config must carry an explicit port (auto-assign cannot replicate — every node
/// would pick a different port).
///
/// `Err` carries the reason recorded in the `Failed` outcome. It must depend
/// only on the op itself, never on per-node state.
pub fn validate(op: &ControlOp) -> Result<(), String> {
    match op {
        ControlOp::PutImposter { config } => validate_replicable_config(config),
        ControlOp::PatchStubs { .. }
        | ControlOp::DeleteImposter { .. }
        | ControlOp::DeleteAll
        | ControlOp::SetEnabled { .. }
        // A delete removes one route from an already-validated table. Ambiguity is pairwise, so
        // removing an element can only shrink the set of matching pairs, never create one — the
        // remaining table is structurally guaranteed valid, so there is nothing to check here.
        | ControlOp::DeleteRoute { .. } => Ok(()),
        ControlOp::PutRoutes { table } => {
            // The U-11 rules (unique ids, ambiguous enabled matches,
            // strip_prefix without path_prefix, malformed wildcard/method/
            // prefix) plus the whole-table atomicity the issue calls for: a
            // table is accepted or refused as a unit, never partially.
            table.validate().map_err(|e| e.to_string())
        }
        ControlOp::SessionKeyPut { key } => {
            // Checked at admission rather than trusted from the caller: a short or malformed key
            // would still verify its own tokens, so the weakness would be silent — every session
            // would work, and only the security property would be gone.
            let decoded =
                hex_decode(key).ok_or_else(|| "session key must be hex-encoded".to_owned())?;
            if decoded.len() != SESSION_KEY_BYTES {
                return Err(format!(
                    "session key must be exactly {SESSION_KEY_BYTES} bytes, got {}",
                    decoded.len()
                ));
            }
            Ok(())
        }
        ControlOp::FleetNamePut { name } => {
            // Deliberately not the `[A-Za-z0-9._-]` charset that guards ids
            // that appear in paths and redb keys. A fleet name is chrome text a human reads in
            // the console's top bar — it is never parsed back into an address — so the only
            // hazards worth guarding against are "renders as nothing" and "corrupts the chrome
            // that displays it".
            if name.trim().is_empty() {
                return Err(
                    "fleet name must not be empty or whitespace-only: a name a human cannot \
                     read is the same confusion as no name at all"
                        .to_owned(),
                );
            }
            // Counted in chars, not bytes: a human-facing length cap should bound how much text
            // renders, not how many bytes a particular character happens to encode to.
            let char_count = name.chars().count();
            if char_count > MAX_FLEET_NAME_CHARS {
                return Err(format!(
                    "fleet name is {char_count} characters, over the {MAX_FLEET_NAME_CHARS} \
                     character cap"
                ));
            }
            if name.chars().any(char::is_control) {
                return Err(
                    "fleet name must not contain control characters: one could corrupt a log \
                     line, a terminal, or the console's own chrome"
                        .to_owned(),
                );
            }
            Ok(())
        }
        // Deliberately shallow: only the checks that hold regardless of state. Whether an
        // imposter exists on `port` is an apply-time question (`raft::store::mutate_tables`'
        // `JournalClearGen` arm) — the same split every other op here draws, and the reason is the
        // same too: `validate` runs identically on every replica from the op alone, so it must
        // never depend on a table a replica could disagree with another about.
        ControlOp::JournalClearGen { port, space } => {
            if *port == 0 {
                return Err("port must be non-zero: 0 addresses no imposter to clear".to_owned());
            }
            if let Some(space) = space
                && space.is_empty()
            {
                return Err(
                    "space must not be empty when given: an empty scope is not a narrower \
                     clear, it is an unaddressed one"
                        .to_owned(),
                );
            }
            Ok(())
        }
        // Shallow for the same reason as `JournalClearGen`: whether the port's imposter
        // exists — and whether a proxy stub with `proxy_to` is still in it — are apply-time
        // questions against the then-current tables.
        ControlOp::ProxyRecorded {
            port,
            sig_hash,
            resp,
            stub,
        } => {
            if *port == 0 {
                return Err("port must be non-zero: 0 addresses no imposter".to_owned());
            }
            if sig_hash.is_empty() || hex_decode(sig_hash).is_none() {
                return Err("sigHash must be a non-empty hex string".to_owned());
            }
            if resp.body.len() > MAX_LOG_PAYLOAD_BYTES {
                return Err(format!(
                    "recorded response body exceeds the {MAX_LOG_PAYLOAD_BYTES}-byte log \
                     entry bound"
                ));
            }
            if let Some(recorded) = stub
                && recorded.proxy_to.is_empty()
            {
                return Err(
                    "proxyTo must not be empty: placement is resolved against the proxy \
                     stub it names"
                        .to_owned(),
                );
            }
            Ok(())
        }
        ControlOp::ProxyRecordedClear { port } => {
            if *port == 0 {
                return Err("port must be non-zero: 0 addresses no imposter to clear".to_owned());
            }
            Ok(())
        }
    }
}

/// Decode a lowercase-or-uppercase hex string, or `None` if it is not hex.
///
/// Hand-rolled to keep a dependency out of the control plane for one 64-character string; the
/// alternative is a crate on the Raft admission path for something this small.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    // The alphabet is checked explicitly rather than left to `from_str_radix`, which accepts a
    // leading sign: `u8::from_str_radix("+0", 16)` is `Ok(0)`, so `"+0"` repeated 32 times is 64
    // characters that decode to 32 zero bytes and would sail through as a valid key. That is
    // exactly the silent weakness this validation exists to prevent — an all-zero key still signs
    // and verifies its own tokens perfectly, so nothing would ever look wrong.
    if !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The rules every config carried by the log must satisfy — an operator's own
/// [`ControlOp::PutImposter`], a compiled OpenAPI import, or a `--imposters`
/// bootstrap, all of which reach the log through that one op (D-72).
fn validate_replicable_config(config: &ImposterConfig) -> Result<(), String> {
    if config.port.is_none() {
        return Err(
            "config must carry an explicit port: an auto-assigned port cannot replicate".to_owned(),
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
    // The clustered store's knobs (#120). Refused here, pre-commit, because
    // `FlowStoreProvider::provide` has no error channel — by the time the
    // provider reads the config it must already be valid.
    crate::stores::FlowConfig::validate(config)
}

/// What stored record an `expected_revision` precondition holds against.
///
/// Two shapes, because the control plane has two things worth conditioning on
/// and they are keyed differently: a single imposter row in `sm_configs`, and the
/// front-door route table as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionTarget {
    /// The `sm_configs` row at `port`; its revision is the record's own
    /// `StoredImposter::revision`.
    Imposter(u16),
    /// The whole route table (issue #210); its revision is the one-row
    /// `sm_routes_revision` table, absent meaning 0.
    ///
    /// Table-wide and not per-route on purpose: `PutRoutes` replaces the set as
    /// a unit, so the only thing a client can meaningfully condition a replace
    /// on is the state of the set it read. `DeleteRoute` stamps the same
    /// revision — a delete mutates the table, so it must invalidate every
    /// outstanding precondition against it, or a client that read before the
    /// delete could replace the table wholesale after it and silently restore
    /// the deleted route.
    RouteTable,
}

/// The record `op`'s `expected_revision` addresses, or `None` if `op` has no
/// such target (a bulk op, or a fleet-wide one). Used by the state
/// machine's expected-revision check (#46, extended to route tables by #210): a
/// precondition can only ever hold against one stored revision, so every op
/// without a target refuses a precondition deterministically rather than
/// silently ignoring it.
///
/// The match below is exhaustive with no wildcard arm, deliberately: a new
/// `ControlOp` variant must fail to compile here until someone has decided
/// whether it is conditionable. A `_ => None` would instead let it silently
/// join the "preconditions do not apply" set, which is exactly how #210's
/// lost-update shipped in the first place.
#[must_use]
pub fn precondition_target(op: &ControlOp) -> Option<PreconditionTarget> {
    match op {
        // `config.port` is validated to be present before this ever matters,
        // but a `None` here must still yield `None`, not a bogus target.
        ControlOp::PutImposter { config } => config.port.map(PreconditionTarget::Imposter),
        ControlOp::PatchStubs { port, .. }
        | ControlOp::DeleteImposter { port }
        | ControlOp::SetEnabled { port, .. } => Some(PreconditionTarget::Imposter(*port)),
        // Both route ops condition on — and stamp — the one table revision. A
        // per-route precondition would be a different feature and needs a
        // single-route upsert op to hang off; #210 deliberately does not add one.
        ControlOp::PutRoutes { .. } | ControlOp::DeleteRoute { .. } => {
            Some(PreconditionTarget::RouteTable)
        }
        ControlOp::DeleteAll
        // The session key addresses the fleet, not an imposter record.
        | ControlOp::SessionKeyPut { .. }
        // The fleet name addresses the fleet, not an imposter record — same reasoning as the
        // session key immediately above.
        | ControlOp::FleetNamePut { .. }
        // A clear is a convergence primitive, not a config write conditioned on a stored
        // revision: it commits unconditionally (apply takes the `max`), so two concurrent clears
        // compose rather than one losing an optimistic-concurrency race the op was never meant
        // to run.
        | ControlOp::JournalClearGen { .. }
        // A recording is submitted by the engine's claim owner, not by an
        // optimistic-concurrency client; its placement is resolved at apply against the
        // then-current stubs, which is the property a stored-revision precondition would
        // re-introduce a race against. The clear follows `JournalClearGen`'s reasoning.
        | ControlOp::ProxyRecorded { .. }
        | ControlOp::ProxyRecordedClear { .. } => None,
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
            // Idempotent, unlike every by-id step above. Those address one named thing the caller
            // asserted exists, so a miss is the caller being wrong; this addresses a *set*, and an
            // empty set is a legitimate answer. A space teardown commits this unconditionally
            // after the flow-state half succeeds, and a space carrying flow state but no stubs is
            // ordinary — erroring here would turn that into a 500.
            StubEdit::DeleteBySpace { space } => {
                next.retain(|s| s.space.as_deref() != Some(space.as_str()));
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

    /// The envelope's wire shape is the log format: field names and the
    /// external variant tag. Locked here so a change fails a test instead of
    /// silently orphaning committed entries. Since #550 no op carries a tenant.
    #[test]
    fn envelope_wire_format_is_stable() {
        let request = ControlRequest {
            op_id: uuid(1),
            principal: None,
            issued_at_secs: 42,
            expected_revision: None,
            op: ControlOp::DeleteImposter { port: 8080 },
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            json!({
                "op_id": "00000000-0000-0000-0000-000000000001",
                "issued_at_secs": 42,
                "op": { "DeleteImposter": { "port": 8080 } },
            })
        );
        // A pre-`issued_at_secs` entry still decodes (the field defaults to 0).
        let legacy: ControlRequest = serde_json::from_value(json!({
            "op_id": "00000000-0000-0000-0000-000000000001",
            "op": "DeleteAll",
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

    /// Every variant tag in the log format: the tags must never change spelling,
    /// and — since #550 — no payload carries a tenant.
    #[test]
    fn every_variant_tag_is_stable() {
        let cases: Vec<(ControlOp, &str)> = vec![
            (ControlOp::PutImposter { config: config(1) }, "PutImposter"),
            (
                ControlOp::PatchStubs {
                    port: 1,
                    edit: StubEditScript(vec![]),
                },
                "PatchStubs",
            ),
            (ControlOp::DeleteImposter { port: 1 }, "DeleteImposter"),
            (
                ControlOp::SetEnabled {
                    port: 1,
                    enabled: true,
                },
                "SetEnabled",
            ),
            (
                ControlOp::PutRoutes {
                    table: RouteTable::default(),
                },
                "PutRoutes",
            ),
            (ControlOp::DeleteRoute { id: "r".to_owned() }, "DeleteRoute"),
            (
                ControlOp::SessionKeyPut {
                    key: "00".repeat(SESSION_KEY_BYTES),
                },
                "SessionKeyPut",
            ),
            (
                ControlOp::FleetNamePut {
                    name: "prod".to_owned(),
                },
                "FleetNamePut",
            ),
            (
                ControlOp::JournalClearGen {
                    port: 1,
                    space: None,
                },
                "JournalClearGen",
            ),
            (
                ControlOp::ProxyRecordedClear { port: 1 },
                "ProxyRecordedClear",
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
            assert!(
                !object[tag]
                    .as_object()
                    .is_some_and(|o| o.contains_key("tenant")),
                "no op carries a tenant since #550: {tag}"
            );
            let _: ControlOp = serde_json::from_value(value).expect("round-trips");
        }
        // `DeleteAll` is a unit variant, so it serializes as the bare tag string
        // rather than a one-key object — asserted separately for that reason.
        assert_eq!(
            serde_json::to_value(ControlOp::DeleteAll).expect("serialize"),
            json!("DeleteAll")
        );
    }

    #[test]
    fn principal_is_omitted_when_absent_and_round_trips_when_present() {
        let mut request = ControlRequest {
            op_id: uuid(2),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op: ControlOp::DeleteAll,
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
    fn validate_accepts_the_real_resource_ops() {
        let ok = [
            ControlOp::PutImposter {
                config: config(8080),
            },
            ControlOp::PatchStubs {
                port: 8080,
                edit: StubEditScript(vec![]),
            },
            ControlOp::DeleteImposter { port: 8080 },
            ControlOp::DeleteAll,
            ControlOp::SetEnabled {
                port: 1,
                enabled: false,
            },
        ];
        for op in ok {
            assert_eq!(validate(&op), Ok(()), "{op:?}");
        }
    }

    #[test]
    fn validate_rejects_a_config_without_an_explicit_port() {
        let op = ControlOp::PutImposter {
            config: serde_json::from_value(json!({ "protocol": "http" })).expect("parses"),
        };
        let err = validate(&op).expect_err("auto-assign cannot replicate");
        assert!(err.contains("port"), "{err}");
    }

    #[test]
    fn validate_rejects_an_unknown_protocol() {
        let op = ControlOp::PutImposter {
            config: serde_json::from_value(json!({ "port": 1, "protocol": "smtp" }))
                .expect("parses"),
        };
        let err = validate(&op).expect_err("protocol outside http/https");
        assert!(err.contains("protocol"), "{err}");
    }

    #[test]
    fn validate_rejects_duplicate_explicit_stub_ids() {
        let op = ControlOp::PutImposter {
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

    // -- validate: JournalClearGen (issue #224) --------------------------------

    #[test]
    fn validate_rejects_a_journal_clear_on_port_zero() {
        let op = ControlOp::JournalClearGen {
            port: 0,
            space: None,
        };
        let err = validate(&op).expect_err("port 0 addresses no imposter");
        assert!(err.contains("port"), "{err}");
    }

    #[test]
    fn validate_rejects_a_journal_clear_on_an_empty_space() {
        let op = ControlOp::JournalClearGen {
            port: 1,
            space: Some(String::new()),
        };
        let err = validate(&op).expect_err("an empty space is not a narrower clear");
        assert!(err.contains("space"), "{err}");
    }

    #[test]
    fn validate_accepts_a_well_formed_journal_clear_for_both_scopes() {
        let port_wide = ControlOp::JournalClearGen {
            port: 1,
            space: None,
        };
        assert_eq!(validate(&port_wide), Ok(()));

        let space_scoped = ControlOp::JournalClearGen {
            port: 1,
            space: Some("checkout".to_owned()),
        };
        assert_eq!(validate(&space_scoped), Ok(()));
    }

    // -- validate: PutRoutes / DeleteRoute -------------------------------------

    use rift_cluster_base::seams::{Route, RouteMatch, RouteTarget};

    fn route(id: &str, port: u16) -> Route {
        Route {
            id: id.to_owned(),
            priority: 0,
            matches: RouteMatch::default(),
            target: RouteTarget {
                port,
                strip_prefix: false,
                set_host: None,
            },
            enabled: true,
        }
    }

    #[test]
    fn validate_accepts_a_well_formed_route_table() {
        let op = ControlOp::PutRoutes {
            table: RouteTable {
                routes: vec![route("a", 1)],
            },
        };
        assert_eq!(validate(&op), Ok(()));
    }

    #[test]
    fn validate_rejects_a_route_table_with_duplicate_ids() {
        let op = ControlOp::PutRoutes {
            table: RouteTable {
                routes: vec![route("same", 1), route("same", 2)],
            },
        };
        let err = validate(&op).expect_err("duplicate route ids must be rejected");
        assert!(err.contains("same"), "{err}");
    }

    #[test]
    fn validate_rejects_a_route_table_with_ambiguous_enabled_matches() {
        let a = route("a", 1);
        let b = route("b", 2);
        let op = ControlOp::PutRoutes {
            // Both catch-all (default `RouteMatch`), both enabled, same
            // priority: the exact ambiguity `RouteTable::validate` exists to
            // catch.
            table: RouteTable { routes: vec![a, b] },
        };
        let err = validate(&op).expect_err("ambiguous enabled matches must be rejected");
        assert!(err.contains("match"), "{err}");
    }

    #[test]
    fn validate_rejects_strip_prefix_without_a_path_prefix() {
        let mut bad = route("bad", 1);
        bad.target.strip_prefix = true;
        let op = ControlOp::PutRoutes {
            table: RouteTable { routes: vec![bad] },
        };
        let err = validate(&op).expect_err("strip_prefix without path_prefix must be rejected");
        assert!(err.contains("strip_prefix"), "{err}");
    }

    #[test]
    fn validate_rejects_a_malformed_wildcard_host() {
        let mut bad = route("bad", 1);
        bad.matches.host = Some("pay*.test".to_owned());
        let op = ControlOp::PutRoutes {
            table: RouteTable { routes: vec![bad] },
        };
        let err = validate(&op).expect_err("a malformed wildcard host must be rejected");
        assert!(err.contains("wildcard"), "{err}");
    }

    #[test]
    fn validate_accepts_delete_route() {
        let op = ControlOp::DeleteRoute {
            id: "any".to_owned(),
        };
        assert_eq!(validate(&op), Ok(()));
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

    /// Pins D-5: a reorder is carried as a `Move` step that relocates the stub
    /// in place — not as a delete+add pair, which would reset the moved slot's
    /// runtime state cluster-wide.
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

    // -- issue #373: the fleet's operator-set name ----------------------------

    fn fleet_name(name: &str) -> ControlOp {
        ControlOp::FleetNamePut {
            name: name.to_owned(),
        }
    }

    #[test]
    fn validate_accepts_a_well_formed_fleet_name() {
        assert_eq!(validate(&fleet_name("rift-prod-eu")), Ok(()));
    }

    #[test]
    fn validate_accepts_a_fleet_name_with_spaces_and_non_ascii() {
        // Deliberately permitted: this is chrome text a human reads, not an id that addresses
        // anything, so the `[A-Za-z0-9._-]` rule that guards path- and redb-key-safe ids
        // would be borrowed reasoning here.
        assert_eq!(validate(&fleet_name("Rift Prod (eu-west) ✱")), Ok(()));
    }

    #[test]
    fn validate_rejects_an_empty_fleet_name() {
        let err = validate(&fleet_name("")).expect_err("an empty fleet name must be rejected");
        assert!(err.contains("fleet name"), "{err}");
    }

    #[test]
    fn validate_rejects_a_whitespace_only_fleet_name() {
        // Distinct from the empty case: a name that renders as nothing is the same operator
        // mistake, and "the top bar is blank" is exactly the confusion #373 exists to remove.
        let err = validate(&fleet_name("   \t ")).expect_err("a blank fleet name must be rejected");
        assert!(err.contains("fleet name"), "{err}");
    }

    #[test]
    fn validate_rejects_an_over_long_fleet_name() {
        let err = validate(&fleet_name(&"n".repeat(129)))
            .expect_err("an over-long fleet name must be rejected");
        assert!(err.contains("128"), "{err}");
    }

    #[test]
    fn validate_accepts_a_fleet_name_at_the_length_cap() {
        assert_eq!(validate(&fleet_name(&"n".repeat(128))), Ok(()));
    }

    #[test]
    fn validate_rejects_a_fleet_name_with_control_characters() {
        // The real hazard for a label that is never parsed: a newline or escape sequence that
        // corrupts a log line, a terminal, or the console's own chrome.
        for bad in ["prod\nstaging", "prod\u{1b}[31m", "prod\u{0}"] {
            let err = validate(&fleet_name(bad))
                .expect_err("a fleet name carrying control characters must be rejected");
            assert!(err.contains("control"), "{bad:?} -> {err}");
        }
    }

    /// Pins the decoding half of [`ControlOp`]'s doc and of D-73's amendment: dropping the
    /// `tenant` field from every variant is **not** a wire break. This enum carries no
    /// `deny_unknown_fields`, so a log entry written before #550 — carrying `tenant` next to
    /// the fields this build knows — still decodes, and the stale key is simply ignored. The
    /// guard that *does* refuse an old fleet is redb's per-table `TableTypeMismatch`, on disk;
    /// it is important that nobody reads a mixed-version fleet's silence on the wire as safety.
    ///
    /// Written against `ControlOp` itself rather than borrowing `raft::network`'s join-reply
    /// tests: those pin `JoinAccepted`, and a `deny_unknown_fields` added to *this* type would
    /// leave them green while every pre-#550 entry stopped replaying.
    #[test]
    fn an_old_entrys_tenant_field_is_ignored() {
        let old = json!({
            "PutImposter": {
                "config": { "port": 4545, "protocol": "http" },
                "tenant": "default",
            }
        });
        let op: ControlOp =
            serde_json::from_value(old).expect("a pre-#550 entry's extra `tenant` key is ignored");
        match op {
            ControlOp::PutImposter { config } => assert_eq!(config.port, Some(4545)),
            other => panic!("expected PutImposter, got {other:?}"),
        }

        // The other direction, so "it decodes" cannot mean the enum decodes anything at all: a
        // *missing* required field is still an error, not a silent default. Bound and grepped,
        // like every other negative assertion in this module: `serde_json` also answers `Err`
        // for `unknown variant \`PatchStubs\``, and #546/#549/#550 each removed variants — one
        // more rename and a bare `expect_err` would assert nothing while staying green, in
        // exactly the "it decodes anything" direction this case exists to rule out.
        let missing = json!({ "PatchStubs": { "port": 4545 } });
        let err = serde_json::from_value::<ControlOp>(missing)
            .expect_err("a variant missing a required field must not decode");
        assert!(
            err.to_string().contains("missing field `edit`"),
            "must fail on the missing field, not on the variant name having moved: {err}"
        );
    }

    /// [`ControlOp`]'s doc claims #550 dropped `tenant` from **every** surviving variant, so
    /// every one of them must ignore the stale key — not just the `PutImposter` that
    /// [`tests::an_old_entrys_tenant_field_is_ignored`] happens to spell out.
    /// `deny_unknown_fields` is per-container: adding it to `PatchStubs` alone would leave that
    /// test green while every pre-#550 `PatchStubs` entry stopped replaying — the same
    /// one-level-down defeat its own rationale describes, one level further down.
    ///
    /// Each op is serialized, given a `tenant` key, decoded, and re-serialized: the round trip
    /// must land back on the byte-identical clean value, so "it decoded" cannot mean it decoded
    /// into something else.
    #[test]
    fn every_surviving_variant_ignores_an_old_entrys_tenant_field() {
        // Adding a variant breaks this match, which is the reminder to extend the list below.
        // `DeleteAll` is absent from it on purpose: a unit variant serializes as the bare tag
        // string, so there is no object for a stale key to sit in.
        fn _every_variant_is_accounted_for(op: &ControlOp) {
            match op {
                ControlOp::PutImposter { .. }
                | ControlOp::PatchStubs { .. }
                | ControlOp::DeleteImposter { .. }
                | ControlOp::DeleteAll
                | ControlOp::SetEnabled { .. }
                | ControlOp::PutRoutes { .. }
                | ControlOp::DeleteRoute { .. }
                | ControlOp::SessionKeyPut { .. }
                | ControlOp::FleetNamePut { .. }
                | ControlOp::JournalClearGen { .. }
                | ControlOp::ProxyRecorded { .. }
                | ControlOp::ProxyRecordedClear { .. } => {}
            }
        }

        let survivors = vec![
            ControlOp::PutImposter { config: config(1) },
            ControlOp::PatchStubs {
                port: 1,
                edit: StubEditScript(vec![]),
            },
            ControlOp::DeleteImposter { port: 1 },
            ControlOp::SetEnabled {
                port: 1,
                enabled: true,
            },
            ControlOp::PutRoutes {
                table: RouteTable::default(),
            },
            ControlOp::DeleteRoute { id: "r".to_owned() },
            ControlOp::SessionKeyPut {
                key: "00".repeat(SESSION_KEY_BYTES),
            },
            ControlOp::FleetNamePut {
                name: "prod".to_owned(),
            },
            ControlOp::JournalClearGen {
                port: 1,
                space: None,
            },
            ControlOp::ProxyRecorded {
                port: 1,
                sig_hash: "0123456789abcdef".to_owned(),
                resp: RecordedResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![],
                    latency_ms: None,
                    timestamp_secs: 0,
                },
                stub: None,
            },
            ControlOp::ProxyRecordedClear { port: 1 },
        ];

        for op in survivors {
            let clean = serde_json::to_value(&op).expect("a ControlOp serializes");
            let mut with_tenant = clean.clone();
            let (tag, fields) = with_tenant
                .as_object_mut()
                .and_then(|object| object.iter_mut().next())
                .expect("externally tagged: one key, the variant tag");
            let tag = tag.clone();
            fields
                .as_object_mut()
                .unwrap_or_else(|| panic!("`{tag}` must be a struct variant to be listed here"))
                .insert("tenant".to_owned(), json!("default"));

            let back: ControlOp = serde_json::from_value(with_tenant).unwrap_or_else(|e| {
                panic!("a pre-#550 `{tag}` entry's stale `tenant` key must be ignored: {e}")
            });
            assert_eq!(
                serde_json::to_value(&back).expect("a ControlOp serializes"),
                clean,
                "`{tag}` must decode back to exactly itself"
            );
        }
    }
}
