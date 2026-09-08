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

/// Tenant scope of a control op. Every op carries one; which tenant ids
/// [`validate`] accepts for a given op depends on the op — see its doc.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

/// The tenant every op ran against before RFC-002 (#17) multi-tenancy, and
/// still the tenant a request implicitly targets when nothing else says
/// otherwise. The one tenant id [`validate`] never lets [`ControlOp::TenantDelete`]
/// remove — the fleet must always have somewhere for an unscoped request to land.
pub const DEFAULT_TENANT: &str = "default";

/// Whether routes stored under `tenant` are compiled into the shared front door.
///
/// **The single definition of that rule.** The state machine's route compiler filters on it, and
/// the admin plane's per-route hit read reports it (issue #368) — two call sites, one answer, so
/// they cannot drift into a console that says a table is live while the fleet never installed it.
///
/// Only the default tenant's routes are installed today. The reasoning is long and lives with the
/// compiler that enforces it (`RedbStateMachine::desired_routes`): the front door is a single
/// listener with no tenant discriminator, so a unioned table would let any tenant publish a
/// catch-all that captures the whole fleet's front-door traffic. Tenanted routes are still stored
/// and still read back per tenant, so a tenant sees what it wrote — they are simply never
/// compiled in. When the front door grows a tenant dimension, this function is what changes.
#[must_use]
pub fn routes_installed_for(tenant: &str) -> bool {
    tenant == DEFAULT_TENANT
}

/// The reserved fleet-wide scope (RFC-002 §3.3, §8.4): not a real tenant —
/// there is no [`ControlOp::TenantPut`] record for it, [`validate`] refuses one
/// — and the only scope [`Role::FleetAdmin`] may bind against. Every other
/// role is meaningless there and [`validate`] refuses that pairing too.
pub const FLEET_SCOPE: &str = "*";

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

/// A principal's identity (RFC-002 §3.2): the RBAC subject a request
/// authenticates as. Newtype over `String` for the same reason [`TenantId`]
/// is one — it is a redb key component and an admin-surface path segment, not
/// a bare string to be confused with a display name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrincipalId(String);

impl PrincipalId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-tenant resource ceilings (RFC-002 §3.4).
///
/// Every field here is a **count of objects**, and that is the whole definition
/// (§7): quotas bound how much a tenant may *have*, never how much CPU it may
/// burn. One tenant's pathological regex still degrades a shared node — a
/// stated non-goal, not a gap in this struct.
///
/// `Default` picks generous ceilings rather than zero, so a tenant created
/// without an explicit quota is immediately usable instead of silently
/// capacity-locked.
///
/// # §11 open question 2, settled here
///
/// `journal_retention` used to live on this struct and no longer does — it is a
/// **duration policy**, not a count, and it is enforced by the M3 request
/// shards rather than by anything that counts objects. Leaving it here would
/// hand M3 (#147) a field whose name says "quota" and whose meaning is "how
/// long to keep data", enforced somewhere no other field in this struct is. It
/// now sits on [`Tenant::journal_retention_secs`], beside the other per-tenant
/// policy. Moved before M2 ships, so nothing inherits the ambiguity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quotas {
    /// Committed imposters a tenant may hold. Enforced at apply (#163).
    pub max_imposters: u32,
    /// Stubs on any one imposter. Enforced at apply (#163).
    pub max_stubs_per_imposter: u32,
    /// Flow-state entries. Enforced by the flow owner, not at apply — the
    /// entries are not in the state machine — so this slice stores it and #147
    /// applies it.
    pub max_flow_entries: u64,
}

impl Default for Quotas {
    fn default() -> Self {
        Self {
            max_imposters: 1_000,
            max_stubs_per_imposter: 1_000,
            max_flow_entries: 100_000,
        }
    }
}

/// One tenant's config-table usage against [`Quotas`] (issue #372): what
/// `GET /admin/tenants` and `GET /admin/tenants/:id` report alongside the
/// limits themselves. Built by [`crate::raft::RedbStateMachine::tenant_config_usage`]
/// in a single scan of `sm_configs` for every tenant at once — see that
/// method's doc for why a per-tenant scan is not an option (issue #372's AC7).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantConfigUsage {
    /// Committed imposters this tenant holds — compared against
    /// [`Quotas::max_imposters`].
    pub imposters: u32,
    /// The **maximum** `stubs.len()` across this tenant's imposters, not the
    /// sum: [`Quotas::max_stubs_per_imposter`] is a per-imposter ceiling, so a
    /// sum would report a tenant as over quota (or nowhere near it) for a
    /// number no single imposter ever carried.
    pub max_stubs: u32,
    /// Every port this tenant holds a config on — what the flow-entry usage
    /// fan-out (`FlowNet::fleet_entry_counts`) is asked to count against.
    pub ports: Vec<u16>,
    /// At least one of this tenant's `sm_configs` rows failed to parse and was
    /// excluded (see `RedbStateMachine::tenant_config_usage`). A per-tenant
    /// field rather than one flag for the whole scan: the corrupt row's key
    /// still names its tenant even when its value does not decode, so the
    /// scan already knows *which* tenant's figures are undercounted, and
    /// flagging every other tenant along with it would be a fabricated
    /// warning about numbers that are actually exact. `dispatch` ORs this
    /// into the response's `Rift-Cluster-Partial`, next to the flow-entry
    /// fan-out's own reason for that header.
    pub incomplete: bool,
}

/// A tenant record (RFC-002 §3.1): the scope every resource op is keyed
/// under.
///
/// `deleted` is a tombstone rather than a removed row: [`ControlOp::TenantDelete`]
/// leaves this record behind (see its `mutate_tables` arm) so the id's
/// history — and the fact that it once existed — survives the delete, the
/// same reason `sm_op_dedup` keeps entries instead of forgetting them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tenant {
    pub id: TenantId,
    pub display_name: String,
    pub quotas: Quotas,
    /// The replicated logical clock at creation (the applying entry's
    /// `issued_at_secs`) — never a local `SystemTime::now()`. Every replica
    /// applies the same committed [`ControlOp::TenantPut`] and must compute the
    /// identical record; a local clock read here would let them diverge, the
    /// same reasoning [`ControlRequest::issued_at_secs`]'s doc gives for dedup.
    pub created_at_secs: u64,
    pub deleted: bool,
    /// How long the M3 request shards keep this tenant's journal, in seconds;
    /// `0` = unlimited. See [`Quotas`]' doc for why it lives here rather than
    /// there (RFC-002 §11 open question 2).
    ///
    /// Stored now, applied by #147 — the shards are what that milestone builds,
    /// so there is nothing here to enforce it against yet. Defaulted so a
    /// `Tenant` written before this field existed still decodes.
    #[serde(default)]
    pub journal_retention_secs: u64,
}

/// How a principal authenticates (RFC-002 §3.2).
///
/// `ApiKey` carries an argon2id *hash*, never a raw key — [`validate`]'s
/// `PrincipalPut` arm refuses anything else. Admitting a raw key into the log
/// would put a live credential into every replica's redb file and every
/// snapshot, forever (there is no way to redact a committed log entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthSource {
    ApiKey { hash: String },
    Oidc { issuer: String, subject: String },
    MtlsSan { san: String },
}

/// A principal record (RFC-002 §3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Principal {
    pub id: PrincipalId,
    pub display_name: String,
    pub auth: AuthSource,
    pub disabled: bool,
}

/// The fast, non-secret index a raw API key resolves to (RFC-002 §3.2's
/// `PrincipalId` format, `"key:<fingerprint>"`): SHA-256 of the key, hex.
///
/// This is **not** the security boundary — [`verify_api_key`]'s argon2id
/// check is — it only lets a presented key find its principal row in one
/// lookup (issue #161) instead of a full-table scan. A collision here would
/// merely point two different keys at the same row to *attempt* verification
/// against; the argon2id compare downstream is what actually authenticates.
#[must_use]
pub fn api_key_fingerprint(raw: &str) -> String {
    use sha2::{Digest as _, Sha256};
    format!("{:x}", Sha256::digest(raw.as_bytes()))
}

/// The [`PrincipalId`] a raw API key resolves to. See [`api_key_fingerprint`].
#[must_use]
pub fn api_key_principal_id(raw: &str) -> PrincipalId {
    PrincipalId::new(format!("key:{}", api_key_fingerprint(raw)))
}

/// The argon2id cost this fleet issues keys at: the OWASP 2024 baseline,
/// m = 19456 KiB, t = 2, p = 1 (RFC-002 §8.2, issue #162).
///
/// Written out rather than taken from `Params::default()` even though the two
/// agree today. A cost parameter is the entire strength of a password hash and
/// it fails silently in both directions — too low and every stored hash is
/// cheaper to attack than anyone believes, too high and the memory cost
/// becomes a self-inflicted DoS — so the number a fleet actually runs at must
/// be visible in this file and asserted by a test, not inherited from whatever
/// a dependency's default happens to become at its next minor release.
///
/// Changing these does **not** invalidate stored hashes: the PHC string
/// records the parameters it was produced with, and [`verify_api_key`] reads
/// them from there, so old keys keep verifying at their original cost.
const ARGON2_M_COST_KIB: u32 = 19_456;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;

/// How many argon2id verifications this process has performed
/// ([`verify_api_key`]).
///
/// Exposed for one specific assertion (issue #162): a credential whose
/// fingerprint indexes no principal must answer `401` having hashed
/// **nothing**, because argon2id is deliberately expensive and an endpoint
/// anyone can reach must not be a memory-amplification lever. Counting is how
/// that is asserted — a timing assertion for the same property is flaky by
/// construction.
static ARGON2_VERIFICATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The running count of [`verify_api_key`] calls that reached the hash
/// comparison. See [`ARGON2_VERIFICATIONS`].
///
/// Hidden from the docs: this exists for one acceptance assertion, not as a
/// supported metric. `rift_cluster_*` on `/metrics` is where observable
/// counters live.
#[doc(hidden)]
#[must_use]
pub fn argon2_verifications() -> u64 {
    ARGON2_VERIFICATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// The pinned hasher. See [`ARGON2_M_COST_KIB`].
fn argon2() -> argon2::Argon2<'static> {
    use argon2::{Algorithm, Argon2, Params, Version};

    // `Params::new` rejects only out-of-range combinations, and these three
    // constants are in range by inspection — a failure here would be a typo in
    // this file, not anything a caller or an attacker can reach.
    let params = Params::new(ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, None)
        .expect("the pinned OWASP 2024 argon2id parameters are in range");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// Hash a raw API key for storage as an [`AuthSource::ApiKey`] (RFC-002 §8.2):
/// argon2id at the pinned [`ARGON2_M_COST_KIB`] cost, a fresh random salt per
/// call. The PHC string this returns is what [`validate_auth_source`] requires
/// and what [`verify_api_key`] checks against — the raw key itself is never
/// stored.
#[must_use]
pub fn hash_api_key(raw: &str) -> String {
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};

    let salt = SaltString::generate(&mut OsRng);
    // The only way this fails is an internal encoding bug in the hasher, not
    // anything about `raw` (argon2 has no length limit this crate approaches)
    // — an `expect` here names a defect in the algorithm, not attacker input.
    argon2()
        .hash_password(raw.as_bytes(), &salt)
        .expect("argon2id hashing does not fail for a well-formed salt")
        .to_string()
}

/// Verify a raw API key against a stored argon2id hash. `false` on anything
/// that is not a match, including a `stored_hash` that will not even parse as
/// a PHC string — a corrupt or foreign hash format must refuse, never panic
/// or read as a pass.
///
/// Counted in [`ARGON2_VERIFICATIONS`]. The counter is incremented only once
/// the hash has parsed, i.e. exactly when a real argon2id computation is about
/// to happen: an unparseable stored hash costs nothing and must not be counted
/// as though it did.
#[must_use]
pub fn verify_api_key(raw: &str, stored_hash: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};

    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    ARGON2_VERIFICATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // The stored PHC string carries the parameters this hash was produced
    // with, so verification runs at *its* cost, not the currently-pinned one —
    // which is what lets the pinned cost be raised without invalidating every
    // key already issued.
    argon2().verify_password(raw.as_bytes(), &parsed).is_ok()
}

/// The number of random bytes behind an issued API key. 32 bytes = 256 bits of
/// entropy, so the key is unguessable independently of the argon2id cost that
/// protects it at rest — the two defences are deliberately not the same
/// defence.
const API_KEY_RANDOM_BYTES: usize = 32;

/// The prefix every issued key carries (RFC-002 §8.2, issue #162).
///
/// Not a security property — it is a *leak-detection* one. A key that
/// announces what it is can be recognized on sight in a log, a pasted
/// snippet, or a secret scanner's ruleset; an opaque blob of base64 cannot.
pub const API_KEY_PREFIX: &str = "rift_";

/// Mint a fresh API key: [`API_KEY_PREFIX`] followed by
/// [`API_KEY_RANDOM_BYTES`] of OS randomness, URL-safe base64, unpadded.
///
/// The returned string is the **only** copy that will ever exist — the control
/// plane stores [`hash_api_key`]'s output and
/// [`api_key_principal_id`]'s fingerprint, neither of which can reproduce it.
/// A caller that does not hand it to the operator has destroyed it.
#[must_use]
pub fn generate_api_key() -> String {
    use argon2::password_hash::rand_core::{OsRng, RngCore as _};
    use base64::Engine as _;

    let mut bytes = [0u8; API_KEY_RANDOM_BYTES];
    OsRng.fill_bytes(&mut bytes);
    format!(
        "{API_KEY_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

/// A principal's binding to one tenant (RFC-002 §3.3): what [`ControlOp::BindingPut`]
/// stores.
///
/// `FleetAdmin` is meaningful only on the reserved [`FLEET_SCOPE`] — [`validate`]'s
/// `BindingPut` arm enforces the pairing in both directions, so a binding
/// naming `FleetAdmin` on an ordinary tenant, or naming any other role on
/// `"*"`, can never be committed.
///
/// Serializes lower-kebab (`tenant-admin`, `fleet-admin`, ...): this is a
/// wire enum an operator writes directly in an admin request body, unlike
/// `ControlOp`'s own snake_case fields, which are never hand-authored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    Viewer,
    Operator,
    Editor,
    TenantAdmin,
    FleetAdmin,
}

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
/// The `Tenant*`/`Principal*`/`Binding*` variants are RFC-002's multi-tenancy
/// and RBAC *records* (issue #159, RFC-002 §10 slice T1): they store tenants,
/// principals and role bindings, deterministically, like every other op here.
/// They do not enforce anything — no request is authorized against a
/// principal or a role anywhere in this crate yet. That is #161. Landing the
/// records first (this slice) and enforcement second means the wire format
/// and the replicated tables are stable before anything depends on them for
/// access control.
///
/// **Removing a variant is a log-format break, and #546 and #549 each took one
/// deliberately.** This enum is externally-tagged `serde_json` with no envelope
/// version and no `#[serde(other)]` catch-all — `raft::store` writes entries with
/// `serde_json::to_vec` and reads them back with `from_slice` — so a variant that
/// is gone here cannot be decoded at all: a node replaying a log that still holds
/// an `AuditSinkPut`, `AuditSinkDelete`, `AuditCheckpointPut`, `DatasetContentRead`,
/// `SourcePut`, `SourceDelete`, `SourcePullResult`, `SpecPut`, `SpecDelete`,
/// `SpecBind`, `SpecUnbind`, `DatasetPut` or `DatasetDelete` entry fails to start
/// rather than skipping it. Pre-release that is the right trade, and clean removal
/// is why it was taken; a fleet upgrading across this commit starts from a fresh
/// `cluster-state-dir`. A *post*-release removal would have to keep the variant as
/// an ignored arm.
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
    /// replace (upstream #817 semantics; cluster #15).
    SetEnabled {
        tenant: TenantId,
        port: u16,
        enabled: bool,
    },
    /// Whole-table replace of the front door's route table (issue #19 / U-11,
    /// cluster #131). Never a partial merge: [`RouteTable::validate`]
    /// checks the table as a unit (ambiguity is a property of the whole set),
    /// so admission must see — and apply must store — the whole thing.
    PutRoutes {
        tenant: TenantId,
        table: RouteTable,
    },
    /// Remove one route by id. Idempotent at the state-machine level, like
    /// [`ControlOp::DeleteImposter`] — see `mutate_tables`'s comment for why.
    DeleteRoute {
        tenant: TenantId,
        id: String,
    },
    /// Create or update a tenant record (RFC-002 §3.1). `display_name` and
    /// `quotas` are always replaced wholesale — a tenant has few enough
    /// fields that a partial-update op would be pure complexity for no real
    /// saving in payload size.
    TenantPut {
        tenant: TenantId,
        display_name: String,
        quotas: Quotas,
        /// See [`Tenant::journal_retention_secs`]. Defaulted so a `TenantPut`
        /// written before this field moved off [`Quotas`] still decodes — it
        /// lands as `0` (unlimited), which is what the old field's own default
        /// was.
        #[serde(default)]
        journal_retention_secs: u64,
    },
    /// Tombstone a tenant and cascade-remove its `sm_configs`/`sm_routes`
    /// rows, in the same committed op — see `mutate_tables`'s
    /// arm for why the cascade cannot be a separate op.
    TenantDelete {
        tenant: TenantId,
    },
    /// Create or update a principal's identity (RFC-002 §3).
    ///
    /// **Principals are a fleet-global namespace, and `tenant` does not scope
    /// them.** [`Principal`] rows are keyed by [`PrincipalId`] alone (see
    /// `SM_PRINCIPALS_TABLE`'s doc in `raft::store`); `tenant` is recorded for
    /// attribution and checked for liveness, nothing more. Two different tenants
    /// naming the same [`PrincipalId`] address the *same* record, so the
    /// second write replaces the first — including its credential.
    ///
    /// That is RFC-002 §3's model, not an oversight: only a `RoleBinding` is
    /// tenant-scoped. It is also exactly why the RFC makes `PrincipalPut` and
    /// `PrincipalDelete` **`FleetAdmin`-only**, while a `TenantAdmin` gets
    /// `BindingPut`/`BindingDelete` within its own tenant. Until #161 lands
    /// that rule there is no enforcement here — so do not read the `tenant`
    /// field as an isolation guarantee, because it is not one.
    PrincipalPut {
        tenant: TenantId,
        principal: Principal,
    },
    /// Mint a principal **and** its binding to `tenant` as one committed op
    /// (RFC-002 §5, issue #162) — what `POST /admin/tenants/:id/principals`
    /// becomes.
    ///
    /// Not sugar for a [`ControlOp::PrincipalPut`] followed by a
    /// [`ControlOp::BindingPut`]. Two ops are two revisions, and the gap
    /// between them is observable on every replica: a principal exists holding
    /// no binding (a credential that authenticates and is authorized for
    /// nothing), or — if the pair is ever reordered or the second op is lost to
    /// a leader change — a binding naming a principal that does not exist.
    /// Neither state is reachable through this op, which is the property the
    /// issue asks for and the reason the op exists.
    ///
    /// `role` may not be [`Role::FleetAdmin`]: this binds against `tenant`, and
    /// fleet privilege binds only on [`FLEET_SCOPE`]. Minting an identity
    /// inside a tenant must never be a way to grant authority outside it — see
    /// [`validate`]'s arm.
    PrincipalCreate {
        tenant: TenantId,
        principal: Principal,
        role: Role,
    },
    PrincipalDelete {
        tenant: TenantId,
        principal_id: PrincipalId,
    },
    /// Bind a principal to a role in a tenant (RFC-002 §3.3). `tenant` is
    /// [`FLEET_SCOPE`] only for a [`Role::FleetAdmin`] binding — [`validate`]
    /// enforces the pairing both ways.
    BindingPut {
        tenant: TenantId,
        principal_id: PrincipalId,
        role: Role,
    },
    BindingDelete {
        tenant: TenantId,
        principal_id: PrincipalId,
    },
    /// Mint or rotate the fleet's session-signing key (RFC-006 §5.3, issue #185).
    ///
    /// One key, fleet-wide, so every node verifies a console session cookie from its own applied
    /// state without asking a peer — which is what makes a login *not* a Raft write. Only minting
    /// and rotating are; the steady state is pure local verification.
    ///
    /// **This op deliberately carries a secret into the replicated log — the only one that
    /// does.** It is admissible because of what the secret means outside the fleet: this key is
    /// fleet-internal and meaningless anywhere else. It cannot be stored hashed the way a
    /// principal's API key is (`argon2id`, RFC-002 §3.2), because verifying an HMAC needs the key
    /// itself, not a one-way digest of it — a hash would make the cookie unverifiable by anyone,
    /// including us. A secret with power over a *third-party* system has no op that carries it:
    /// the credential-bearing source ops were removed with the tracking sources (#549, D-72).
    ///
    /// So it sits inside the same trust boundary as the state directory, which already holds every
    /// principal's argon2 record and all committed config. Rotation is the containment: writing a
    /// new key invalidates every outstanding session at once. Recorded in
    /// `docs/architecture/08-tenancy-security.md`.
    SessionKeyPut {
        tenant: TenantId,
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
        tenant: TenantId,
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
        tenant: TenantId,
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
        tenant: TenantId,
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
        tenant: TenantId,
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
            ControlOp::DeleteAll { .. } => "DeleteAll",
            ControlOp::SetEnabled { .. } => "SetEnabled",
            ControlOp::PutRoutes { .. } => "PutRoutes",
            ControlOp::DeleteRoute { .. } => "DeleteRoute",
            ControlOp::TenantPut { .. } => "TenantPut",
            ControlOp::TenantDelete { .. } => "TenantDelete",
            ControlOp::PrincipalPut { .. } => "PrincipalPut",
            ControlOp::PrincipalCreate { .. } => "PrincipalCreate",
            ControlOp::PrincipalDelete { .. } => "PrincipalDelete",
            ControlOp::BindingPut { .. } => "BindingPut",
            ControlOp::BindingDelete { .. } => "BindingDelete",
            ControlOp::SessionKeyPut { .. } => "SessionKeyPut",
            ControlOp::FleetNamePut { .. } => "FleetNamePut",
            ControlOp::JournalClearGen { .. } => "JournalClearGen",
            ControlOp::ProxyRecorded { .. } => "ProxyRecorded",
            ControlOp::ProxyRecordedClear { .. } => "ProxyRecordedClear",
        }
    }

    /// The tenant this op acts on. Every variant has one (#159).
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        match self {
            ControlOp::PutImposter { tenant, .. }
            | ControlOp::PatchStubs { tenant, .. }
            | ControlOp::DeleteImposter { tenant, .. }
            | ControlOp::DeleteAll { tenant }
            | ControlOp::SetEnabled { tenant, .. }
            | ControlOp::PutRoutes { tenant, .. }
            | ControlOp::DeleteRoute { tenant, .. }
            | ControlOp::TenantPut { tenant, .. }
            | ControlOp::TenantDelete { tenant }
            | ControlOp::PrincipalPut { tenant, .. }
            | ControlOp::PrincipalCreate { tenant, .. }
            | ControlOp::PrincipalDelete { tenant, .. }
            | ControlOp::BindingPut { tenant, .. }
            | ControlOp::BindingDelete { tenant, .. }
            | ControlOp::SessionKeyPut { tenant, .. }
            | ControlOp::FleetNamePut { tenant, .. }
            | ControlOp::JournalClearGen { tenant, .. }
            | ControlOp::ProxyRecorded { tenant, .. }
            | ControlOp::ProxyRecordedClear { tenant, .. } => tenant,
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
/// duplicate explicit stub ids), plus the cluster-only rules: an explicit port
/// (auto-assign cannot replicate — every node would pick a different port),
/// tenant-id shape, and the RFC-002 tenancy/RBAC rules below.
///
/// # What T1 does and does not make tenant-aware
///
/// The single-tenant gate is lifted here — every op now accepts any
/// well-formed tenant slug — but T1 (RFC-002 §10) delivers the tenancy
/// *records and their storage*, not tenant-aware serving. Concretely, a
/// resource op naming a non-`default` tenant is validated, committed and
/// stored against `(tenant, …)`, and its `TenantDelete` cascades over it — but
/// the read and sync paths (`desired_configs`, `desired_routes`,
/// `read_config`, `configured_ports`, `sources`, `config_provenance`) still
/// filter to `default`, so nothing binds it and no operator surface reports
/// it. **Storing is not serving in this slice.**
///
/// That is deliberate rather than an oversight, and it is why T1's exit
/// criterion — *no observable change* — still holds: the admin HTTP front
/// constructs `TenantId::default()` at every call site, so nothing reachable
/// over the API can create such a row. Only a direct `RiftNode::submit` can,
/// which is how the tenancy tests exercise the cascade and the fleet-wide port
/// rule that RFC-002 §3.2 requires.
///
/// One consequence to know before widening anything: because ports are
/// fleet-unique across tenants, a config stored for tenant A *does* claim its
/// port against tenant B — see `mutate_tables`' collision check. That is the
/// rule, not a bug, but it means the slice that makes serving tenant-aware
/// must land the read paths in the same PR, or an operator can be refused a
/// port that nothing is listening on and no read path reports as taken.
///
/// `Err` carries the reason recorded in the `Failed` outcome. It must depend
/// only on the op itself, never on per-node state — a tenant's *existence* is
/// state, so that check lives in `raft::store::mutate_tables` instead (see
/// its `PrincipalPut`/`BindingPut` arms), not here.
pub fn validate(op: &ControlOp) -> Result<(), String> {
    match op {
        ControlOp::PutImposter { tenant, config } => {
            require_real_tenant(tenant)?;
            validate_replicable_config(config)
        }
        ControlOp::PatchStubs { tenant, .. } | ControlOp::DeleteImposter { tenant, .. } => {
            require_real_tenant(tenant)
        }
        ControlOp::DeleteAll { tenant } => require_real_tenant(tenant),
        ControlOp::SetEnabled { tenant, .. } => require_real_tenant(tenant),
        ControlOp::PutRoutes { tenant, table } => {
            require_real_tenant(tenant)?;
            // The U-11 rules (unique ids, ambiguous enabled matches,
            // strip_prefix without path_prefix, malformed wildcard/method/
            // prefix) plus the whole-table atomicity the issue calls for: a
            // table is accepted or refused as a unit, never partially.
            table.validate().map_err(|e| e.to_string())
        }
        // A delete removes one route from an already-validated table.
        // Ambiguity is pairwise, so removing an element can only shrink the
        // set of matching pairs, never create one — the remaining table is
        // structurally guaranteed valid, so there is nothing to check here
        // beyond the tenant shape.
        ControlOp::DeleteRoute { tenant, .. } => require_real_tenant(tenant),
        ControlOp::TenantPut {
            tenant,
            display_name,
            quotas,
            journal_retention_secs: _,
        } => {
            require_real_tenant(tenant)?;
            if display_name.trim().is_empty() {
                return Err("tenant display_name must not be empty".to_owned());
            }
            // A zero ceiling is refused rather than stored (#163). It is
            // *representable* and it is almost never what anyone means: it
            // makes the tenant permanently unable to hold a single imposter,
            // and the operator finds out later, from a write that fails for a
            // reason they will not connect to a quota they set. "Unlimited" has
            // its own spelling — a large number — so a zero here is a typo far
            // more often than an intention.
            if quotas.max_imposters == 0 {
                return Err(
                    "maxImposters must be at least 1: a ceiling of 0 makes the tenant unusable"
                        .to_owned(),
                );
            }
            if quotas.max_stubs_per_imposter == 0 {
                return Err(
                    "maxStubsPerImposter must be at least 1: a ceiling of 0 refuses every imposter"
                        .to_owned(),
                );
            }
            Ok(())
        }
        ControlOp::TenantDelete { tenant } => {
            require_real_tenant(tenant)?;
            if tenant.is_default() {
                return Err(
                    "the default tenant cannot be deleted: it is the fleet's always-present \
                     scope for an unscoped request"
                        .to_owned(),
                );
            }
            Ok(())
        }
        ControlOp::PrincipalPut { tenant, principal } => {
            require_real_tenant(tenant)?;
            require_principal_id(&principal.id)?;
            if principal.display_name.trim().is_empty() {
                return Err("principal display_name must not be empty".to_owned());
            }
            validate_auth_source(&principal.auth)
        }
        ControlOp::PrincipalCreate {
            tenant,
            principal,
            role,
        } => {
            require_real_tenant(tenant)?;
            require_principal_id(&principal.id)?;
            if principal.display_name.trim().is_empty() {
                return Err("principal display_name must not be empty".to_owned());
            }
            // The binding half of this op targets `tenant`, never
            // `FLEET_SCOPE` — so a `FleetAdmin` role here would be a binding
            // `BindingPut` itself refuses (see its arm below), reached through
            // a different door. Refused for the same reason and with the same
            // wording, rather than silently downgraded: an operator who asked
            // for fleet privilege must learn they did not get it.
            if matches!(role, Role::FleetAdmin) {
                return Err(format!(
                    "fleet-admin may only be bound on the reserved fleet scope {FLEET_SCOPE:?}, \
                     not tenant {:?}",
                    tenant.as_str()
                ));
            }
            validate_auth_source(&principal.auth)
        }
        ControlOp::PrincipalDelete {
            tenant,
            principal_id,
        } => {
            require_real_tenant(tenant)?;
            require_principal_id(principal_id)
        }
        ControlOp::BindingPut {
            tenant,
            principal_id,
            role,
        } => {
            require_tenant_or_fleet_scope(tenant)?;
            require_principal_id(principal_id)?;
            let is_fleet_scope = tenant.as_str() == FLEET_SCOPE;
            let is_fleet_admin = matches!(role, Role::FleetAdmin);
            if is_fleet_scope && !is_fleet_admin {
                Err(format!(
                    "role {role:?} is not valid on the reserved fleet scope {FLEET_SCOPE:?}: \
                     only fleet-admin binds there"
                ))
            } else if !is_fleet_scope && is_fleet_admin {
                Err(format!(
                    "fleet-admin may only be bound on the reserved fleet scope {FLEET_SCOPE:?}, \
                     not tenant {:?}",
                    tenant.as_str()
                ))
            } else {
                Ok(())
            }
        }
        ControlOp::BindingDelete {
            tenant,
            principal_id,
        } => {
            require_tenant_or_fleet_scope(tenant)?;
            require_principal_id(principal_id)
        }
        ControlOp::SessionKeyPut { tenant, key } => {
            require_fleet_scope(tenant)?;
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
        ControlOp::FleetNamePut { tenant, name } => {
            require_fleet_scope(tenant)?;
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
        // Deliberately shallow: only the checks that hold regardless of state. Whether `tenant`
        // exists and whether it owns `port` are apply-time questions (`raft::store::mutate_tables`'
        // `JournalClearGen` arm) — the same split every other op here draws, and the reason is the
        // same too: `validate` runs identically on every replica from the op alone, so it must
        // never depend on a table a replica could disagree with another about.
        ControlOp::JournalClearGen {
            tenant,
            port,
            space,
        } => {
            require_real_tenant(tenant)?;
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
            tenant,
            port,
            sig_hash,
            resp,
            stub,
        } => {
            require_real_tenant(tenant)?;
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
        ControlOp::ProxyRecordedClear { tenant, port } => {
            require_real_tenant(tenant)?;
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

/// The session-signing key and the fleet name each address one fleet-wide piece of state, so
/// the fleet scope is the only tenant either op may carry.
///
/// Refused rather than tolerated: `ControlOp::tenant` means "the tenant the op acted on", and
/// admitting a fleet rename under `tenant: "acme"` would file a fleet-wide configuration change
/// under one tenant's name in the log that is the record of every change.
fn require_fleet_scope(tenant: &TenantId) -> Result<(), String> {
    if tenant.as_str() == FLEET_SCOPE {
        Ok(())
    } else {
        Err(format!(
            "this op addresses fleet-wide state, so it carries the reserved fleet scope \
             {FLEET_SCOPE:?}, not tenant {:?}",
            tenant.as_str()
        ))
    }
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

/// A tenant id's wire shape (RFC-002 §3.1): `[a-z0-9][a-z0-9-]{0,63}`.
///
/// Deliberately narrower than an arbitrary UTF-8 string: a tenant id is a
/// redb key component today and an admin-surface path segment as soon as
/// #161 exposes one, so it is restricted to the safe subset once, here,
/// rather than escaped at every future use site.
fn is_tenant_slug(id: &str) -> bool {
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && id.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn require_tenant_slug(tenant: &TenantId) -> Result<(), String> {
    if is_tenant_slug(tenant.as_str()) {
        Ok(())
    } else {
        Err(format!(
            "tenant id {:?} must match [a-z0-9][a-z0-9-]{{0,63}}",
            tenant.as_str()
        ))
    }
}

/// A well-formed tenant id that is also a *real* tenant scope: every op
/// except a binding may target [`FLEET_SCOPE`] (`"*"`) — there is no tenant
/// record there, ever, so an op that would create or address one must refuse
/// it up front rather than let it silently succeed against nothing.
fn require_real_tenant(tenant: &TenantId) -> Result<(), String> {
    require_tenant_slug(tenant)?;
    if tenant.as_str() == FLEET_SCOPE {
        Err(format!(
            "{FLEET_SCOPE:?} is the reserved fleet-wide scope, not a tenant: it names no \
             TenantPut record and never will"
        ))
    } else {
        Ok(())
    }
}

/// Accepts a well-formed tenant slug OR [`FLEET_SCOPE`] — the two valid
/// binding targets (RFC-002 §3.3): an ordinary tenant, or the fleet-wide
/// scope [`Role::FleetAdmin`] binds against. Which roles are valid for which
/// of the two is a separate, op-specific check — see `validate`'s
/// `BindingPut` arm.
fn require_tenant_or_fleet_scope(tenant: &TenantId) -> Result<(), String> {
    if tenant.as_str() == FLEET_SCOPE {
        Ok(())
    } else {
        require_tenant_slug(tenant)
    }
}

/// Whether `id` is usable as a [`PrincipalId`]: a bounded, control-character-free
/// token. Wider than the `[A-Za-z0-9._-]` charset an operator-chosen id takes — a
/// principal id may come
/// from an external identity provider (an OIDC `subject`, say) rather than be
/// operator-chosen — but still bounded, because it is a redb key component.
fn require_principal_id(id: &PrincipalId) -> Result<(), String> {
    let s = id.as_str();
    if !s.is_empty() && s.len() <= 256 && s.chars().all(|c| !c.is_control()) {
        Ok(())
    } else {
        Err(
            "principal id must be a non-empty string of at most 256 bytes with no control \
             characters"
                .to_owned(),
        )
    }
}

/// The credential-hygiene rule for principals, mirroring
/// [`require_credential_free_uri`]'s role for sources: a raw API key must
/// never enter the replicated log, so only an already-hashed argon2id
/// credential is accepted — recognizable by the `$argon2id$` prefix argon2's
/// own PHC-string encoder produces. There is no way to tell a raw key from an
/// unknown hash format by inspection alone, so anything without that prefix
/// is refused rather than guessed at.
fn validate_auth_source(auth: &AuthSource) -> Result<(), String> {
    match auth {
        AuthSource::ApiKey { hash } => {
            const PREFIX: &str = "$argon2id$";
            if hash.is_empty() {
                Err("principal auth hash must not be empty".to_owned())
            } else if !hash.starts_with(PREFIX) {
                Err(format!(
                    "principal auth hash must be an argon2id encoded hash (starting with \
                     {PREFIX:?}), never a raw key"
                ))
            } else {
                Ok(())
            }
        }
        AuthSource::Oidc { issuer, subject } => {
            if issuer.trim().is_empty() || subject.trim().is_empty() {
                Err("oidc auth must carry a non-empty issuer and subject".to_owned())
            } else {
                Ok(())
            }
        }
        AuthSource::MtlsSan { san } => {
            if san.trim().is_empty() {
                Err("mtls_san auth must carry a non-empty san".to_owned())
            } else {
                Ok(())
            }
        }
    }
}

/// What stored record an `expected_revision` precondition holds against.
///
/// Two shapes, because the control plane has two things worth conditioning on
/// and they are keyed differently: a single imposter row in `sm_configs`, and a
/// tenant's front-door route table as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionTarget<'a> {
    /// The `sm_configs` row at `(tenant, port)`; its revision is the record's
    /// own `StoredImposter::revision`.
    Imposter(&'a TenantId, u16),
    /// `tenant`'s whole route table (issue #210); its revision is the
    /// `sm_routes_revision` row, absent meaning 0.
    ///
    /// Table-wide and not per-route on purpose: `PutRoutes` replaces the set as
    /// a unit, so the only thing a client can meaningfully condition a replace
    /// on is the state of the set it read. `DeleteRoute` stamps the same
    /// revision — a delete mutates the table, so it must invalidate every
    /// outstanding precondition against it, or a client that read before the
    /// delete could replace the table wholesale after it and silently restore
    /// the deleted route.
    RouteTable(&'a TenantId),
}

/// The record `op`'s `expected_revision` addresses, or `None` if `op` has no
/// such target (a bulk op, or a reserved RFC-002 variant). Used by the state
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
pub fn precondition_target(op: &ControlOp) -> Option<PreconditionTarget<'_>> {
    match op {
        // `config.port` is validated to be present before this ever matters,
        // but a `None` here must still yield `None`, not a bogus target.
        ControlOp::PutImposter { tenant, config } => config
            .port
            .map(|port| PreconditionTarget::Imposter(tenant, port)),
        ControlOp::PatchStubs { tenant, port, .. }
        | ControlOp::DeleteImposter { tenant, port }
        | ControlOp::SetEnabled { tenant, port, .. } => {
            Some(PreconditionTarget::Imposter(tenant, *port))
        }
        // Both route ops condition on — and stamp — the one per-tenant table
        // revision. A per-route precondition would be a different feature and
        // needs a single-route upsert op to hang off; #210 deliberately does
        // not add one.
        ControlOp::PutRoutes { tenant, .. } | ControlOp::DeleteRoute { tenant, .. } => {
            Some(PreconditionTarget::RouteTable(tenant))
        }
        ControlOp::DeleteAll { .. }
        | ControlOp::TenantPut { .. }
        | ControlOp::TenantDelete { .. }
        | ControlOp::PrincipalPut { .. }
        | ControlOp::PrincipalCreate { .. }
        | ControlOp::PrincipalDelete { .. }
        | ControlOp::BindingPut { .. }
        | ControlOp::BindingDelete { .. }
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

    /// A well-formed argon2id hash shape (RFC-002 §3.2). Not a real hash of
    /// anything — `validate` only ever checks the PHC-string prefix, never
    /// verifies a password against it — so a fixed placeholder is enough to
    /// stand in wherever a valid one is needed.
    const VALID_ARGON2_HASH: &str =
        "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$RdescudvJCsgt3ub+b+dWRWJTmaaJObG";

    fn test_principal(id: &str) -> Principal {
        Principal {
            id: PrincipalId::new(id),
            display_name: id.to_owned(),
            auth: AuthSource::ApiKey {
                hash: VALID_ARGON2_HASH.to_owned(),
            },
            disabled: false,
        }
    }

    // -- API key hashing / verification (issue #161) ---------------------------

    #[test]
    fn hash_api_key_produces_a_verifiable_argon2id_hash() {
        let hash = hash_api_key("s3cr3t-key");
        assert!(
            hash.starts_with("$argon2id$"),
            "must satisfy validate_auth_source's prefix check: {hash}"
        );
        assert!(verify_api_key("s3cr3t-key", &hash));
        assert!(
            !verify_api_key("wrong-key", &hash),
            "a different key must not verify"
        );
    }

    #[test]
    fn hash_api_key_salts_per_call() {
        // Two hashes of the same key must differ (random salt) but both verify.
        let a = hash_api_key("same-key");
        let b = hash_api_key("same-key");
        assert_ne!(a, b, "argon2id must not reuse a salt across calls");
        assert!(verify_api_key("same-key", &a));
        assert!(verify_api_key("same-key", &b));
    }

    #[test]
    fn verify_api_key_fails_closed_on_a_corrupt_hash() {
        assert!(!verify_api_key("any-key", "not a phc string"));
        assert!(!verify_api_key("any-key", ""));
    }

    #[test]
    fn api_key_fingerprint_is_deterministic_and_key_sensitive() {
        assert_eq!(api_key_fingerprint("a"), api_key_fingerprint("a"));
        assert_ne!(api_key_fingerprint("a"), api_key_fingerprint("b"));
        assert_eq!(
            api_key_principal_id("a").as_str(),
            format!("key:{}", api_key_fingerprint("a"))
        );
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
            (
                ControlOp::PutRoutes {
                    tenant: TenantId::default(),
                    table: RouteTable::default(),
                },
                "PutRoutes",
            ),
            (
                ControlOp::DeleteRoute {
                    tenant: TenantId::default(),
                    id: "r".to_owned(),
                },
                "DeleteRoute",
            ),
            (
                ControlOp::TenantPut {
                    tenant: TenantId::new("acme"),
                    display_name: "Acme Corp".to_owned(),
                    quotas: Quotas::default(),
                    journal_retention_secs: 0,
                },
                "TenantPut",
            ),
            (
                ControlOp::TenantDelete {
                    tenant: TenantId::new("acme"),
                },
                "TenantDelete",
            ),
            (
                ControlOp::PrincipalPut {
                    tenant: TenantId::new("acme"),
                    principal: test_principal("alice"),
                },
                "PrincipalPut",
            ),
            (
                ControlOp::PrincipalDelete {
                    tenant: TenantId::new("acme"),
                    principal_id: PrincipalId::new("alice"),
                },
                "PrincipalDelete",
            ),
            (
                ControlOp::BindingPut {
                    tenant: TenantId::new("acme"),
                    principal_id: PrincipalId::new("alice"),
                    role: Role::Editor,
                },
                "BindingPut",
            ),
            (
                ControlOp::BindingDelete {
                    tenant: TenantId::new("acme"),
                    principal_id: PrincipalId::new("alice"),
                },
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

    /// RFC-002 §10 T1 lifts the single-tenant gate: a resource op now accepts
    /// any well-formed tenant slug, not just `"default"`.
    #[test]
    fn validate_accepts_a_well_formed_non_default_tenant() {
        let op = ControlOp::DeleteAll {
            tenant: TenantId::new("acme"),
        };
        assert_eq!(validate(&op), Ok(()));
    }

    #[test]
    fn validate_rejects_a_malformed_tenant_id() {
        for id in [
            "",
            "Acme",
            "-acme",
            "acme_corp",
            "acme.corp",
            &"a".repeat(65),
        ] {
            let op = ControlOp::DeleteAll {
                tenant: TenantId::new(id),
            };
            let err = validate(&op).expect_err("malformed tenant id must be rejected");
            assert!(err.contains("tenant"), "id {id:?}: {err}");
        }
    }

    /// [`FLEET_SCOPE`] is the reserved fleet-wide scope, never a real tenant:
    /// no op that addresses a tenant record may target it.
    #[test]
    fn validate_rejects_the_fleet_scope_as_a_real_tenant() {
        let op = ControlOp::DeleteAll {
            tenant: TenantId::new(FLEET_SCOPE),
        };
        let err = validate(&op).expect_err("the fleet scope is not a tenant");
        assert!(err.contains(FLEET_SCOPE), "{err}");
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
    fn validate_accepts_set_enabled_on_any_well_formed_tenant() {
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
        assert_eq!(validate(&op), Ok(()));
    }

    // -- validate: JournalClearGen (issue #224) --------------------------------

    #[test]
    fn validate_rejects_a_journal_clear_on_port_zero() {
        let op = ControlOp::JournalClearGen {
            tenant: TenantId::default(),
            port: 0,
            space: None,
        };
        let err = validate(&op).expect_err("port 0 addresses no imposter");
        assert!(err.contains("port"), "{err}");
    }

    #[test]
    fn validate_rejects_a_journal_clear_on_an_empty_space() {
        let op = ControlOp::JournalClearGen {
            tenant: TenantId::default(),
            port: 1,
            space: Some(String::new()),
        };
        let err = validate(&op).expect_err("an empty space is not a narrower clear");
        assert!(err.contains("space"), "{err}");
    }

    #[test]
    fn validate_accepts_a_well_formed_journal_clear_for_both_scopes() {
        let port_wide = ControlOp::JournalClearGen {
            tenant: TenantId::default(),
            port: 1,
            space: None,
        };
        assert_eq!(validate(&port_wide), Ok(()));

        let space_scoped = ControlOp::JournalClearGen {
            tenant: TenantId::new("acme"),
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
            tenant: TenantId::default(),
            table: RouteTable {
                routes: vec![route("a", 1)],
            },
        };
        assert_eq!(validate(&op), Ok(()));
    }

    #[test]
    fn validate_accepts_put_routes_on_any_well_formed_tenant() {
        let op = ControlOp::PutRoutes {
            tenant: TenantId::new("acme"),
            table: RouteTable::default(),
        };
        assert_eq!(validate(&op), Ok(()));
    }

    #[test]
    fn validate_rejects_put_routes_on_a_malformed_tenant() {
        let op = ControlOp::PutRoutes {
            tenant: TenantId::new("Not Valid"),
            table: RouteTable::default(),
        };
        let err = validate(&op).expect_err("malformed tenant id must be rejected");
        assert!(err.contains("tenant"), "{err}");
    }

    #[test]
    fn validate_rejects_a_route_table_with_duplicate_ids() {
        let op = ControlOp::PutRoutes {
            tenant: TenantId::default(),
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
            tenant: TenantId::default(),
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
            tenant: TenantId::default(),
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
            tenant: TenantId::default(),
            table: RouteTable { routes: vec![bad] },
        };
        let err = validate(&op).expect_err("a malformed wildcard host must be rejected");
        assert!(err.contains("wildcard"), "{err}");
    }

    #[test]
    fn validate_accepts_delete_route_on_any_well_formed_tenant() {
        let op = ControlOp::DeleteRoute {
            tenant: TenantId::default(),
            id: "any".to_owned(),
        };
        assert_eq!(validate(&op), Ok(()));

        let op = ControlOp::DeleteRoute {
            tenant: TenantId::new("acme"),
            id: "any".to_owned(),
        };
        assert_eq!(validate(&op), Ok(()));
    }

    // -- validate: tenancy and RBAC records (issue #159, RFC-002 §10 T1) -------

    fn tenant_put(id: &str) -> ControlOp {
        ControlOp::TenantPut {
            tenant: TenantId::new(id),
            display_name: "Acme Corp".to_owned(),
            quotas: Quotas::default(),
            journal_retention_secs: 0,
        }
    }

    #[test]
    fn validate_accepts_a_well_formed_tenant_put() {
        assert_eq!(validate(&tenant_put("acme")), Ok(()));
    }

    #[test]
    fn validate_rejects_a_tenant_put_with_an_empty_display_name() {
        let op = ControlOp::TenantPut {
            tenant: TenantId::new("acme"),
            display_name: "   ".to_owned(),
            quotas: Quotas::default(),
            journal_retention_secs: 0,
        };
        let err = validate(&op).expect_err("an empty display name names nothing");
        assert!(err.contains("display_name"), "{err}");
    }

    /// The fleet's always-present, unscoped-request tenant must never become
    /// deletable — there would be nowhere left for a pre-#159 request to land.
    #[test]
    fn validate_rejects_deleting_the_default_tenant() {
        let op = ControlOp::TenantDelete {
            tenant: TenantId::default(),
        };
        let err = validate(&op).expect_err("the default tenant must never be deletable");
        assert!(err.contains("default"), "{err}");
    }

    #[test]
    fn validate_accepts_deleting_a_non_default_tenant() {
        let op = ControlOp::TenantDelete {
            tenant: TenantId::new("acme"),
        };
        assert_eq!(validate(&op), Ok(()));
    }

    #[test]
    fn validate_accepts_a_well_formed_principal_put() {
        let op = ControlOp::PrincipalPut {
            tenant: TenantId::new("acme"),
            principal: test_principal("alice"),
        };
        assert_eq!(validate(&op), Ok(()));
    }

    /// The secret-hygiene rule for principals, mirroring the source-uri rule
    /// above: a raw API key must never be admitted into the replicated log.
    #[test]
    fn validate_rejects_a_principal_put_carrying_a_raw_key_instead_of_a_hash() {
        let op = ControlOp::PrincipalPut {
            tenant: TenantId::new("acme"),
            principal: Principal {
                auth: AuthSource::ApiKey {
                    hash: "rift_live_sk_notahash".to_owned(),
                },
                ..test_principal("alice")
            },
        };
        let err = validate(&op).expect_err("a raw key is not an argon2id hash");
        assert!(err.contains("argon2id"), "{err}");
    }

    #[test]
    fn validate_rejects_a_principal_put_with_an_empty_hash() {
        let op = ControlOp::PrincipalPut {
            tenant: TenantId::new("acme"),
            principal: Principal {
                auth: AuthSource::ApiKey {
                    hash: String::new(),
                },
                ..test_principal("alice")
            },
        };
        let err = validate(&op).expect_err("an empty hash names no credential");
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn validate_rejects_an_unusable_principal_id() {
        let op = ControlOp::PrincipalPut {
            tenant: TenantId::new("acme"),
            principal: Principal {
                id: PrincipalId::new(""),
                ..test_principal("alice")
            },
        };
        let err = validate(&op).expect_err("an empty principal id addresses nothing");
        assert!(err.contains("principal id"), "{err}");
    }

    fn binding_put(tenant: &str, role: Role) -> ControlOp {
        ControlOp::BindingPut {
            tenant: TenantId::new(tenant),
            principal_id: PrincipalId::new("alice"),
            role,
        }
    }

    #[test]
    fn validate_accepts_fleet_admin_only_on_the_fleet_scope() {
        assert_eq!(
            validate(&binding_put(FLEET_SCOPE, Role::FleetAdmin)),
            Ok(())
        );
    }

    /// `Role::FleetAdmin` bound on an ordinary tenant would let a tenant-scoped
    /// principal act with fleet-wide power — the exact escalation RFC-002 §3.3
    /// exists to prevent.
    #[test]
    fn validate_rejects_fleet_admin_on_an_ordinary_tenant() {
        let err = validate(&binding_put("payments", Role::FleetAdmin))
            .expect_err("fleet-admin must never bind on an ordinary tenant");
        assert!(err.contains("fleet-admin"), "{err}");
    }

    /// The converse: every non-fleet-admin role is meaningless on the
    /// fleet-wide scope, so it is refused rather than silently accepted.
    #[test]
    fn validate_rejects_a_non_fleet_admin_role_on_the_fleet_scope() {
        for role in [
            Role::Viewer,
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
        ] {
            let err = validate(&binding_put(FLEET_SCOPE, role))
                .expect_err("only fleet-admin binds on the fleet scope");
            assert!(err.contains(FLEET_SCOPE), "{role:?}: {err}");
        }
    }

    #[test]
    fn validate_accepts_an_ordinary_role_on_an_ordinary_tenant() {
        for role in [
            Role::Viewer,
            Role::Operator,
            Role::Editor,
            Role::TenantAdmin,
        ] {
            assert_eq!(validate(&binding_put("payments", role)), Ok(()), "{role:?}");
        }
    }

    /// Every wire role value, locked to lower-kebab: this is what an operator
    /// writes in an admin request body, so a spelling drift here is a wire
    /// break for every existing client.
    #[test]
    fn every_role_spelling_is_stable_lower_kebab() {
        for (role, spelling) in [
            (Role::Viewer, "viewer"),
            (Role::Operator, "operator"),
            (Role::Editor, "editor"),
            (Role::TenantAdmin, "tenant-admin"),
            (Role::FleetAdmin, "fleet-admin"),
        ] {
            let value = serde_json::to_value(role).expect("serialize");
            assert_eq!(value, json!(spelling));
            let back: Role = serde_json::from_value(value).expect("round-trips");
            assert_eq!(back, role);
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
            tenant: TenantId::new(FLEET_SCOPE),
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

    #[test]
    fn validate_rejects_a_tenant_scoped_fleet_name_write() {
        // One fleet, one name: a tenant admin sending `X-Rift-Tenant: acme` must not be able to
        // rename the cluster every other tenant is also looking at.
        let op = ControlOp::FleetNamePut {
            tenant: TenantId::new("acme"),
            name: "acme-only".to_owned(),
        };
        let err = validate(&op).expect_err("a tenant-scoped fleet rename must be rejected");
        assert!(err.contains(FLEET_SCOPE), "{err}");
    }
}
