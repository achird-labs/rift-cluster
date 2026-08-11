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
/// The `Tenant*`/`Principal*`/`Binding*` variants are RFC-002's multi-tenancy
/// and RBAC *records* (issue #159, RFC-002 §10 slice T1): they store tenants,
/// principals and role bindings, deterministically, like every other op here.
/// They do not enforce anything — no request is authorized against a
/// principal or a role anywhere in this crate yet. That is #161. Landing the
/// records first (this slice) and enforcement second means the wire format
/// and the replicated tables are stable before anything depends on them for
/// access control.
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
    /// Create or replace an imposter source (issue #134 / #20 B). A source is a
    /// durable control-plane object: the fleet agrees on which URI backs which
    /// imposters, and every node can answer for it without asking a peer.
    SourcePut {
        tenant: TenantId,
        id: String,
        uri: String,
        mode: SourceMode,
        /// The *name* of a credential, never a credential. Resolution ships with
        /// the providers that need it (#136); a URI carrying embedded
        /// credentials is refused by [`validate`] so a secret can never enter
        /// the replicated log.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_ref: Option<String>,
        on_drift: OnDrift,
        /// How often the leader re-fetches a [`SourceMode::Tracking`] source,
        /// in seconds (issue #135). Required for `tracking`, refused for
        /// `pinned` — a poll interval on a source nobody polls is a setting
        /// that silently does nothing.
        ///
        /// Defaulted so a `SourcePut` written before #135 still decodes; such
        /// an entry is necessarily `pinned`, which is exactly what `None`
        /// means.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        poll_secs: Option<u64>,
    },
    /// Forget a source. Its imposters stay bound and lose their provenance —
    /// see `mutate_tables`' arm for why deleting them would be the wrong
    /// default.
    SourceDelete {
        tenant: TenantId,
        id: String,
    },
    /// The outcome of one fetch, submitted by whichever node performed it
    /// (issue #134). This is the whole reason fetching is not in the apply path:
    /// apply must be deterministic and infallible, and two nodes fetching the
    /// same URI can get different bytes. The fetcher canonicalizes and hashes
    /// what it got, and *this* op — an ordinary validated write — is what every
    /// replica applies identically.
    SourcePullResult {
        tenant: TenantId,
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
        digest: Digest,
        configs: Vec<ImposterConfig>,
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
    /// Tombstone a tenant and cascade-remove its `sm_configs`/`sm_routes`/
    /// `sm_sources` rows, in the same committed op — see `mutate_tables`'s
    /// arm for why the cascade cannot be a separate op.
    TenantDelete {
        tenant: TenantId,
    },
    /// Create or update a principal's identity (RFC-002 §3).
    ///
    /// **Principals are a fleet-global namespace, and `tenant` does not scope
    /// them.** [`Principal`] rows are keyed by [`PrincipalId`] alone (see
    /// `SM_PRINCIPALS_TABLE`'s doc in `raft::store`); `tenant` is recorded for
    /// audit and checked for liveness, nothing more. Two different tenants
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
    /// Declare (or replace) the fleet's audit export sink (#164).
    ///
    /// Fleet state rather than node config, so every node agrees on where the
    /// audit stream goes and a node joining inherits it. `auth_ref` is the
    /// *name* of a credential and never a credential — the same split
    /// [`ControlOp::SourcePut`] enforces, using the same
    /// `require_credential_free_uri` check, so a secret cannot enter the
    /// replicated log by either door.
    ///
    /// One sink, fleet-wide. A per-tenant sink would need a checkpoint per
    /// tenant; the checkpoint here is a single revision, and that is the whole
    /// mechanism by which a failover resumes rather than restarting.
    AuditSinkPut {
        tenant: TenantId,
        uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_ref: Option<String>,
        /// Rows per shipped batch. Bounded by [`validate`].
        batch_max_rows: u32,
    },
    /// Remove the audit export sink; the exporter goes quiet without losing its
    /// checkpoint, so re-declaring a sink resumes rather than re-ships history.
    AuditSinkDelete {
        tenant: TenantId,
    },
    /// The last revision the leader has successfully shipped (#164).
    ///
    /// Committed *after* the batch is on the wire, never before — that ordering
    /// is the at-least-once guarantee: a leader dying in between re-ships the
    /// batch, and the consumer dedups on `(revision, op_id)`.
    ///
    /// Applied as `max(existing, new)` so a stale leader's late write cannot
    /// rewind the stream. Deliberately **not** audited — see
    /// [`ControlOp::audit_action`].
    AuditCheckpointPut {
        tenant: TenantId,
        revision: u64,
    },
    /// Mint or rotate the fleet's session-signing key (RFC-006 §5.3, issue #185).
    ///
    /// One key, fleet-wide, so every node verifies a console session cookie from its own applied
    /// state without asking a peer — which is what makes a login *not* a Raft write. Only minting
    /// and rotating are; the steady state is pure local verification.
    ///
    /// **This op deliberately carries a secret into the replicated log, which
    /// [`ControlOp::SourcePut`] and [`ControlOp::AuditSinkPut`] both refuse to do.** The
    /// distinction is what the secret means outside the fleet:
    ///
    /// - Those ops carry the *name* of an operator credential for a third-party system (an S3
    ///   bucket, a webhook). Replicating one would spread a credential that has power somewhere
    ///   else, so `validate` refuses it and only a reference travels.
    /// - This key is fleet-internal and meaningless anywhere else. It cannot be stored hashed the
    ///   way a principal's API key is (`argon2id`, RFC-002 §3.2), because verifying an HMAC needs
    ///   the key itself, not a one-way digest of it — a hash would make the cookie unverifiable by
    ///   anyone, including us.
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
    JournalClearGen {
        tenant: TenantId,
        port: u16,
        space: Option<String>,
    },
    /// One `proxyOnce`/`proxyAlways` recording, as consensus fact (#226, Ch.7 §proxyOnce).
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

/// The fleet's audit export sink, as applied state (#164).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditSink {
    /// `https://` (webhook, JSON lines) or `s3://` (bucket, batched objects).
    /// Never carries credentials — [`validate`] refuses a URI whose authority
    /// has any.
    pub uri: String,
    /// The *name* of a node-local credential, resolved at export time. Safe to
    /// serve back over the admin API precisely because it is not a secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<String>,
    pub batch_max_rows: u32,
    /// The revision of the `AuditSinkPut` that produced this record.
    pub revision: u64,
}

/// Rows per shipped batch when an operator does not choose. Small enough that a
/// re-ship after a failover is cheap, large enough that a busy fleet is not
/// shipping one row per request.
pub const DEFAULT_AUDIT_BATCH_MAX_ROWS: u32 = 500;

/// Ceiling on `batch_max_rows`. A batch is buffered in memory and shipped as
/// one request; an unbounded value is an operator-set OOM.
pub const MAX_AUDIT_BATCH_MAX_ROWS: u32 = 10_000;

/// How a source is kept current.
///
/// `Tracking` is in the log format now so #135 does not need a wire break, but
/// [`validate`] refuses it until the leader-only poll scheduler exists — a
/// source that claims to track and does not would be worse than one that never
/// promised to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMode {
    /// Explicit pulls only.
    #[default]
    Pinned,
    /// Scheduled polls (#135).
    Tracking,
}

/// What a pull does when the source's imposters have been edited by hand since
/// it last applied.
///
/// The default is `Overwrite` — the source is the declared truth, which is
/// Solo's semantics — but it is *declared* rather than assumed, and readable
/// from `GET /admin/sources/:id`, so an operator can see which way their fleet
/// will go before it goes there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDrift {
    #[default]
    Overwrite,
    Skip,
    Fail,
}

impl std::fmt::Display for OnDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Overwrite => "overwrite",
            Self::Skip => "skip",
            Self::Fail => "fail",
        })
    }
}

/// A content digest of a fetched config set: lowercase hex SHA-256 over the
/// canonical encoding in [`crate::sources::digest_of`].
///
/// Opaque on purpose. The only questions anyone asks of it are "is this the
/// same content as last time" and "what do I show an operator", so exposing it
/// as a bare `String` would invite comparing it against something that is not a
/// digest of the same canonical form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Digest(String);

impl Digest {
    #[must_use]
    pub fn new(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which source a stored config came from, stamped per config when a pull
/// applies. This is what makes drift detection deterministic: provenance lives
/// in the replicated state machine, so every replica reaches the same verdict
/// about whether a manual edit touched source-owned state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceProvenance {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// The shortest poll interval a [`SourceMode::Tracking`] source may declare.
///
/// A floor, not a suggestion: `poll_secs: 0` (or a typo'd `1`) turns the fleet
/// into a request flood against someone else's host, and the operator who wrote
/// it would see only that their mocks update promptly. Five seconds is far
/// below any realistic config-change cadence while keeping a mistyped value
/// from being a denial of service — and the digest short circuit means a poll
/// that finds nothing new costs no log growth, so there is no reason to want
/// less.
pub const MIN_POLL_SECS: u64 = 5;

/// Largest config payload a [`ControlOp::SourcePullResult`] may carry, matching
/// the 10 MB body cap upstream's providers enforce at fetch time
/// (`rift_http_proxy::sources::MAX_BODY_BYTES`). Checked again here as defence
/// in depth: the provider cap bounds what a *fetch* buffers, while this bounds
/// what a *log entry* carries, and only the second one is a fleet-wide
/// liability. Both sit under the cluster transport's own cap.
pub const MAX_SOURCE_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Longest a fleet name ([`ControlOp::FleetNamePut`]) may be, in `char`s. A cap exists so the
/// name stays chrome-sized wherever it renders (a top bar, a members-list column); 128
/// matches the length ceiling this crate already uses for other operator-chosen names
/// (`is_source_name`), not any technical constraint of the field itself.
pub const MAX_FLEET_NAME_CHARS: usize = 128;

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

/// One audit record (RFC-002 §9, issue #163).
///
/// **Derived at apply, from the committed log entry** — never written by the
/// handler that accepted the request. That is the entire design: the #14 intent
/// log *is* the write-path audit record, so a row computed from it cannot
/// disagree with it, and every replica applying the same entry computes the
/// identical row. An audit log that can disagree with the thing it audits is
/// worse than none.
///
/// The consequence worth stating, because it is unusual in this crate:
/// **`GET /admin/audit` needs no fan-out.** Any node answers from local state.
/// (Contrast the M3 request journal, #147, which is genuinely per-node and
/// needs merge-on-read — do not import that machinery here.)
///
/// # What is not in here
///
/// Only ops that reach the log. Reads are not audited in v1 (§9, on volume),
/// and — the part that is easy to misread as a bug — neither are the
/// *reads-that-mutate* still served over the **proxy** path (`ScenarioReset`,
/// and the flow-state half of a space teardown). Those are forwarded to the
/// loopback core admin and never become a [`ControlOp`], so a log-derived
/// projection cannot see them. Auditing them means putting them on consensus;
/// recording them at the front door instead would produce per-node rows that can
/// disagree with the log, which is the one thing this design refuses.
///
/// `SavedRequestsClear` **used to be on that list and no longer is.** Issue #224
/// took the clear onto consensus as [`ControlOp::JournalClearGen`] — for
/// convergence rather than for auditing, but the audit row falls out of that for
/// free, which is exactly the trade this paragraph describes. The `?match=`
/// narrowed form stays proxied and therefore stays unaudited: it is a targeted,
/// best-effort per-shard deletion with no fleet-wide meaning to record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRow {
    /// The applying entry's `issued_at_secs` — the replicated clock, so the
    /// timestamp is identical on every replica rather than each node's idea of
    /// now.
    pub ts_secs: u64,
    /// Who ([`ControlRequest::principal`]). `None` for an op submitted with no
    /// attribution — the open admin plane, or an internal submitter.
    pub principal: Option<String>,
    /// The tenant the op acted on.
    ///
    /// Never null, and deliberately so. RFC-002 §6 reasons that a fleet-wide
    /// delete "carries no port, therefore no tenant" — but #159 put an explicit
    /// `tenant` on **every** op, so a [`ControlOp::DeleteAll`] destroys one
    /// named tenant's imposters and knows exactly which. Recording `null` there
    /// would hide whose data was destroyed, in the row describing the most
    /// destructive operation in the set. The wildcard belongs in `resource`,
    /// which is where it is.
    pub tenant: TenantId,
    /// The RFC-002 §4.1 action slug — the same strings `authz::Action::as_str`
    /// produces, so an audit row and an authorization decision name the same
    /// thing.
    pub action: String,
    /// What was acted on: a port, an id, or `"*"` for a whole-scope op.
    pub resource: String,
    pub op_id: Uuid,
    /// The applying log index — the same number the write's response carried.
    pub revision: u64,
    pub outcome: ControlOutcome,
}

/// The wildcard [`AuditRow::resource`] for an op that addresses a whole scope
/// rather than one object.
pub const AUDIT_RESOURCE_ALL: &str = "*";

impl ControlOp {
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
            | ControlOp::SourcePut { tenant, .. }
            | ControlOp::SourceDelete { tenant, .. }
            | ControlOp::SourcePullResult { tenant, .. }
            | ControlOp::TenantPut { tenant, .. }
            | ControlOp::TenantDelete { tenant }
            | ControlOp::PrincipalPut { tenant, .. }
            | ControlOp::PrincipalCreate { tenant, .. }
            | ControlOp::PrincipalDelete { tenant, .. }
            | ControlOp::BindingPut { tenant, .. }
            | ControlOp::BindingDelete { tenant, .. }
            | ControlOp::AuditSinkPut { tenant, .. }
            | ControlOp::AuditSinkDelete { tenant }
            | ControlOp::AuditCheckpointPut { tenant, .. }
            | ControlOp::SessionKeyPut { tenant, .. }
            | ControlOp::FleetNamePut { tenant, .. }
            | ControlOp::JournalClearGen { tenant, .. }
            | ControlOp::ProxyRecorded { tenant, .. }
            | ControlOp::ProxyRecordedClear { tenant, .. } => tenant,
        }
    }

    /// The §4.1 action slug for [`AuditRow::action`], or `None` for an op that
    /// is deliberately not audited.
    ///
    /// Exhaustive with no wildcard arm, for the same reason
    /// `admin_front::action_for` is: a new op added without deciding what it is
    /// called in the audit stream should fail to compile, not appear under
    /// whatever the fallthrough happened to say. The `Option` keeps that
    /// property while letting an op opt *out* explicitly — silence has to be a
    /// decision someone wrote down, not a missing arm.
    ///
    /// Exactly one op opts out today, and it has to:
    /// [`ControlOp::AuditCheckpointPut`] records how far the exporter has
    /// shipped. Auditing it would append a row, which the exporter would then
    /// ship, which would write a new checkpoint, which would append a row —
    /// an unbounded loop whose whole content is the loop itself.
    #[must_use]
    pub fn audit_action(&self) -> Option<&'static str> {
        Some(match self {
            ControlOp::AuditCheckpointPut { .. } => return None,
            // The second op that opts out, for the same "the silence is a decision" reason:
            // a recording is data-plane behavior — a proxied request the engine chose to
            // record — with no administrative principal behind it, and under proxyAlways it
            // commits once per proxied request. Auditing it would flood the stream that
            // exists to carry administrative intent with traffic-rate noise (#226).
            ControlOp::ProxyRecorded { .. } => return None,
            ControlOp::PutImposter { .. } => "imposter.write",
            ControlOp::PatchStubs { .. } => "stub.write",
            ControlOp::DeleteImposter { .. } | ControlOp::DeleteAll { .. } => "imposter.delete",
            ControlOp::SetEnabled { .. } => "lifecycle.toggle",
            // The front door's route table predates §4.1's closed list and has
            // no action of its own; `admin_front::action_for` gates it as an
            // imposter-tier config write, and this matches that decision rather
            // than inventing a second name for the same thing.
            ControlOp::PutRoutes { .. } => "imposter.write",
            ControlOp::DeleteRoute { .. } => "imposter.write",
            ControlOp::SourcePut { .. } | ControlOp::SourcePullResult { .. } => "imposter.write",
            ControlOp::SourceDelete { .. } => "imposter.delete",
            ControlOp::TenantPut { .. }
            | ControlOp::TenantDelete { .. }
            | ControlOp::PrincipalPut { .. }
            | ControlOp::PrincipalCreate { .. }
            | ControlOp::PrincipalDelete { .. }
            | ControlOp::BindingPut { .. }
            | ControlOp::BindingDelete { .. } => "tenant.manage",
            // Where the fleet's audit goes is a fleet-scoped decision, and it
            // is named the same thing here as the action that gates the
            // endpoint (`authz::Action::ClusterAdmin`).
            ControlOp::AuditSinkPut { .. } | ControlOp::AuditSinkDelete { .. } => "cluster.admin",
            // Minting or rotating the session key is a fleet-scoped security event, and rotation is
            // how every console session is revoked at once — precisely the kind of act an auditor
            // reading an incident timeline needs to see.
            ControlOp::SessionKeyPut { .. } => "cluster.admin",
            // Named identically to the session-key and audit-sink arms above: this is the same
            // "fleet-scoped operator act" category, and a rename is exactly the kind of thing an
            // incident timeline needs attributed — "which fleet was this?" is unanswerable
            // afterwards if the rename itself left no row.
            ControlOp::FleetNamePut { .. } => "cluster.admin",
            // Unlike `AuditCheckpointPut`, this one IS audited (issue #224): taking the clear onto
            // consensus is what makes an honest audit row possible at all — the pre-#224 fan-out
            // had no log entry to attribute one to. Named identically to `authz::Action::SavedRequestsClear`'s
            // own `as_str()`, matching every other action string here.
            ControlOp::JournalClearGen { .. } => "savedRequests.clear",
            // The clustered half of `DELETE .../savedProxyResponses` — gated by the same
            // `authz::Action::SavedRequestsClear` as the journal clear above, so it carries
            // the same name in the stream (#226).
            ControlOp::ProxyRecordedClear { .. } => "savedRequests.clear",
        })
    }

    /// What this op addressed, for [`AuditRow::resource`].
    #[must_use]
    pub fn audit_resource(&self) -> String {
        match self {
            ControlOp::PutImposter { config, .. } => config
                .port
                .map_or_else(|| AUDIT_RESOURCE_ALL.to_owned(), |port| port.to_string()),
            ControlOp::PatchStubs { port, .. }
            | ControlOp::DeleteImposter { port, .. }
            | ControlOp::SetEnabled { port, .. }
            | ControlOp::ProxyRecorded { port, .. }
            | ControlOp::ProxyRecordedClear { port, .. } => port.to_string(),
            // A whole-scope op names no single object. Wildcard rather than an
            // empty string so a reader never has to guess whether the field was
            // omitted or the op really did address everything.
            ControlOp::DeleteAll { .. }
            | ControlOp::PutRoutes { .. }
            | ControlOp::TenantDelete { .. } => AUDIT_RESOURCE_ALL.to_owned(),
            ControlOp::DeleteRoute { id, .. } => id.clone(),
            ControlOp::SourcePut { id, .. }
            | ControlOp::SourceDelete { id, .. }
            | ControlOp::SourcePullResult { id, .. } => id.clone(),
            ControlOp::TenantPut { tenant, .. } => tenant.as_str().to_owned(),
            ControlOp::PrincipalPut { principal, .. } => principal.id.as_str().to_owned(),
            ControlOp::PrincipalCreate { principal, .. } => principal.id.as_str().to_owned(),
            ControlOp::PrincipalDelete { principal_id, .. }
            | ControlOp::BindingPut { principal_id, .. }
            | ControlOp::BindingDelete { principal_id, .. } => principal_id.as_str().to_owned(),
            // One sink, fleet-wide: it addresses the whole scope, not a named
            // object. `AuditCheckpointPut` never reaches here (it is not
            // audited) but must still answer, so it answers the same way.
            ControlOp::AuditSinkPut { .. }
            | ControlOp::AuditSinkDelete { .. }
            | ControlOp::AuditCheckpointPut { .. }
            // One key, fleet-wide: it addresses the whole scope, not a named object. The key itself
            // must never reach an audit row.
            | ControlOp::SessionKeyPut { .. }
            // Same reasoning again: one name, fleet-wide, so it addresses the whole scope rather
            // than a named object.
            | ControlOp::FleetNamePut { .. } => AUDIT_RESOURCE_ALL.to_owned(),
            // A port-wide clear addresses the port, exactly like `PatchStubs`/`SetEnabled`; a
            // space-scoped one addresses the narrower `port/space` pair so an audit reader can
            // tell the two apart without decoding the outcome text.
            ControlOp::JournalClearGen { port, space, .. } => match space {
                Some(space) => format!("{port}/{space}"),
                None => port.to_string(),
            },
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
        ControlOp::SourcePut {
            tenant,
            id,
            uri,
            mode,
            auth_ref,
            poll_secs,
            ..
        } => {
            require_real_tenant(tenant)?;
            require_source_id(id)?;
            // Hygiene strictly before shape, and the order is load-bearing.
            // Both must hold, but a URI that fails *both* — say
            // `git+https://oauth2:ghp_secret@host/o/r` with no `#ref:path` —
            // must be refused by the credential check, whose message is
            // deliberately free of the URI. Shape errors are more specific and
            // so more tempting to run first; doing that puts a secret-bearing
            // URI into an operator-facing error string.
            require_credential_free_uri(uri)?;
            require_well_formed_uri(uri)?;
            match (mode, poll_secs) {
                (SourceMode::Tracking, None) => {
                    return Err(
                        "mode \"tracking\" requires pollSecs: a source the fleet polls has to say \
                         how often"
                            .to_owned(),
                    );
                }
                (SourceMode::Tracking, Some(secs)) if *secs < MIN_POLL_SECS => {
                    return Err(format!(
                        "pollSecs {secs} is below the {MIN_POLL_SECS}s floor: a shorter interval \
                         floods the source host, and an unchanged document costs nothing to \
                         re-poll at the floor"
                    ));
                }
                // Refused rather than ignored: a poll interval on a source
                // nobody polls is a setting that silently does nothing, which
                // is how an operator ends up believing their mocks track.
                (SourceMode::Pinned, Some(_)) => {
                    return Err(
                        "pollSecs applies to mode \"tracking\" only; a pinned source is pulled \
                         explicitly"
                            .to_owned(),
                    );
                }
                (SourceMode::Tracking, Some(_)) | (SourceMode::Pinned, None) => {}
            }
            // Validated as a *name* only. Resolving it to a credential ships
            // with the providers that need one (#136) — see the module doc.
            if let Some(auth_ref) = auth_ref
                && !is_source_name(auth_ref)
            {
                return Err(
                    "auth_ref must be a non-empty name of at most 128 characters drawn from \
                     [A-Za-z0-9._-]"
                        .to_owned(),
                );
            }
            Ok(())
        }
        ControlOp::SourceDelete { tenant, id } => {
            require_real_tenant(tenant)?;
            require_source_id(id)
        }
        ControlOp::SourcePullResult {
            tenant,
            id,
            digest,
            configs,
            ..
        } => {
            require_real_tenant(tenant)?;
            require_source_id(id)?;
            if digest.as_str().is_empty() {
                return Err(
                    "a pull result must carry a digest: it is what the no-change short circuit \
                     compares"
                        .to_owned(),
                );
            }
            // The bound on what a *log entry* carries. The provider's own 10 MB
            // fetch cap bounds a single fetch; this bounds what every replica
            // then stores and every snapshot copies, which is the fleet-wide
            // liability, so it is checked here rather than trusted upstream.
            let encoded = serde_json::to_vec(configs)
                .map_err(|e| format!("pull result configs do not serialize: {e}"))?;
            if encoded.len() > MAX_SOURCE_PAYLOAD_BYTES {
                return Err(format!(
                    "pull result carries {} bytes of configs, over the {MAX_SOURCE_PAYLOAD_BYTES} \
                     byte limit",
                    encoded.len()
                ));
            }
            // Held to exactly the `PutImposter` rules: a source must not be a
            // way to land a config that admission would otherwise refuse.
            let mut ports = std::collections::HashSet::new();
            for config in configs {
                validate_replicable_config(config)?;
                if let Some(port) = config.port
                    && !ports.insert(port)
                {
                    return Err(format!(
                        "source document declares port {port} twice; each port may be declared once"
                    ));
                }
            }
            Ok(())
        }
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
        ControlOp::AuditSinkPut {
            tenant,
            uri,
            auth_ref,
            batch_max_rows,
        } => {
            require_fleet_scope(tenant)?;
            // The same call, in the same order, as the `SourcePut` arm: hygiene
            // strictly before shape. #164 asks for "the same error shape as a
            // source URI", and reusing the function is the only way to keep
            // that true as either side changes.
            require_credential_free_uri(uri)?;
            require_audit_sink_uri(uri)?;
            if let Some(auth_ref) = auth_ref
                && !is_source_name(auth_ref)
            {
                return Err(
                    "auth_ref must be a non-empty name of at most 128 characters drawn from \
                     [A-Za-z0-9._-]"
                        .to_owned(),
                );
            }
            if *batch_max_rows == 0 || *batch_max_rows > MAX_AUDIT_BATCH_MAX_ROWS {
                return Err(format!(
                    "batchMaxRows must be between 1 and {MAX_AUDIT_BATCH_MAX_ROWS}: a batch is \
                     buffered whole before it is shipped"
                ));
            }
            Ok(())
        }
        ControlOp::AuditSinkDelete { tenant } | ControlOp::AuditCheckpointPut { tenant, .. } => {
            require_fleet_scope(tenant)
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
            // Deliberately not `is_source_name`'s `[A-Za-z0-9._-]` charset: that rule guards ids
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
            // The same defence-in-depth bound `SourcePullResult` draws, for the same
            // reason: a recorded body becomes a log entry every replica carries.
            if resp.body.len() > MAX_SOURCE_PAYLOAD_BYTES {
                return Err(format!(
                    "recorded response body exceeds the {MAX_SOURCE_PAYLOAD_BYTES}-byte log \
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

/// Several ops (the audit sink, the session-signing key, the fleet name) each address one
/// fleet-wide piece of state, so the fleet scope is the only tenant any of them may carry.
///
/// Refused rather than tolerated: these ops are audited, and `AuditRow::tenant` means "the
/// tenant the op acted on". Admitting `AuditSinkPut { tenant: "acme" }` — or a fleet rename
/// under the same tenant — would file a fleet-wide configuration change under one tenant's
/// name, in the stream this feature exists to produce.
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

/// Whether `uri` is a cleartext webhook that cannot leave this host.
///
/// The one exception to the https-only rule, and it is narrow on purpose: a
/// loopback sink puts no bytes on a network, so there is nothing for the
/// cleartext rule to protect. It exists so the export path is exercisable end
/// to end — by this crate's own tests, and by an operator running a collector
/// as a sidecar — without standing up a TLS terminator to prove it works.
///
/// Shared by [`validate`] (admission) and the transport factory (egress) so
/// the two enforcement points cannot drift into disagreeing about what
/// cleartext is permitted. They already did once: admission allowed a loopback
/// sink the transport then refused to build, which is a sink that commits
/// cleanly and silently exports nothing.
pub(crate) fn is_loopback_http(uri: &str) -> bool {
    let Some(rest) = uri.trim().strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // `rsplit_once` so an IPv6 literal's own colons stay with the host.
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host,
        _ => authority,
    };
    matches!(host, "127.0.0.1" | "localhost" | "[::1]")
}

/// Which schemes the audit exporter can actually ship to.
///
/// Refused at admission rather than at export time for the same reason the
/// source providers refuse an unfetchable URI: a sink that no transport can
/// reach would otherwise be committed, agreed by the fleet, and then fail every
/// batch forever while looking configured.
///
/// `http://` is refused as well as unknown schemes, save the narrow loopback
/// carve-out [`is_loopback_http`] defines. An audit stream names who did what
/// to which tenant; shipping it in cleartext is not a trade-off an operator
/// should be able to make by typing one fewer character.
///
/// Every arm below recognises its scheme by a lowercase prefix test, so a
/// *known* scheme spelled otherwise is refused with a targeted message rather
/// than the generic one (issue #313). RFC 3986 §3.1 makes schemes
/// case-insensitive on the wire, so without that the refusal quotes back the
/// very schemes the operator believes they wrote — and `HTTP://127.0.0.1`
/// misses [`is_loopback_http`] for the same reason, refusing a *permitted*
/// sink as cleartext.
fn require_audit_sink_uri(uri: &str) -> Result<(), String> {
    let uri = uri.trim();
    if uri.starts_with("https://") {
        return Ok(());
    }
    if is_loopback_http(uri) {
        return Ok(());
    }
    if let Some(rest) = uri.strip_prefix("s3://") {
        // Same shape the s3 source parses: `s3://<bucket>/<key-prefix>`.
        return match rest.split_once('/') {
            Some((bucket, prefix)) if !bucket.is_empty() && !prefix.is_empty() => Ok(()),
            _ => Err(
                "an s3 audit sink is written `s3://<bucket>/<key-prefix>`: the prefix is where \
                 the batched objects are written"
                    .to_owned(),
            ),
        };
    }
    // Refused, not normalized: lowercasing here and nowhere else would put this
    // checker and the transport factory on different spellings, which is the
    // two-parsers-disagree bug (#301) in a new place. The message names only the
    // canonical lowercase literal, never the input, keeping the no-echo rule
    // above intact.
    //
    // A mixed-case scheme whose lowercase form would *still* be refused — say
    // `HTTP://evil.example` — is told about the case first and meets the
    // cleartext refusal on the next attempt. That teaching sequence is the same
    // one #312 established for source URIs, and is why this check does not try
    // to predict whether the corrected URI would pass.
    let (scheme, rest) = split_scheme(uri);
    if scheme.bytes().any(|b| b.is_ascii_uppercase()) {
        let lower = scheme.to_ascii_lowercase();
        // `http` is the one known scheme that lowercasing alone does not make
        // admissible — only a loopback host may use cleartext. Hinting at the
        // case for a remote `HTTP://` would replace an accurate, terminal
        // message ("cleartext is refused") with a detour to the same refusal,
        // so the hint is offered only where the corrected URI would actually be
        // accepted. `https` and `s3` need no such test: for them lowercasing is
        // either the whole fix or the next refusal is about shape, which is the
        // teaching sequence #312 established.
        let worth_hinting = match lower.as_str() {
            "https" | "s3" => true,
            "http" => is_loopback_http(&format!("{lower}:{rest}")),
            _ => false,
        };
        if worth_hinting {
            return Err(format!(
                "an audit sink scheme must be lowercase: write `{lower}://…`"
            ));
        }
    }

    Err(
        "an audit sink uri must be https:// (webhook, JSON lines) or s3:// (bucket, batched \
         objects); http:// is refused because an audit stream must not cross the network in \
         cleartext"
            .to_owned(),
    )
}

/// The rules every config carried by the log must satisfy, whether an operator
/// wrote it directly ([`ControlOp::PutImposter`]) or a source produced it
/// ([`ControlOp::SourcePullResult`]). Shared so the two paths cannot drift into
/// admitting different things.
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

/// Whether `name` is usable as a source id or an `auth_ref`: a bounded,
/// path-safe, redb-key-safe token.
fn is_source_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

fn require_source_id(id: &str) -> Result<(), String> {
    if is_source_name(id) {
        Ok(())
    } else {
        Err(
            "source id must be a non-empty name of at most 128 characters drawn from \
             [A-Za-z0-9._-]: it addresses the record and appears in the request path"
                .to_owned(),
        )
    }
}

/// Refuse a source URI that carries credentials in its authority.
///
/// This is the secret-hygiene rule, and it lives in `validate` rather than only
/// in the admin handler because `validate` is what runs before the state
/// machine mutates anything on *every* replica: a URI that gets past admission
/// by some other route still never becomes stored state. `auth_ref` is the only
/// credential path.
///
/// The scheme is deliberately not checked against a registry here. Which
/// schemes a node serves is per-node configuration (an embedder registers its
/// own providers), so a registry check inside deterministic validation would
/// let two replicas disagree about the same committed op. The admin handler
/// makes that check node-locally, before the op is ever submitted.
///
/// A URI has an authority — and therefore somewhere to hide a credential — only
/// when the scheme is followed immediately by `//` (RFC 3986 §3). Anything else
/// is a path: upstream's `SourceRef::scheme` routes a `file:` prefix and a bare
/// path to the local file provider, so an `@` there is a filename character.
///
/// The scheme is parsed properly — via [`split_scheme`] — rather than by
/// searching for `://` anywhere in the string, because "anywhere" finds the
/// wrong one: in a URI like `s3:key@bucket/p?endpoint=https://minio.local` the
/// *query* would be read as the authority, see no `@`, and admit a credential
/// into the log. Parsing from the front means the delimiter has to be where a
/// scheme delimiter actually is. (That particular URI is now refused earlier,
/// by the `s3` shape check — but the reasoning is what
/// [`require_well_formed_uri`] was missing until #301, and it is the reason
/// both functions share one parse.)
///
/// **No message here echoes the URI.** The same rule
/// [`require_well_formed_uri`] states for the shape family holds here, and
/// for a sharper reason: this function *deliberately permits* a token in a
/// query string, so its own refusals are the ones most likely to be holding
/// a secret when they fire (#309).
fn require_credential_free_uri(uri: &str) -> Result<(), String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err("source uri must not be empty".to_owned());
    }
    let (scheme, rest) = split_scheme(uri);
    // Everything before the first `/` (or query/fragment) of the hier-part,
    // and only when the hier-part actually opens with `//`.
    let Some(hier) = rest.strip_prefix("//") else {
        // No authority to carry a credential: `file:…`, `s3:key@bucket/p`, and
        // bare paths all address something local or opaque, never a host this
        // node would authenticate to.
        return Ok(());
    };
    let authority = hier.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.contains('@') {
        return Err(
            "source uri carries credentials in its authority; pass a credential name as auth_ref \
             instead so no secret enters the replicated log"
                .to_owned(),
        );
    }
    if authority.is_empty() {
        // `file:///path` and `git+file:///path` are the standard spellings of a
        // *local* URL — RFC 8089 gives them an explicitly empty authority.
        // There is no host to name and none this node could authenticate to, so
        // refusing them here rejected the only correct way to write them: it
        // left `git+file:` with no admissible spelling at all once the
        // single-colon form is refused below (#301). Schemes that really do
        // address a host keep the refusal.
        // Pinned to the schemes actually known to mean "local", not a
        // `*+file` suffix heuristic: this is a security classifier, and a
        // naming convention is the wrong thing to have deciding that an
        // embedder's future `webdav+file:` needs no host. Case-SENSITIVE for
        // the same reason every other scheme comparison on this path is —
        // `require_well_formed_uri`'s `match`, the provider registry, and
        // `parse_git_uri`'s `strip_prefix`. Folding case would admit
        // `GIT+FILE:///…`, which matches no shape check and no provider:
        // committed to the log and unfetchable forever.
        if matches!(scheme, "file" | "git+file") {
            return Ok(());
        }
        // Shape description only, never the URI: this fires on exactly the
        // inputs whose query string — where a token is deliberately permitted
        // — would ride an echo into the 400 body and the admission log (#309).
        return Err(
            "source uri opens an authority (`//`) but names no host: write `<scheme>://<host>/…`"
                .to_owned(),
        );
    }
    Ok(())
}

/// The scheme, and everything after its colon — parsed **once**, so the two
/// admission checks can never disagree about the same URI.
///
/// They used to. [`require_credential_free_uri`] anchored the scheme to the
/// first colon whose prefix satisfies the RFC 3986 grammar, while
/// [`require_well_formed_uri`] took whatever preceded the first `"://"`
/// *anywhere* in the string. So `git+file:/x#main:a://b` — a single-colon git
/// URI, exactly the shape #301 refuses — yielded the scheme
/// `"git+file:/x#main:a"`, matched no arm, and fell through the permissive
/// unknown-scheme catch-all into the replicated log, where every pull then
/// failed forever. Two parsers disagreeing about one URI is the whole subject
/// of #301; having it happen twice in one file was not defensible.
///
/// Returns an empty scheme when the URI has no grammar-valid one, leaving the
/// whole string as the rest — which is what makes a scheme-less `//host/path`
/// still present its authority to the credential check.
fn split_scheme(uri: &str) -> (&str, &str) {
    match uri.find(':').filter(|end| {
        // RFC 3986: ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
        let scheme = &uri[..*end];
        scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    }) {
        Some(end) => (&uri[..end], &uri[end + 1..]),
        None => ("", uri),
    }
}

/// Per-scheme URI *shape* checks for the cluster providers (#136), so a URI
/// that no provider could ever fetch is refused at admission with a 400 instead
/// of being committed and then failing every pull forever.
///
/// This is deterministic and stays deterministic: it is a pure function of the
/// URI string. That is precisely what separates it from the check
/// [`require_credential_free_uri`]'s doc rules out — asking *which schemes this
/// node serves* is per-node configuration and would let two replicas disagree
/// about one committed op, while asking *whether a `git+https:` URI has a
/// `#ref:path` fragment* has the same answer on every node forever.
///
/// A scheme this build knows nothing about is not an error here. Embedders
/// register their own providers, so an unknown scheme is the node-local check
/// the admin handler already makes before submitting. A *known* scheme spelled
/// in anything but its canonical lowercase form, however, is refused: the
/// provider registry matches the lowercase spelling only, so such an op could
/// never be pulled (#308).
///
/// **No message here echoes the URI.** [`require_credential_free_uri`] runs
/// first and guarantees the *authority* holds no credential, but it
/// deliberately permits an `@` — and therefore a token — in a query string, and
/// these refusals are rendered straight back to the caller and into the
/// admission log. Naming the scheme and the shape that was expected is just as
/// actionable to the operator who wrote the URI, and cannot leak.
fn require_well_formed_uri(uri: &str) -> Result<(), String> {
    let uri = uri.trim();
    // Whether an authority followed the scheme is itself a fact the git arm
    // needs: the fetch path's parser (upstream `SourceRef::scheme`) splits on
    // `"://"`, so a `git+` URI written with a bare colon is one the two parsers
    // *disagree* about (#301).
    let (scheme, rest) = split_scheme(uri);
    let has_authority = rest.starts_with("//");

    // RFC 3986 §3.1: schemes are case-insensitive on the wire, but the
    // provider registry and `parse_git_uri` match the canonical lowercase
    // form only. A non-lowercase spelling of a known scheme would skip the
    // shape checks below and then fail provider lookup on every pull, so it
    // is refused at admission rather than normalized — normalizing here and
    // nowhere else is the two-parsers-disagree bug (#301) again. The message
    // names only the canonical lowercase literal, never the input, keeping
    // the no-echo rule above trivially intact.
    if scheme.bytes().any(|b| b.is_ascii_uppercase()) {
        let lower = scheme.to_ascii_lowercase();
        if matches!(lower.as_str(), "git+https" | "git+file" | "s3" | "registry") {
            return Err(format!(
                "source uri scheme must be lowercase: write `{lower}://…`"
            ));
        }
    }

    match scheme {
        "git+https" | "git+file" => {
            // The disagreement, refused rather than resolved. Upstream routes
            // `git+file:/srv/r.git` to the `file:` provider, which opens the
            // whole string as a path and fails with a not-found naming a path
            // nobody wrote — after this function has called it a well-formed
            // git source. Admitting one canonical spelling makes the ambiguity
            // unrepresentable in the replicated log instead of merely
            // differently-routed, and the refusal teaches the shape.
            if !has_authority {
                return Err(format!(
                    "source uri uses the single-colon `{scheme}:` form, which the fetch path \
                     routes to the `file:` provider: write `{scheme}://…`"
                ));
            }
            let git_shape = format!("write `{scheme}://host/org/repo#<ref>:<path>`");
            let (before_fragment, fragment) = uri
                .split_once('#')
                .ok_or_else(|| format!("source uri names no ref and path: {git_shape}"))?;
            let (git_ref, path) = fragment
                .split_once(':')
                .ok_or_else(|| format!("source uri names a ref but no path: {git_shape}"))?;
            if git_ref.trim().is_empty() {
                return Err(format!("source uri names an empty git ref: {git_shape}"));
            }
            if path.trim().is_empty() {
                return Err(format!(
                    "source uri names an empty path in the repo: {git_shape}"
                ));
            }
            // The same argument-injection refusal `sources::git` applies at
            // fetch time (issue #136 review, B1), mirrored here so a remote
            // or ref that `git` would read as an option — or an `ext::`
            // transport helper — is refused at *admission*, before the op
            // ever reaches the replicated log, rather than only discovered
            // when the leader (or whichever node performs the pull) tries to
            // fetch it. `before_fragment` is split exactly the way
            // `sources::git::parse_git_uri` splits it, so this checks the
            // same remote the fetch would actually use. Neither
            // `check_remote` nor `check_ref` ever echoes its input, so this
            // cannot violate the "no message here echoes the URI" rule above.
            let is_file = scheme == "git+file";
            let remote = if is_file {
                before_fragment.strip_prefix("git+file:")
            } else {
                before_fragment.strip_prefix("git+")
            }
            .unwrap_or(before_fragment);
            crate::sources::git::check_remote(remote, is_file).map_err(|e| e.to_string())?;
            crate::sources::git::check_ref(git_ref).map_err(|e| e.to_string())?;
            Ok(())
        }
        "s3" => {
            let rest = uri
                .strip_prefix("s3://")
                .ok_or_else(|| "source uri is not written `s3://<bucket>/<key>`".to_owned())?;
            let (bucket, key) = rest.split_once('/').ok_or_else(|| {
                "source uri names a bucket but no key: write `s3://<bucket>/<key>`".to_owned()
            })?;
            if bucket.is_empty() {
                return Err("source uri names no bucket: write `s3://<bucket>/<key>`".to_owned());
            }
            if key.trim_matches('/').is_empty() {
                return Err("source uri names no key: write `s3://<bucket>/<key>`".to_owned());
            }
            Ok(())
        }
        "registry" => {
            let rest = uri.strip_prefix("registry://").ok_or_else(|| {
                "source uri is not written `registry://<service-id>[,…]`".to_owned()
            })?;
            if rest.is_empty() {
                return Err("source uri names no service: write \
                            `registry://<service-id>[,<service-id>…]`"
                    .to_owned());
            }
            // A trailing or doubled comma yields an empty id, which would
            // otherwise become a request to the registry's collection endpoint
            // — a very different query than the operator wrote.
            if rest.split(',').any(|id| id.trim().is_empty()) {
                return Err("source uri has an empty service id; write \
                            `registry://<service-id>[,<service-id>…]`"
                    .to_owned());
            }
            Ok(())
        }
        _ => Ok(()),
    }
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
/// token. Wider than [`is_source_name`]'s charset — a principal id may come
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
        // A source op addresses a source, not an imposter: `expected_revision`
        // is defined against `sm_configs` rows, so there is no record here for
        // a precondition to hold against.
        | ControlOp::SourcePut { .. }
        | ControlOp::SourceDelete { .. }
        | ControlOp::SourcePullResult { .. }
        | ControlOp::TenantPut { .. }
        | ControlOp::TenantDelete { .. }
        | ControlOp::PrincipalPut { .. }
        | ControlOp::PrincipalCreate { .. }
        | ControlOp::PrincipalDelete { .. }
        | ControlOp::BindingPut { .. }
        | ControlOp::BindingDelete { .. }
        // The audit-export ops address the fleet's sink, not an imposter record.
        | ControlOp::AuditSinkPut { .. }
        | ControlOp::AuditSinkDelete { .. }
        | ControlOp::AuditCheckpointPut { .. }
        // The session key addresses the fleet, not an imposter record.
        | ControlOp::SessionKeyPut { .. }
        // The fleet name addresses the fleet, not an imposter record — same reasoning as the
        // session key immediately above.
        | ControlOp::FleetNamePut { .. }
        // A clear is a convergence primitive, not a config write conditioned on a stored
        // revision: it commits unconditionally, like `AuditCheckpointPut`'s `max` does, so two
        // concurrent clears compose rather than one losing an optimistic-concurrency race the
        // op was never meant to run.
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
                ControlOp::SourcePut {
                    tenant: TenantId::default(),
                    id: "mocks".to_owned(),
                    uri: "https://h/i.json".to_owned(),
                    mode: SourceMode::Pinned,
                    auth_ref: None,
                    on_drift: OnDrift::Overwrite,
                    poll_secs: None,
                },
                "SourcePut",
            ),
            (
                ControlOp::SourceDelete {
                    tenant: TenantId::default(),
                    id: "mocks".to_owned(),
                },
                "SourceDelete",
            ),
            (
                ControlOp::SourcePullResult {
                    tenant: TenantId::default(),
                    id: "mocks".to_owned(),
                    version: None,
                    digest: Digest::new("a".repeat(64)),
                    configs: vec![],
                },
                "SourcePullResult",
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

    /// The source enums are log format too: a replica decoding `"overwrite"` as
    /// something else would apply a committed pull differently from its peers.
    #[test]
    fn source_enum_spellings_are_stable() {
        for (mode, spelling) in [
            (SourceMode::Pinned, "pinned"),
            (SourceMode::Tracking, "tracking"),
        ] {
            assert_eq!(
                serde_json::to_value(mode).expect("serialize"),
                json!(spelling)
            );
        }
        for (on_drift, spelling) in [
            (OnDrift::Overwrite, "overwrite"),
            (OnDrift::Skip, "skip"),
            (OnDrift::Fail, "fail"),
        ] {
            assert_eq!(
                serde_json::to_value(on_drift).expect("serialize"),
                json!(spelling)
            );
            assert_eq!(on_drift.to_string(), spelling);
        }
        // The defaults a source takes when the operator declares neither.
        assert_eq!(SourceMode::default(), SourceMode::Pinned);
        assert_eq!(OnDrift::default(), OnDrift::Overwrite);
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

    // -- validate: source ops (issue #134) --------------------------------------

    fn source_put(id: &str, uri: &str) -> ControlOp {
        ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: id.to_owned(),
            uri: uri.to_owned(),
            mode: SourceMode::Pinned,
            auth_ref: None,
            on_drift: OnDrift::Overwrite,
            poll_secs: None,
        }
    }

    fn pull_result(id: &str, configs: Vec<ImposterConfig>) -> ControlOp {
        ControlOp::SourcePullResult {
            tenant: TenantId::default(),
            id: id.to_owned(),
            version: Some("v1".to_owned()),
            digest: Digest::new("a".repeat(64)),
            configs,
        }
    }

    #[test]
    fn validate_accepts_a_well_formed_pinned_source() {
        assert_eq!(
            validate(&source_put("mocks", "https://h/imposters.json")),
            Ok(())
        );
        assert_eq!(
            validate(&source_put("mocks", "file:/srv/mocks.json")),
            Ok(())
        );
        assert_eq!(
            validate(&ControlOp::SourceDelete {
                tenant: TenantId::default(),
                id: "mocks".to_owned(),
            }),
            Ok(())
        );
    }

    /// The secret-hygiene rule: `auth_ref` is the only credential path, so a URI
    /// that carries one is refused at admission — before it can be written to a
    /// log that every replica keeps and every snapshot copies.
    #[test]
    fn validate_rejects_embedded_credentials_in_a_source_uri() {
        for uri in [
            "https://user:pass@host/x.json",
            "https://token@host/x.json",
            "git+https://oauth2:ghp_secret@github.com/o/r#main:p",
        ] {
            let err = validate(&source_put("mocks", uri))
                .expect_err("a URI carrying credentials must be refused");
            assert!(
                err.contains("auth_ref") && err.contains("credential"),
                "the refusal must point at the supported path: {err}"
            );
            assert!(
                !err.contains(uri) && !err.contains('@'),
                "the refusal must not echo the credential-bearing uri back: {err}"
            );
        }
    }

    // -- audit export sink (#164) -------------------------------------------

    fn sink_put(uri: &str) -> ControlOp {
        ControlOp::AuditSinkPut {
            tenant: TenantId::new(FLEET_SCOPE),
            uri: uri.to_owned(),
            auth_ref: None,
            batch_max_rows: DEFAULT_AUDIT_BATCH_MAX_ROWS,
        }
    }

    /// #164's "no credential in the log" criterion, asserted the strong way:
    /// not merely that a credential-bearing sink URI is refused, but that it is
    /// refused with the **byte-identical** message a source URI gets.
    ///
    /// Equality rather than a substring match is the point. The criterion says
    /// "the same error shape as a source URI", and the only way that stays true
    /// as either path changes is if both call one function — so the test is
    /// written to fail the moment they diverge, including if someone
    /// "improves" one message and not the other.
    #[test]
    fn a_sink_uri_with_embedded_credentials_is_refused_like_a_source_uri() {
        for (sink_uri, source_uri) in [
            (
                "https://user:pass@collector.example/audit",
                "https://user:pass@host/x.json",
            ),
            (
                "https://token@collector.example/audit",
                "https://token@host/x.json",
            ),
        ] {
            let sink_err =
                validate(&sink_put(sink_uri)).expect_err("a sink URI carrying credentials");
            let source_err = validate(&source_put("mocks", source_uri))
                .expect_err("a source URI carrying credentials");
            assert_eq!(
                sink_err, source_err,
                "the sink and source hygiene refusals must be the same message, produced by \
                 the same check"
            );
            assert!(
                !sink_err.contains(sink_uri) && !sink_err.contains('@'),
                "the refusal must not echo the credential-bearing uri back: {sink_err}"
            );
        }
    }

    /// Hygiene runs strictly before shape here, exactly as it does for a
    /// source. A URI that fails both must be caught by the credential check,
    /// whose message is deliberately free of the URI — running the more
    /// specific scheme check first would put a secret into an operator-facing
    /// error string.
    #[test]
    fn sink_credential_hygiene_runs_before_the_scheme_check() {
        // A distinctive secret: the refusal's own text contains the word
        // "pass" ("pass a credential name as auth_ref"), so a literal
        // `user:pass@` would make this assertion fire on the message rather
        // than on a leak.
        let err = validate(&sink_put("ftp://user:s3cr3t-t0ken@host/audit"))
            .expect_err("credential-bearing and wrong-scheme");
        assert!(
            err.contains("auth_ref"),
            "the credential check must win: {err}"
        );
        assert!(
            !err.contains("s3cr3t-t0ken"),
            "the refusal must not echo the secret: {err}"
        );
    }

    #[test]
    fn an_audit_sink_uri_must_be_https_or_s3() {
        validate(&sink_put("https://collector.example/audit")).expect("https is a webhook sink");
        validate(&sink_put("s3://bucket/audit-prefix")).expect("s3 is a bucket sink");

        // Cleartext over the network is refused rather than merely
        // discouraged: an audit stream names who did what to which tenant.
        let err = validate(&sink_put("http://collector.example/audit"))
            .expect_err("http:// to a remote host must be refused");
        assert!(err.contains("cleartext"), "{err}");

        // …but a loopback collector never puts those bytes on a network, so it
        // is allowed. Asserted so the carve-out stays exactly this narrow: a
        // later "simplification" that allowed any http:// would pass the check
        // above and be caught here.
        for loopback in [
            "http://127.0.0.1:9000/audit",
            "http://localhost:9000/audit",
            "http://[::1]:9000/audit",
        ] {
            validate(&sink_put(loopback))
                .unwrap_or_else(|e| panic!("a loopback collector is allowed: {loopback}: {e}"));
        }
        for remote in [
            "http://127.0.0.1.example.com/audit",
            "http://evil.com/audit",
            "http://10.0.0.1/audit",
        ] {
            validate(&sink_put(remote))
                .expect_err("only a genuine loopback host may use cleartext");
        }

        let err = validate(&sink_put("s3://bucket")).expect_err("an s3 sink needs a key prefix");
        assert!(err.contains("<bucket>/<key-prefix>"), "{err}");
    }

    /// RFC 3986 §3.1 makes schemes case-insensitive, but every arm of
    /// `require_audit_sink_uri` recognises its scheme with a lowercase prefix
    /// test. A mixed-case spelling of a *known* scheme therefore falls through
    /// to the generic refusal, which quotes the very schemes the operator
    /// believes they wrote (issue #313).
    ///
    /// `HTTP://127.0.0.1` is the case that makes this more than cosmetic:
    /// `is_loopback_http` misses it for the same reason, so a sink that is
    /// **permitted** in lowercase is refused with the cleartext-danger
    /// message — wrong twice, since nothing about a loopback sink crosses a
    /// network.
    ///
    /// The refusal names only the canonical lowercase literal, never the URI:
    /// the file's standing no-echo rule, inherited from #312.
    #[test]
    fn an_audit_sink_scheme_must_be_lowercase() {
        // The expected literal is pinned per case, not merely "contains
        // lowercase": a message hardcoded to one scheme would satisfy a
        // contains-check for all four while telling the `S3://` operator to
        // write `https://`, which is worse than the generic refusal it replaced.
        for (uri, want) in [
            ("S3://bucket/audit-prefix", "`s3://"),
            ("HTTPS://collector.example/audit", "`https://"),
            ("Https://collector.example/audit", "`https://"),
            ("HTTP://127.0.0.1:9000/audit", "`http://"),
        ] {
            let err = validate(&sink_put(uri)).expect_err("a mixed-case known scheme is refused");
            assert!(
                err.contains("lowercase"),
                "refusing {uri}: expected a lowercase-scheme refusal, got {err:?}"
            );
            assert!(
                err.contains(want),
                "refusing {uri}: expected the hint to name {want}…`, got {err:?}"
            );
            assert!(!err.contains(uri), "the refusal echoed the uri: {err}");
        }

        // An *unknown* scheme in uppercase keeps the generic refusal. Telling
        // the operator to try lowercase would be a lie: `ftp` is not a sink
        // scheme in any spelling, and the hint would send them in circles.
        let err = validate(&sink_put("FTP://host/audit")).expect_err("ftp is not a sink scheme");
        assert!(
            !err.contains("lowercase"),
            "an unknown scheme must not be told to try lowercase: {err}"
        );
        assert!(
            err.contains("cleartext"),
            "expected the generic sink refusal: {err}"
        );

        // A *remote* uppercase `HTTP://` is the asymmetric case: unlike `https`
        // and `s3`, lowercasing does not make it admissible, so the accurate
        // cleartext refusal must survive rather than being displaced by a hint
        // that leads back to it.
        let err = validate(&sink_put("HTTP://collector.example/audit"))
            .expect_err("remote cleartext is refused in any spelling");
        assert!(
            !err.contains("lowercase"),
            "a remote HTTP:// must keep the accurate cleartext refusal: {err}"
        );
        assert!(err.contains("cleartext"), "{err}");
    }

    /// Credential hygiene runs before the scheme check, and must keep winning
    /// when the URI is *both* credential-bearing and mixed-case — the ordering
    /// #312 pinned for source URIs (`validate_rejects_embedded_credentials_in_a_source_uri`)
    /// and which nothing pinned on the sink path until now. Without this, a
    /// reordering of the two checks would leak a token into the admission log
    /// silently, since every refusal on this path is echo-free and the tests
    /// would all still pass.
    #[test]
    fn sink_credential_hygiene_wins_over_the_scheme_case_check() {
        let err = validate(&sink_put(
            "HTTPS://oauth2:ghp_supersecret@collector.example/audit",
        ))
        .expect_err("a credential-bearing sink uri is refused");
        assert!(
            !err.contains("ghp_supersecret"),
            "the refusal echoed the secret: {err}"
        );
        assert!(
            err.contains("auth_ref"),
            "expected the hygiene refusal, got the scheme-case refusal: {err}"
        );
    }

    #[test]
    fn a_sink_batch_size_is_bounded() {
        for (rows, why) in [
            (0, "zero would ship nothing forever"),
            (u32::MAX, "unbounded"),
        ] {
            let err = validate(&ControlOp::AuditSinkPut {
                tenant: TenantId::new(FLEET_SCOPE),
                uri: "https://collector.example/audit".to_owned(),
                auth_ref: None,
                batch_max_rows: rows,
            })
            .expect_err(why);
            assert!(err.contains("batchMaxRows"), "{rows}: {err}");
        }
    }

    /// The feedback loop this `None` exists to prevent: a checkpoint write that
    /// produced an audit row would be shipped by the exporter, which would
    /// write a new checkpoint, which would produce a new row — forever, with no
    /// content but the loop itself.
    #[test]
    fn audit_checkpoint_writes_are_not_themselves_audited() {
        assert_eq!(
            ControlOp::AuditCheckpointPut {
                tenant: TenantId::new(FLEET_SCOPE),
                revision: 42,
            }
            .audit_action(),
            None,
            "auditing the exporter's own checkpoint is an unbounded feedback loop"
        );

        // Every *other* op still is audited — the opt-out must stay a
        // deliberate single case, not a hole new ops fall into.
        for op in [
            ControlOp::AuditSinkPut {
                tenant: TenantId::new(FLEET_SCOPE),
                uri: "https://collector.example/audit".to_owned(),
                auth_ref: None,
                batch_max_rows: DEFAULT_AUDIT_BATCH_MAX_ROWS,
            },
            ControlOp::AuditSinkDelete {
                tenant: TenantId::new(FLEET_SCOPE),
            },
            ControlOp::DeleteAll {
                tenant: TenantId::default(),
            },
        ] {
            assert!(
                op.audit_action().is_some(),
                "{op:?} must appear in the audit stream"
            );
        }
    }

    /// A fleet-scoped change must be *attributed* to the fleet, not filed under
    /// whichever tenant happened to be handy.
    ///
    /// This shipped wrong once: the admin handler minted the sink ops with
    /// `TenantId::default()`, so changing where the whole fleet's audit goes
    /// produced a row claiming the `default` tenant did it — a wrong answer in
    /// the one stream this feature exists to produce, and one that reads as
    /// perfectly ordinary.
    #[test]
    fn a_fleet_scoped_sink_change_is_audited_against_the_fleet_scope() {
        for op in [
            ControlOp::AuditSinkPut {
                tenant: TenantId::new(FLEET_SCOPE),
                uri: "https://collector.example/audit".to_owned(),
                auth_ref: None,
                batch_max_rows: DEFAULT_AUDIT_BATCH_MAX_ROWS,
            },
            ControlOp::AuditSinkDelete {
                tenant: TenantId::new(FLEET_SCOPE),
            },
        ] {
            validate(&op).expect("the fleet scope is a valid tenant for a sink op");
            assert_eq!(
                op.tenant().as_str(),
                FLEET_SCOPE,
                "{op:?} must be audited against the fleet, not a tenant"
            );
            assert_eq!(
                op.audit_resource(),
                AUDIT_RESOURCE_ALL,
                "one fleet-wide sink addresses a whole scope, not a named object"
            );
        }
    }

    /// The sink record is what a snapshot copies and `GET /admin/audit/sink`
    /// serves, so the type must be incapable of carrying a secret in the first
    /// place — a check on the *shape*, not on one instance's contents.
    #[test]
    fn the_stored_sink_record_carries_a_name_and_never_a_credential() {
        let record = AuditSink {
            uri: "s3://bucket/audit".to_owned(),
            auth_ref: Some("prod-collector".to_owned()),
            batch_max_rows: DEFAULT_AUDIT_BATCH_MAX_ROWS,
            revision: 7,
        };
        let encoded = serde_json::to_value(&record).expect("sink record serializes");
        // Compared as a set: `serde_json::Map` is a sorted map, so key order
        // here is alphabetical and says nothing about the struct. The claim
        // being locked is *which* fields exist, not their order.
        let fields: std::collections::BTreeSet<&str> = encoded
            .as_object()
            .expect("a sink record is a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            fields,
            ["authRef", "batchMaxRows", "revision", "uri"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "a new field here is a new chance to leak a secret into the log; add it deliberately"
        );
    }

    // -- per-provider URI shape (#136) --------------------------------------

    /// A `git+https:` URI whose fragment is missing or malformed can never be
    /// fetched, so it is refused at admission rather than committed and then
    /// failing every pull for the life of the source.
    #[test]
    fn validate_rejects_a_malformed_git_uri() {
        for (uri, expected) in [
            ("git+https://host/o/r", "names no ref and path"),
            ("git+https://host/o/r#main", "names a ref but no path"),
            ("git+https://host/o/r#:mocks.json", "empty git ref"),
            ("git+https://host/o/r#main:", "empty path"),
        ] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                err.contains(expected),
                "refusing {uri}: expected {expected:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn validate_accepts_a_well_formed_git_uri() {
        for uri in [
            "git+https://github.com/org/repo#main:imposters.json",
            "git+https://github.com/org/repo#v1.2.3:mocks/",
            "git+file:///srv/repos/mocks.git#main:imposters.json",
            // uppercase outside the scheme is untouched by the #308 refusal
            "git+https://Host/Org/Repo#Main:Path.json",
        ] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
        }
    }

    /// RFC 3986 §3.1 makes schemes case-insensitive, but the provider
    /// registry (and upstream `SourceRef::scheme`) match the lowercase
    /// spelling only. A non-lowercase spelling of a known scheme therefore
    /// must be refused at admission — otherwise it skips every per-scheme
    /// shape check above, commits, and then fails provider lookup on every
    /// pull. The refusal names only the canonical lowercase scheme, never
    /// the URI (issue #308). It runs before the per-scheme arms, so a URI
    /// that is both mixed-case and mis-spelled (single-colon `Git+File:`)
    /// gets the case refusal first — fixing the case then teaches the
    /// spelling (#301).
    #[test]
    fn validate_refuses_a_non_lowercase_known_scheme() {
        for uri in [
            "GIT+HTTPS://host/o/r#main:p",
            "Git+File:/srv/r.git#main:x",
            "S3://bucket/key",
            "REGISTRY://svc",
            "git+HTTPS://host/o/r#main:p",
        ] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                err.contains("lowercase"),
                "refusing {uri}: expected a lowercase-scheme refusal, got {err:?}"
            );
            assert!(!err.contains(uri), "the refusal echoed the uri: {err}");
        }
    }

    // -- issue #301: the two scheme parsers must not disagree ------------------

    /// The defect this closes: upstream's `SourceRef::scheme` splits on `"://"`
    /// and calls `git+file:/srv/r.git` a **`file:`** URI, while this function's
    /// bare-colon fallback calls the same string **`git+file`**. So the
    /// single-colon spelling was validated as git and then fetched by
    /// `FileSource`, which opened the literal string as a path and failed with
    /// a not-found naming a path nobody wrote — after the control plane had
    /// just called it well-formed.
    ///
    /// Refusing at admission makes the ambiguous URI unrepresentable in the
    /// replicated log, rather than leaving it merely differently-routed.
    #[test]
    fn validate_rejects_the_single_colon_git_spelling() {
        for (uri, expected) in [
            ("git+file:/srv/r.git#main:m.json", "git+file://"),
            ("git+https:host/org/repo#main:m.json", "git+https://"),
        ] {
            let err = validate(&source_put("mocks", uri))
                .expect_err("the single-colon spelling must be refused at admission");
            assert!(
                err.contains(expected),
                "refusing {uri}: expected the message to name {expected:?}, got {err:?}"
            );
        }
    }

    /// A `://` later in the URI must not be mistaken for the authority.
    ///
    /// The two admission checks used to derive the scheme differently — one
    /// grammar-anchored, one "whatever precedes the first `://` anywhere" — so
    /// a single-colon git URI carrying a `://` in its ref or path produced a
    /// garbage scheme, matched no arm, and slipped through the unknown-scheme
    /// catch-all into the log, to fail every pull forever. Both now share
    /// `split_scheme`, which is the only reason the refusal above is total.
    #[test]
    fn a_later_double_slash_cannot_satisfy_the_authority_requirement() {
        for uri in [
            "git+file:/x#main:a://b",
            "git+file:/srv/r.git#main:https://example.com/x",
            "git+https:host/o/r#main:a://b",
        ] {
            let err = validate(&source_put("mocks", uri))
                .expect_err("a `://` in the fragment is not an authority");
            assert!(
                err.contains("single-colon"),
                "refusing {uri}: expected the spelling refusal, got {err:?}"
            );
        }
    }

    /// The other half of #301, and the reason the refusal above does not simply
    /// leave `git+file:` unusable.
    ///
    /// [`require_credential_free_uri`] runs first and refused *any* empty
    /// authority as "names no host" — but `file:///path` is RFC 8089's standard
    /// spelling of a local URL, whose authority is empty by design. So the
    /// documented `git+file://` form was already refused at admission before
    /// this issue, and refusing the single-colon form as well would have left
    /// the scheme with no admissible spelling at all. A host is a *shape*
    /// concern belonging to the per-scheme checks, not to credential hygiene:
    /// an empty authority cannot carry a credential, which is all that function
    /// is for.
    #[test]
    fn validate_accepts_the_empty_authority_of_a_local_url() {
        for uri in [
            "git+file:///srv/repos/mocks.git#main:imposters.json",
            "file:///srv/mocks/imposters.json",
        ] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
        }
        // Schemes that really do address a host keep the refusal.
        for uri in ["s3:///key", "registry://"] {
            assert!(
                validate(&source_put("mocks", uri)).is_err(),
                "an empty authority must still be refused for {uri}"
            );
        }
        // And so does a non-canonical case. Every other scheme comparison on
        // this path is case-sensitive, so folding case here would admit a URI
        // that then matches no shape check and no provider — committed, and
        // unfetchable forever.
        for uri in [
            "FILE:///srv/mocks/imposters.json",
            "GIT+FILE:///srv/repos/mocks.git#main:imposters.json",
        ] {
            assert!(
                validate(&source_put("mocks", uri)).is_err(),
                "an uppercase scheme must not be exempted: {uri}"
            );
        }
        // And an empty authority is still no excuse for a credential.
        assert!(
            validate(&source_put(
                "mocks",
                "git+file://u:p@/srv/r.git#main:m.json"
            ))
            .is_err(),
            "a credential-bearing authority must still be refused"
        );
    }

    /// The refusal must teach the shape without quoting the URI back. The
    /// function's doc states this rule for the whole family: these strings are
    /// rendered to the caller *and* into the admission log, and
    /// `require_credential_free_uri` deliberately permits a token in a query
    /// string, so echoing the URI would leak it.
    #[test]
    fn the_single_colon_refusal_does_not_echo_the_uri() {
        let uri = "git+https:host/org/repo?token=hunter2#main:m.json";
        let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
        assert!(
            !err.contains("hunter2"),
            "refusal leaked the query string: {err}"
        );
        assert!(!err.contains(uri), "refusal echoed the whole uri: {err}");
        assert!(err.contains("git+https://"), "{err}");
    }

    /// Same rule, hygiene side (issue #309): the "names no host" refusal fires
    /// on an empty authority — exactly the shape whose *query string* the
    /// hygiene check deliberately leaves alone, so a token there would ride an
    /// echo into the 400 body and the admission log. This was the one refusal
    /// in `require_credential_free_uri` that echoed its input.
    #[test]
    fn the_names_no_host_refusal_does_not_echo_the_uri() {
        for uri in [
            "s3:///key?token=hunter2",
            "git+https://#main:x?token=hunter2",
        ] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                !err.contains("hunter2"),
                "refusal leaked the query string: {err}"
            );
            assert!(!err.contains(uri), "refusal echoed the whole uri: {err}");
            // "opens an authority" pins *this* refusal: `sources::git` has an
            // echo-free "names no host" of its own that a reordering could
            // reach for the git row.
            assert!(err.contains("opens an authority"), "{err}");
            assert!(err.contains("names no host"), "{err}");
        }
    }

    // -- issue #136 review, B1: the git argument-injection refusal is
    // mirrored at admission, not only at fetch time ---------------------------

    /// A remote that `git fetch` would read as an option (`--upload-pack=…`
    /// runs its argument as a command) must never even reach the log, let
    /// alone get as far as a node actually shelling out to `git`.
    #[test]
    fn validate_refuses_a_git_remote_that_would_be_read_as_an_option() {
        let err = validate(&source_put(
            "mocks",
            "git+file:--upload-pack=/tmp/pwn.sh#main:x",
        ))
        .expect_err("an option-shaped remote must be refused at admission");
        // Since #301 this is caught one rule earlier, by the single-colon
        // refusal, and the message names the spelling rather than the option.
        // The shape is only *writable* in the single-colon form — with the `//`
        // spelling the remote always begins `//`, so it can never start with
        // `-` — which is why `check_remote`'s own guard is proven directly in
        // `sources::tests` instead of through admission.
        assert!(
            err.contains("git+file://"),
            "an option-shaped remote must still never reach the log: {err}"
        );
    }

    /// Same class of bug in the ref position: `git fetch <remote>
    /// --upload-pack=<cmd>` is just as effective as putting it in the remote.
    #[test]
    fn validate_refuses_a_git_ref_that_would_be_read_as_an_option() {
        let err = validate(&source_put(
            "mocks",
            "git+https://github.com/org/repo#--upload-pack=/tmp/pwn.sh:x",
        ))
        .expect_err("an option-shaped ref must be refused at admission");
        assert!(err.contains("option"), "{err}");
    }

    /// `ext::` is a transport helper: git runs its argument as a shell
    /// command. This must be refused before the op is committed, not only
    /// when a node's `git.rs` fetch happens to notice it.
    #[test]
    fn validate_refuses_a_git_transport_helper_remote() {
        // Written in the `//` spelling deliberately: `::` is the one
        // `check_remote` guard that survives it (the remote becomes
        // `//ext::sh -c whoami`, which still contains `::`), so this keeps
        // exercising the real guard through admission rather than being
        // short-circuited by the #301 spelling refusal.
        let err = validate(&source_put("mocks", "git+file://ext::sh -c whoami#main:x"))
            .expect_err("a transport helper remote must be refused at admission");
        assert!(err.contains("transport"), "{err}");
    }

    /// A `git+file:` remote must be an absolute path — the same positive
    /// shape rule `sources::git::check_remote` enforces at fetch time,
    /// mirrored here so a relative one never reaches the log either.
    #[test]
    fn validate_refuses_a_relative_git_file_remote() {
        for uri in ["git+file:relative/path#main:x", "git+file:relative#main:x"] {
            let err = validate(&source_put("mocks", uri))
                .expect_err("a relative git+file: remote must be refused");
            // As with the option-shaped remote above: since #301 the spelling
            // rule refuses these first, and a relative remote is only writable
            // in the single-colon form (the `//` spelling always yields a
            // `//`-prefixed, hence absolute, remote). `check_remote`'s
            // absolute-path guard is proven directly in `sources::tests`.
            assert!(err.contains("git+file://"), "{uri}: {err}");
        }
    }

    #[test]
    fn validate_rejects_a_malformed_s3_uri() {
        for (uri, expected) in [("s3://bucket", "no key"), ("s3://bucket/", "no key")] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                err.contains(expected),
                "refusing {uri}: expected {expected:?}, got {err:?}"
            );
        }
        // `s3:///key` has an empty authority, so the *hygiene* check refuses it
        // first with "names no host". That is a correct refusal and the order
        // that produces it is deliberate — see `validate`'s comment and
        // `a_malformed_credential_bearing_uri_is_refused_without_echoing_it`.
        assert!(validate(&source_put("mocks", "s3:///key")).is_err());
        assert_eq!(
            validate(&source_put("mocks", "s3://bucket/mocks/imposters.json")),
            Ok(())
        );
    }

    /// An empty service id would become a request to the registry's
    /// *collection* endpoint — a very different query from the one written.
    #[test]
    fn validate_rejects_a_malformed_registry_uri() {
        for uri in [
            "registry://",
            "registry://a,",
            "registry://,b",
            "registry://a,,b",
        ] {
            assert!(
                validate(&source_put("mocks", uri)).is_err(),
                "{uri} must be refused"
            );
        }
        assert_eq!(validate(&source_put("mocks", "registry://svc-a")), Ok(()));
        assert_eq!(
            validate(&source_put("mocks", "registry://svc-a,svc-b")),
            Ok(())
        );
    }

    /// The regression this pins: a URI that is **both** credential-bearing and
    /// malformed must be refused by the hygiene check, not the shape check.
    ///
    /// Shape errors are the more specific ones, which makes running them first
    /// tempting — and every existing credential test uses a *well-formed* URI,
    /// so nothing else here would notice. Running them first puts the operator's
    /// token into a 400 body and into the admission log.
    #[test]
    fn a_malformed_credential_bearing_uri_is_refused_without_echoing_it() {
        for uri in [
            // credentialed and missing its `#ref:path` entirely
            "git+https://oauth2:ghp_supersecret@github.com/o/r",
            // credentialed and its fragment names no path
            "git+https://oauth2:ghp_supersecret@github.com/o/r#main",
            // credentialed and names no key
            "s3://key-id:ghp_supersecret@bucket-only",
            // credentialed and its scheme is not the canonical lowercase —
            // hygiene runs first and parses the scheme case-insensitively,
            // so the lowercase-scheme refusal (#308) never sees the secret
            "GIT+HTTPS://oauth2:ghp_supersecret@github.com/o/r",
        ] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                !err.contains("ghp_supersecret"),
                "the refusal leaked the credential: {err}"
            );
            assert!(
                !err.contains(uri),
                "the refusal echoed the credential-bearing uri: {err}"
            );
            // Pin *which* check fired, not just that nothing leaked: every
            // shape refusal is also echo-free, so without this a reordering
            // of the two checks would pass silently.
            assert!(
                err.contains("auth_ref"),
                "expected the hygiene refusal, got a shape refusal: {err}"
            );
        }
    }

    /// Belt and braces on the rule above: no shape refusal echoes the URI at
    /// all, so a token hidden somewhere the hygiene check permits (a query
    /// string) cannot leak either.
    #[test]
    fn no_shape_refusal_echoes_the_uri() {
        for uri in [
            "git+https://host/o/r?token=ghp_supersecret",
            "s3://bucket?token=ghp_supersecret",
            "registry://a,,b?token=ghp_supersecret",
        ] {
            let err = validate(&source_put("mocks", uri)).expect_err("must be refused");
            assert!(
                !err.contains("ghp_supersecret"),
                "a shape refusal echoed a query-string token: {err}"
            );
        }
    }

    /// The shape check must not become a scheme allow-list: an embedder's own
    /// provider registers a scheme this crate has never heard of, and
    /// deterministic validation cannot know which those are.
    #[test]
    fn validate_leaves_an_unknown_scheme_alone() {
        for uri in [
            "custom://host/thing",
            "scripted:whatever",
            "file:/tmp/x.json",
            // the lowercase-scheme refusal (#308) applies to known schemes
            // only — an unknown scheme stays permissive in any case, and in
            // both the `://` and bare `scheme:` spellings
            "MyScheme://host/x",
            "MyScheme:/x",
        ] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
        }
    }

    /// Userinfo is only userinfo before the first `/`. A password-looking
    /// substring in a *path* or *query* is not a credential, and refusing it
    /// would make ordinary URIs unusable.
    #[test]
    fn validate_allows_an_at_sign_outside_the_authority() {
        for uri in [
            "https://host/teams/a@b/mocks.json",
            "https://host/x.json?owner=a@b",
            "file:/srv/a@b/mocks.json",
        ] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
        }
    }

    /// The authority is located by parsing the scheme from the front, not by
    /// searching for `://` anywhere in the string. A `://` in a *query* is not
    /// a scheme delimiter, and reading it as one would look past the real
    /// userinfo and admit a credential into the log.
    #[test]
    fn a_later_double_slash_cannot_be_mistaken_for_the_authority() {
        let err = validate(&source_put(
            "mocks",
            "https://user:pw@cfg.test/i.json?endpoint=https://minio.local",
        ))
        .expect_err("the credential is in the real authority");
        assert!(err.contains("auth_ref"), "{err}");

        // The same shape without a credential still parses to the real host.
        assert_eq!(
            validate(&source_put(
                "mocks",
                "https://cfg.test/i.json?endpoint=https://minio.local"
            )),
            Ok(())
        );
    }

    /// A path is a path however it is spelled. `file:` and bare forms both
    /// dispatch to the file provider upstream and have no authority, so an `@`
    /// in them is a filename character — refusing it would make ordinary paths
    /// unusable while protecting nothing, since no network provider is
    /// reachable without `://`.
    #[test]
    fn an_at_sign_in_a_path_only_uri_is_not_a_credential() {
        for uri in ["a@b/mocks.json", "file:a@b/mocks.json", "./a@b.json"] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
        }
    }

    #[test]
    fn validate_rejects_a_source_without_a_usable_uri() {
        for uri in ["", "   ", "https://"] {
            let err = validate(&source_put("mocks", uri))
                .expect_err("a source must name something fetchable");
            assert!(err.contains("uri"), "{err}");
        }
    }

    /// The id is a path segment in `/admin/sources/:id` and a redb key. A blank
    /// or slash-bearing id would address a different route than it names.
    #[test]
    fn validate_rejects_an_unusable_source_id() {
        for id in ["", " ", "a/b", "a?b", "a b", &"x".repeat(129)] {
            let err = validate(&source_put(id, "https://h/x.json"))
                .expect_err("id must be a usable path segment");
            assert!(err.contains("id"), "id {id:?}: {err}");
        }
        for id in ["mocks", "team-a.payments_v2", "A1"] {
            assert_eq!(
                validate(&source_put(id, "https://h/x.json")),
                Ok(()),
                "id {id}"
            );
        }
    }

    fn tracking_put(poll_secs: Option<u64>) -> ControlOp {
        ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: "mocks".to_owned(),
            uri: "https://h/x.json".to_owned(),
            mode: SourceMode::Tracking,
            auth_ref: None,
            on_drift: OnDrift::Overwrite,
            poll_secs,
        }
    }

    #[test]
    fn validate_accepts_a_tracking_source_at_or_above_the_poll_floor() {
        assert_eq!(validate(&tracking_put(Some(MIN_POLL_SECS))), Ok(()));
        assert_eq!(validate(&tracking_put(Some(300))), Ok(()));
    }

    /// The floor is a denial-of-service guard, not a style preference: the
    /// operator who typos `1` sees only that their mocks update promptly, while
    /// the fleet hammers someone else's host.
    #[test]
    fn validate_rejects_a_poll_interval_below_the_floor() {
        for secs in [0, 1, MIN_POLL_SECS - 1] {
            let err = validate(&tracking_put(Some(secs)))
                .expect_err("a sub-floor interval must be refused");
            assert!(
                err.contains(&MIN_POLL_SECS.to_string()),
                "the refusal must name the floor so it is actionable: {err}"
            );
        }
    }

    #[test]
    fn validate_rejects_a_tracking_source_with_no_interval() {
        let err = validate(&tracking_put(None))
            .expect_err("a polled source must say how often it is polled");
        assert!(err.contains("pollSecs"), "{err}");
    }

    /// Refused rather than ignored: silently accepting a poll interval on a
    /// pinned source is how an operator ends up believing their mocks track
    /// when nothing polls them.
    #[test]
    fn validate_rejects_a_poll_interval_on_a_pinned_source() {
        let op = ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: "mocks".to_owned(),
            uri: "https://h/x.json".to_owned(),
            mode: SourceMode::Pinned,
            auth_ref: None,
            on_drift: OnDrift::Overwrite,
            poll_secs: Some(60),
        };
        let err = validate(&op).expect_err("a pinned source is never polled");
        assert!(err.contains("tracking"), "{err}");
    }

    /// The op is log format: a `SourcePut` written before #135 has no
    /// `poll_secs` and must still decode — as a pinned source, which is what it
    /// necessarily was.
    #[test]
    fn a_pre_poll_source_put_still_decodes() {
        let legacy = json!({
            "SourcePut": {
                "tenant": "default",
                "id": "mocks",
                "uri": "https://h/x.json",
                "mode": "pinned",
                "on_drift": "overwrite",
            }
        });
        let op: ControlOp = serde_json::from_value(legacy).expect("a pre-#135 op decodes");
        match op {
            ControlOp::SourcePut {
                poll_secs, mode, ..
            } => {
                assert_eq!(poll_secs, None);
                assert_eq!(mode, SourceMode::Pinned);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_an_unusable_auth_ref() {
        let bad = ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: "mocks".to_owned(),
            uri: "https://h/x.json".to_owned(),
            mode: SourceMode::Pinned,
            auth_ref: Some(String::new()),
            on_drift: OnDrift::Overwrite,
            poll_secs: None,
        };
        let err = validate(&bad).expect_err("an empty credential name names nothing");
        assert!(err.contains("auth_ref"), "{err}");
    }

    /// A pull result carries configs into the log, so it is held to exactly the
    /// same config rules as a hand-written `PutImposter` — a source must not be
    /// a way to smuggle an unreplicable config past admission.
    #[test]
    fn validate_holds_pull_results_to_the_put_imposter_config_rules() {
        assert_eq!(validate(&pull_result("mocks", vec![*config(8080)])), Ok(()));

        let portless: ImposterConfig =
            serde_json::from_value(json!({ "protocol": "http" })).expect("parses");
        let err = validate(&pull_result("mocks", vec![portless]))
            .expect_err("auto-assign cannot replicate");
        assert!(err.contains("port"), "{err}");

        let bad_protocol: ImposterConfig =
            serde_json::from_value(json!({ "port": 1, "protocol": "smtp" })).expect("parses");
        let err = validate(&pull_result("mocks", vec![bad_protocol]))
            .expect_err("protocol outside http/https");
        assert!(err.contains("protocol"), "{err}");

        let dup_stubs: ImposterConfig = serde_json::from_value(json!({
            "port": 1,
            "protocol": "http",
            "stubs": [ { "id": "a" }, { "id": "a" } ],
        }))
        .expect("parses");
        let err = validate(&pull_result("mocks", vec![dup_stubs]))
            .expect_err("duplicate ids corrupt the stub-key diff");
        assert!(err.contains('a'), "{err}");
    }

    /// A document declaring the same port twice would apply as "last one wins"
    /// and leave the operator with one of the two imposters they wrote.
    #[test]
    fn validate_rejects_a_pull_result_that_declares_a_port_twice() {
        let err = validate(&pull_result("mocks", vec![*config(8080), *config(8080)]))
            .expect_err("a port may be declared once per document");
        assert!(err.contains("8080"), "{err}");
    }

    #[test]
    fn validate_rejects_a_pull_result_without_a_digest() {
        let op = ControlOp::SourcePullResult {
            tenant: TenantId::default(),
            id: "mocks".to_owned(),
            version: None,
            digest: Digest::new(""),
            configs: vec![],
        };
        let err = validate(&op).expect_err("the digest is what the short-circuit compares");
        assert!(err.contains("digest"), "{err}");
    }

    /// The log-entry size bound. A fetch is capped at the provider; this caps
    /// what reaches the log, which is the fleet-wide liability.
    #[test]
    fn an_oversize_pull_result_is_refused() {
        // One config whose serialized form comfortably exceeds the cap.
        let huge: ImposterConfig = serde_json::from_value(json!({
            "port": 8080,
            "protocol": "http",
            "name": "x".repeat(MAX_SOURCE_PAYLOAD_BYTES + 1),
        }))
        .expect("parses");
        let err = validate(&pull_result("mocks", vec![huge]))
            .expect_err("an oversize payload must never reach the log");
        assert!(
            err.contains(&MAX_SOURCE_PAYLOAD_BYTES.to_string()),
            "the refusal must name the bound: {err}"
        );
    }

    #[test]
    fn validate_accepts_source_ops_on_any_well_formed_tenant() {
        let ops = [
            ControlOp::SourcePut {
                tenant: TenantId::new("acme"),
                id: "mocks".to_owned(),
                uri: "https://h/x.json".to_owned(),
                mode: SourceMode::Pinned,
                auth_ref: None,
                on_drift: OnDrift::Overwrite,
                poll_secs: None,
            },
            ControlOp::SourceDelete {
                tenant: TenantId::new("acme"),
                id: "mocks".to_owned(),
            },
            ControlOp::SourcePullResult {
                tenant: TenantId::new("acme"),
                id: "mocks".to_owned(),
                version: None,
                digest: Digest::new("a".repeat(64)),
                configs: vec![],
            },
        ];
        for op in ops {
            assert_eq!(validate(&op), Ok(()), "{op:?}");
        }
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
        // (`is_source_name`) would be borrowed reasoning here.
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

    #[test]
    fn fleet_name_put_is_audited_as_cluster_admin() {
        // A rename is an operator act an incident timeline needs: "which fleet was this?" is
        // unanswerable afterwards if the rename itself left no row.
        assert_eq!(
            fleet_name("rift-prod-eu").audit_action(),
            Some("cluster.admin")
        );
    }
}
