//! The control-plane op set (ADR-001 §4.1): what an admin mutation becomes in the
//! Raft log, and the deterministic pure logic the state machine runs before it
//! mutates anything.
//!
//! Everything here must be deterministic across nodes: the same committed
//! [`ControlRequest`] against the same state-machine state yields the same
//! [`ControlResponse`] and the same table mutation on every replica. Anything
//! that can differ per node (port binds, listener state) lives in the engine
//! drive *after* apply, never here.

use rift_ee::seams::{ImposterConfig, RouteTable, Stub};
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
    /// Whole-table replace of the front door's route table (issue #19 / U-11,
    /// enterprise #131). Never a partial merge: [`RouteTable::validate`]
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
            validate_replicable_config(config)
        }
        ControlOp::PatchStubs { tenant, .. } | ControlOp::DeleteImposter { tenant, .. } => {
            require_default_tenant(tenant)
        }
        ControlOp::DeleteAll { tenant } => require_default_tenant(tenant),
        ControlOp::SetEnabled { tenant, .. } => require_default_tenant(tenant),
        ControlOp::PutRoutes { tenant, table } => {
            require_default_tenant(tenant)?;
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
        // beyond the tenant gate.
        ControlOp::DeleteRoute { tenant, .. } => require_default_tenant(tenant),
        ControlOp::SourcePut {
            tenant,
            id,
            uri,
            mode,
            auth_ref,
            poll_secs,
            ..
        } => {
            require_default_tenant(tenant)?;
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
            require_default_tenant(tenant)?;
            require_source_id(id)
        }
        ControlOp::SourcePullResult {
            tenant,
            id,
            digest,
            configs,
            ..
        } => {
            require_default_tenant(tenant)?;
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
/// The scheme is parsed properly rather than by searching for `://` anywhere in
/// the string, because "anywhere" finds the wrong one:
/// `s3:key@bucket/p?endpoint=https://minio.local` would have its *query* read
/// as the authority, see no `@`, and admit a credential into the log. Parsing
/// from the front means the delimiter has to be where a scheme delimiter
/// actually is.
fn require_credential_free_uri(uri: &str) -> Result<(), String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err("source uri must not be empty".to_owned());
    }
    let scheme_len = uri
        .find(':')
        .filter(|end| {
            // RFC 3986: ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
            let scheme = &uri[..*end];
            scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        })
        .map_or(0, |end| end + 1);
    // Everything before the first `/` (or query/fragment) of the hier-part,
    // and only when the hier-part actually opens with `//`.
    let Some(hier) = uri[scheme_len..].strip_prefix("//") else {
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
        return Err(format!("source uri {uri:?} names no host"));
    }
    Ok(())
}

/// Per-scheme URI *shape* checks for the enterprise providers (#136), so a URI
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
/// the admin handler already makes before submitting.
///
/// **No message here echoes the URI.** [`require_credential_free_uri`] runs
/// first and guarantees the *authority* holds no credential, but it
/// deliberately permits an `@` — and therefore a token — in a query string, and
/// these refusals are rendered straight back to the caller and into the
/// admission log. Naming the scheme and the shape that was expected is just as
/// actionable to the operator who wrote the URI, and cannot leak.
fn require_well_formed_uri(uri: &str) -> Result<(), String> {
    let uri = uri.trim();
    let scheme = match uri.split_once("://") {
        Some((scheme, _)) => scheme,
        None => uri.split_once(':').map_or("", |(scheme, _)| scheme),
    };

    match scheme {
        "git+https" | "git+file" => {
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
        // No `expected_revision` support for routes (yet): `PutRoutes` is a
        // whole-table replace with no single stored record to condition on,
        // and a per-route precondition is future work if a single-route
        // upsert op ever lands.
        | ControlOp::PutRoutes { .. }
        | ControlOp::DeleteRoute { .. }
        // A source op addresses a source, not an imposter: `expected_revision`
        // is defined against `sm_configs` rows, so there is no record here for
        // a precondition to hold against.
        | ControlOp::SourcePut { .. }
        | ControlOp::SourceDelete { .. }
        | ControlOp::SourcePullResult { .. }
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

    // -- validate: PutRoutes / DeleteRoute -------------------------------------

    use rift_ee::seams::{Route, RouteMatch, RouteTarget};

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
    fn validate_rejects_put_routes_on_a_non_default_tenant() {
        let op = ControlOp::PutRoutes {
            tenant: TenantId::new("acme"),
            table: RouteTable::default(),
        };
        let err = validate(&op).expect_err("non-default tenant must be rejected");
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
    fn validate_accepts_delete_route_on_the_default_tenant_only() {
        let op = ControlOp::DeleteRoute {
            tenant: TenantId::default(),
            id: "any".to_owned(),
        };
        assert_eq!(validate(&op), Ok(()));

        let op = ControlOp::DeleteRoute {
            tenant: TenantId::new("acme"),
            id: "any".to_owned(),
        };
        validate(&op).expect_err("non-default tenant is still refused");
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
            "git+file:/srv/repos/mocks.git#main:imposters.json",
        ] {
            assert_eq!(validate(&source_put("mocks", uri)), Ok(()), "uri {uri}");
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
        assert!(err.contains("option"), "{err}");
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
        let err = validate(&source_put("mocks", "git+file:ext::sh -c whoami#main:x"))
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
            assert!(err.contains("absolute"), "{uri}: {err}");
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
    fn validate_rejects_source_ops_on_a_non_default_tenant() {
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
            let err = validate(&op).expect_err("non-default tenant must be rejected");
            assert!(err.contains("tenant"), "{err}");
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
