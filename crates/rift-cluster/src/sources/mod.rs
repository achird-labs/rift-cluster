//! Imposter sources: fetch outside the log, apply inside it (issue #134).
//!
//! A source is a URI the fleet agrees backs some of its imposters. Turning that
//! URI into imposters is I/O against a system this cluster does not control, so
//! it happens **here**, on whichever node received the request — never in the
//! Raft apply path:
//!
//! 1. Apply must be deterministic and infallible (the #9 state-machine
//!    contract). A fetch is neither: it can time out, and two nodes fetching the
//!    same URI a second apart can legitimately get different bytes. A cluster
//!    whose replicas applied *different* configs from the "same" op would have
//!    diverged with nothing to point at.
//! 2. So the fetching node canonicalizes what it got, hashes it, and submits a
//!    [`ControlOp::SourcePullResult`] — an ordinary validated write that
//!    commits through the leader like any other. The fetched bytes enter the log
//!    exactly once, and every replica applies those same bytes.
//!
//! The fetch itself is not leader-privileged: any node may do it, and the
//! *write* forwards to the leader the way every op does. That is what makes
//! "fetch exactly once regardless of node count" true — followers never fetch,
//! they apply.
//!
//! A pull whose content matches what the source last applied produces **no log
//! entry at all** (the digest short circuit). Without it, a fleet polling a
//! stable document would grow its log forever and re-churn imposter state on
//! every round.
//!
//! ## Where these endpoints live
//!
//! The `/admin/sources*` routes are registered on the **cluster port** through
//! the `NodeConfig.routes` seam, beside `/_cluster/*` — not on the public admin
//! front. Sources are a control-plane object an operator manages, they are
//! authenticated with the cluster credential, and the cluster port is the
//! single authenticated listener that already carries exactly that.
//!
//! ## Credentials
//!
//! A source names a credential (`auth_ref`); it never carries one. A URI with
//! embedded credentials is refused by [`crate::control::validate`] before it can
//! reach the log. *Resolving* `auth_ref` into a request header ships with the
//! providers that need it (#136): upstream's `HttpSource` has no
//! header-injection seam, so a credentialed HTTPS fetch needs a new provider
//! rather than a hook here.
//!
//! Upstream's [`rift_ee::seams::ImposterSource::fetch`] is handed a [`SourceRef`], which carries
//! a URI and nothing else — while `auth_ref` lives *beside* the URI on the
//! record. [`CredentialedSource`] is the enterprise-side seam that closes that
//! gap: a provider that needs a credential is handed the ref's *name*, and
//! resolves it through [`auth::CredentialResolver`] at fetch time. The
//! alternatives were all worse in the same way — smuggling the name into the URI
//! collides with `git+https:`'s own `#ref:path` fragment, and a side table keyed
//! by URI races two sources that legitimately share one.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use rift_ee::seams::{FetchedImposters, ImposterConfig, SourceRef, SourceRegistry};
use uuid::Uuid;

use crate::control::{
    ControlOp, ControlOutcome, ControlRequest, Digest, OnDrift, SourceMode, TenantId,
};
use crate::raft::{NodeError, PullOutcome, RaftNode, SourceRecord};
use crate::rpc::{HandlerFuture, Router, RpcError};

/// How long a source write waits for *this node* to apply what the leader
/// committed, before answering from local state.
///
/// Every read-back here (render a created source, decide whether a pull was
/// skipped) answers from local applied state, and `submit` returns on the
/// leader's commit — so on a follower the read races the apply (#99). This is a
/// local wait only: no peer is consulted, and it is generous because guessing
/// wrong means reporting a durably committed write as absent.
const LOCAL_APPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// What a pull did, as reported to the caller.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullReport {
    /// The applying log index, or the source's last one when the digest short
    /// circuit meant nothing was written.
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub digest: String,
    /// True when the fetched content matched what the source last applied, so
    /// no log entry was written.
    pub unchanged: bool,
    /// True when the pull committed a decision *not* to apply: the source had
    /// drifted and its `on_drift` said `skip`. Distinct from `unchanged` — a
    /// skip did reach the log, and the fleet does not hold this content.
    pub skipped: bool,
    /// The ports this pull created, replaced or removed. Empty when nothing was
    /// applied (`unchanged` or `skipped`).
    pub changed: Vec<u16>,
    /// Anything the document declared that a clustered pull does not apply.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Why a pull could not be completed.
///
/// The split matters to the caller: [`Self::BadRequest`] is the operator's to
/// fix and will fail identically forever, while [`Self::Unavailable`] is the
/// cluster's and is worth retrying — which is exactly the 400/503 split the
/// handler renders.
#[derive(Debug, thiserror::Error)]
pub enum PullError {
    #[error("unknown source {0:?}")]
    UnknownSource(String),

    #[error("{0}")]
    BadRequest(String),

    /// The fetch itself failed: the URI is unreachable, refused, or served
    /// something that is not a config document.
    #[error("fetching source {id:?}: {detail}")]
    Fetch { id: String, detail: String },

    /// The write could not be committed. Carries the op id so a client can poll
    /// `GET /_cluster/ops/:id` for the outcome, the Ch. 4 write-path contract.
    #[error("{detail}")]
    Unavailable { detail: String, op_id: Uuid },

    #[error("{0}")]
    Internal(String),
}

/// A provider that authenticates with a named credential.
///
/// Deliberately *not* an extension of [`ImposterSource`]: a credentialed
/// provider must never be reachable through the plain `fetch` path, because
/// that path has no `auth_ref` to give it and would therefore fetch
/// anonymously. Keeping the two traits disjoint makes "fetched without the
/// credential it was configured with" unrepresentable rather than merely
/// unlikely.
pub trait CredentialedSource: Send + Sync {
    /// The schemes this provider claims, same contract as
    /// [`rift_ee::seams::ImposterSource::schemes`].
    fn schemes(&self) -> &'static [&'static str];

    /// Fetch `r`, authenticating with the credential `auth_ref` names.
    ///
    /// `None` means the source declared no `auth_ref` — an anonymous fetch,
    /// which is legitimate for a public repo or a bucket reached by an ambient
    /// role. A *named* ref that cannot be resolved is an error, never a
    /// fallback to anonymous: see [`auth`].
    fn fetch_with_auth<'a>(
        &'a self,
        r: &'a SourceRef,
        auth_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>,
    >;
}

/// Every provider a node can fetch through: upstream's scheme registry plus the
/// enterprise providers that take a credential.
///
/// Two maps rather than one because the two traits are disjoint by design (see
/// [`CredentialedSource`]). A scheme claimed by both is refused at build time,
/// for the same reason upstream refuses a doubly-claimed scheme: resolving it
/// by declaration order would let an enterprise provider silently shadow a
/// built-in, or vice versa, and the operator would find out from behaviour.
#[derive(Default)]
pub struct SourceProviders {
    upstream: SourceRegistry,
    credentialed: HashMap<String, Arc<dyn CredentialedSource>>,
}

impl SourceProviders {
    #[must_use]
    pub fn new(upstream: SourceRegistry) -> Self {
        Self {
            upstream,
            credentialed: HashMap::new(),
        }
    }

    /// Register a credentialed provider for every scheme it claims.
    ///
    /// # Errors
    /// If any of its schemes is already claimed, by either map.
    pub fn register_credentialed(
        &mut self,
        provider: Arc<dyn CredentialedSource>,
    ) -> anyhow::Result<()> {
        for scheme in provider.schemes() {
            if self.upstream.get(scheme).is_some() || self.credentialed.contains_key(*scheme) {
                anyhow::bail!(
                    "two imposter sources both claim the `{scheme}:` scheme; each scheme may have \
                     exactly one source"
                );
            }
            self.credentialed
                .insert((*scheme).to_string(), provider.clone());
        }
        Ok(())
    }

    /// Every scheme this build can fetch, sorted.
    #[must_use]
    pub fn schemes(&self) -> Vec<String> {
        let mut schemes: Vec<String> = self
            .upstream
            .schemes()
            .into_iter()
            .map(str::to_owned)
            .chain(self.credentialed.keys().cloned())
            .collect();
        schemes.sort();
        schemes
    }

    #[must_use]
    fn serves(&self, scheme: &str) -> bool {
        self.upstream.get(scheme).is_some() || self.credentialed.contains_key(scheme)
    }

    /// Whether `scheme`'s provider consumes a credential at all.
    ///
    /// Only the credentialed map can ever answer yes: an upstream
    /// [`rift_ee::seams::ImposterSource`] has no `auth_ref` parameter to give
    /// it, so a scheme served only there is anonymous by construction. This is
    /// what lets [`SourcePuller::uses_credential`] refuse an `auth_ref` that a
    /// scheme could never use, instead of silently dropping it.
    #[must_use]
    fn supports_credential(&self, scheme: &str) -> bool {
        self.credentialed.contains_key(scheme)
    }

    /// Every scheme that *does* consume a credential, sorted — what a refusal
    /// lists so the operator can see which schemes their `authRef` would have
    /// worked on.
    #[must_use]
    fn credentialed_schemes(&self) -> Vec<String> {
        let mut schemes: Vec<String> = self.credentialed.keys().cloned().collect();
        schemes.sort();
        schemes
    }

    /// Fetch `r` through whichever provider claims its scheme, handing the
    /// credential name only to a provider that takes one.
    async fn fetch(
        &self,
        r: &SourceRef,
        auth_ref: Option<&str>,
    ) -> Option<anyhow::Result<FetchedImposters>> {
        let scheme = r.scheme();
        if let Some(provider) = self.credentialed.get(scheme) {
            return Some(provider.fetch_with_auth(r, auth_ref).await);
        }
        let provider = self.upstream.get(scheme)?;
        Some(provider.fetch(r).await)
    }
}

impl From<SourceRegistry> for SourceProviders {
    fn from(upstream: SourceRegistry) -> Self {
        Self::new(upstream)
    }
}

/// Performs pulls for a node, against the schemes this build registers.
///
/// Bound late for the same reason [`crate::PullOnMissInterceptor`] is: the
/// registry and the routes it serves must exist before the node (the routes are
/// needed to bind the cluster port, whose address the node then advertises), so
/// the node arrives afterwards through [`Self::bind`]. Until then a pull answers
/// "not available yet" rather than silently doing nothing.
pub struct SourcePuller {
    registry: SourceProviders,
    node: OnceLock<Weak<RaftNode>>,
    /// Leader-local poll status from the tracking scheduler (#135), when one is
    /// running. Absent on a node with no scheduler (a test fixture, or an
    /// embedder that wired only explicit pulls) — and, by construction, empty
    /// on a follower, since only the leader polls.
    poll_status: OnceLock<Weak<scheduler::PollStatus>>,
}

impl std::fmt::Debug for SourcePuller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourcePuller")
            .field("schemes", &self.registry.schemes())
            .field("bound", &self.node.get().is_some())
            .finish()
    }
}

impl SourcePuller {
    /// Build a puller over `registry`. The registry is the node's own view of
    /// which schemes it can serve — deliberately *not* consulted by the
    /// deterministic op validation, which must not depend on per-node config.
    #[must_use]
    pub fn new(registry: impl Into<SourceProviders>) -> Self {
        Self {
            registry: registry.into(),
            node: OnceLock::new(),
            poll_status: OnceLock::new(),
        }
    }

    /// Publish the node to the puller. Weak for the same reason `NodeSlot` is:
    /// the node owns the task serving these handlers, so a strong reference
    /// would keep it alive forever and its `Drop` would never release the
    /// cluster port or the redb lock.
    ///
    /// Binding twice is a composition bug — the second node would be invisible
    /// to every source endpoint — so it is reported rather than ignored.
    pub fn bind(&self, node: &Arc<RaftNode>) -> Result<(), AlreadyBound> {
        self.node
            .set(Arc::downgrade(node))
            .map_err(|_| AlreadyBound)
    }

    /// Publish the scheduler's poll status so `GET /admin/sources/:id` can
    /// report why a tracking source is not advancing.
    ///
    /// `Weak`, like the node handle: the scheduler's lifetime is the node's,
    /// and the puller must not extend it. A second attach is ignored — the
    /// composition wires exactly one scheduler.
    pub fn attach_poll_status(&self, status: &Arc<scheduler::PollStatus>) {
        let _ = self.poll_status.set(Arc::downgrade(status));
    }

    /// The last poll error this node recorded for `id`, if any.
    #[must_use]
    pub fn last_poll_error(&self, id: &str) -> Option<String> {
        self.poll_status.get()?.upgrade()?.last_error(id)
    }

    /// Whether this build can serve the scheme `uri` names. Node-local
    /// knowledge, checked before an op is submitted so an operator is told "no
    /// provider for `git+https:`" instead of committing a source nothing can
    /// fetch.
    #[must_use]
    pub fn serves(&self, uri: &str) -> bool {
        self.registry.serves(SourceRef::new(uri).scheme())
    }

    /// Whether `uri`'s scheme is served by a provider that consumes a
    /// credential. Node-local knowledge, like [`Self::serves`] — checked
    /// before an op is submitted so an `authRef` that a scheme could never use
    /// is refused with a named reason instead of silently fetched anonymously
    /// forever.
    #[must_use]
    pub fn uses_credential(&self, uri: &str) -> bool {
        self.registry
            .supports_credential(SourceRef::new(uri).scheme())
    }

    /// Every scheme this build can authenticate to, sorted — what an
    /// `authRef`-on-an-anonymous-scheme refusal lists.
    #[must_use]
    pub fn credentialed_schemes(&self) -> Vec<String> {
        self.registry.credentialed_schemes()
    }

    /// The schemes this build registers, sorted — what an unknown-scheme
    /// refusal lists so the operator can see what they *could* have written.
    #[must_use]
    pub fn schemes(&self) -> Vec<String> {
        self.registry.schemes()
    }

    fn node(&self) -> Result<Arc<RaftNode>, PullError> {
        self.node
            .get()
            .ok_or_else(|| PullError::Internal("cluster node is not available yet".to_owned()))?
            .upgrade()
            .ok_or_else(|| PullError::Internal("cluster node is shutting down".to_owned()))
    }

    /// Fetch the named source and submit what it produced.
    ///
    /// `principal` is recorded on the op for audit — "who updated the mocks, and
    /// to which version" is a log query rather than a mystery.
    pub async fn pull(&self, id: &str, principal: Option<String>) -> Result<PullReport, PullError> {
        let node = self.node()?;
        let record = node
            .source(id)
            .map_err(|e| PullError::Internal(e.to_string()))?
            .ok_or_else(|| PullError::UnknownSource(id.to_owned()))?;

        let source_ref = SourceRef::new(record.uri.clone());
        let scheme = source_ref.scheme().to_owned();

        // The credential *name* travels with the fetch; the secret is resolved
        // inside the provider and never returns here, so it cannot reach the
        // op, the audit row below, or a `PullError` rendered to the caller.
        let fetched = self
            .registry
            .fetch(&source_ref, record.auth_ref.as_deref())
            .await
            .ok_or_else(|| {
                PullError::BadRequest(format!(
                    "no imposter source is registered for the `{scheme}:` scheme; this build \
                     serves: {}",
                    self.schemes().join(", ")
                ))
            })?
            .map_err(|e| PullError::Fetch {
                id: id.to_owned(),
                detail: e.to_string(),
            })?;

        let mut warnings = Vec::new();
        // The cluster refuses the TLS-MITM intercept listener fleet-wide
        // (`ConfigError::InterceptUnsupported`): its state is per-node and is
        // not replicated. A document that declares one is refused rather than
        // applied-minus-the-block, which would leave the operator with a
        // listener they configured and never got.
        if fetched.intercept.is_some() {
            return Err(PullError::BadRequest(format!(
                "source {id:?} declares an `intercept` block, which a clustered fleet cannot \
                 honour: intercept state is per-node and is not replicated"
            )));
        }
        // Routes are their own replicated object with their own op (#131). A
        // pull does not quietly rewrite the front door's table, but the
        // operator is told their block did nothing rather than left to wonder.
        if fetched.routes.is_some() {
            warnings.push(format!(
                "source {id:?} declares a `routes` block, which a clustered pull does not apply; \
                 replicate routes with PUT /front-door/routes"
            ));
        }

        let digest = digest_of(&fetched.configs).map_err(|e| {
            PullError::BadRequest(format!(
                "source {id:?} returned a config that will not encode: {e}"
            ))
        })?;

        // The no-change fast path: identical content writes nothing at all, so
        // a fleet re-pulling a stable document neither grows its log nor
        // re-churns imposter state.
        //
        // Two conditions, and both are load-bearing:
        //
        // * `applied_digest` — not `last_digest` — because a *skipped* pull
        //   also records the digest it saw, and short-circuiting on content the
        //   fleet never took would strand it.
        // * `!drifted`, because "the document has not changed" does not mean
        //   "the fleet matches it". A hand-edited source-owned imposter is
        //   exactly the case where the operator pulls to restore declared
        //   truth; answering `unchanged` there would make drift unrepairable
        //   except by editing the upstream document.
        if !record.drifted && record.applied_digest() == Some(digest.as_str()) {
            return Ok(PullReport {
                revision: record.revision,
                version: record.last_version,
                digest: digest.as_str().to_owned(),
                unchanged: true,
                skipped: false,
                changed: Vec::new(),
                warnings,
            });
        }

        let version = fetched.meta.version.clone();
        let changed = changed_ports(&record, &fetched.configs);
        let request = mint(
            principal.clone(),
            ControlOp::SourcePullResult {
                tenant: TenantId::default(),
                id: id.to_owned(),
                version: version.clone(),
                digest: digest.clone(),
                configs: fetched.configs,
            },
        );
        // Refused here, before the write, so a payload that could never apply
        // does not occupy a log entry on every replica first.
        if let Err(reason) = crate::control::validate(&request.op) {
            return Err(PullError::BadRequest(reason));
        }

        let op_id = request.op_id;
        let response = match node.submit(request).await {
            Ok(response) => response,
            Err(NodeError::Unavailable(detail)) => {
                return Err(PullError::Unavailable {
                    detail: format!("no quorum / leader unreachable: {detail}"),
                    op_id,
                });
            }
            Err(e) => return Err(PullError::Internal(e.to_string())),
        };

        match response.outcome {
            ControlOutcome::Applied => {}
            // A committed refusal: the write succeeded, the state machine
            // declined it — a drifted source under `on_drift: fail`, or a
            // source deleted mid-flight. The operator's to resolve.
            ControlOutcome::Failed { reason } => return Err(PullError::BadRequest(reason)),
        }

        // Wait for *this* node to apply what the leader committed before
        // reading back what happened. Without it the read below races the local
        // apply on a follower, exactly as `await_local_applied`'s own doc
        // describes (#99).
        node.await_local_applied(response.revision, LOCAL_APPLY_TIMEOUT)
            .await;

        // `Applied` means the op was committed and ran — not that it changed
        // anything. A drifted source under `on_drift: skip` commits a decision
        // to hold off, and reporting that as an apply would put a false
        // "source pull applied" line in the audit log and name ports in
        // `changed` that were never touched.
        let skipped = node
            .source(id)
            .map_err(|e| PullError::Internal(e.to_string()))?
            .is_some_and(|after| {
                after.revision == response.revision
                    && after.last_outcome == Some(PullOutcome::Skipped)
            });

        // The audit row the issue asks for: who pulled what, to which version,
        // at which revision, and whether it landed. `target: "audit"` so a
        // deployment can route these somewhere durable without also shipping
        // every debug line.
        tracing::info!(
            target: "audit",
            event = "source.pull",
            source_id = %id,
            principal = principal.as_deref().unwrap_or("-"),
            version = version.as_deref().unwrap_or("-"),
            digest = %digest,
            revision = response.revision,
            outcome = if skipped { "skipped" } else { "applied" },
            "source pull committed"
        );
        Ok(PullReport {
            revision: response.revision,
            version,
            digest: digest.as_str().to_owned(),
            unchanged: false,
            skipped,
            changed: if skipped { Vec::new() } else { changed },
            warnings,
        })
    }

    /// Declare a source, then pull it — the `--imposters` bootstrap path.
    ///
    /// Idempotent by id: re-running the same command upserts the same record
    /// rather than accumulating near-duplicates, and the digest short circuit
    /// then makes the pull a no-op when nothing changed.
    pub async fn declare_and_pull(
        &self,
        id: &str,
        uri: &str,
        on_drift: OnDrift,
    ) -> Result<PullReport, PullError> {
        let node = self.node()?;
        if !self.serves(uri) {
            return Err(PullError::BadRequest(format!(
                "no imposter source is registered for the `{}:` scheme (from {uri:?}); this build \
                 serves: {}",
                SourceRef::new(uri).scheme(),
                self.schemes().join(", ")
            )));
        }
        let request = mint(
            None,
            ControlOp::SourcePut {
                tenant: TenantId::default(),
                id: id.to_owned(),
                uri: uri.to_owned(),
                mode: SourceMode::Pinned,
                auth_ref: None,
                on_drift,
                poll_secs: None,
            },
        );
        if let Err(reason) = crate::control::validate(&request.op) {
            return Err(PullError::BadRequest(reason));
        }
        let op_id = request.op_id;
        let response = match node.submit(request).await {
            Ok(response) => match response.outcome {
                ControlOutcome::Applied => response,
                ControlOutcome::Failed { reason } => return Err(PullError::BadRequest(reason)),
            },
            Err(NodeError::Unavailable(detail)) => {
                return Err(PullError::Unavailable { detail, op_id });
            }
            Err(e) => return Err(PullError::Internal(e.to_string())),
        };
        // `submit` returns once the *leader* has committed; on a follower this
        // node's own apply still lags. `pull` reads the source from local
        // applied state, so without this wait a node joining with `--imposters`
        // would read "unknown source" for a source it just committed — and,
        // because a bootstrap failure fails the start, would refuse to boot
        // under ordinary replication lag.
        node.await_local_applied(response.revision, LOCAL_APPLY_TIMEOUT)
            .await;
        self.pull(id, None).await
    }
}

/// Binding an already-bound [`SourcePuller`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the source puller was bound to a node twice")]
pub struct AlreadyBound;

/// The ports a pull would create, replace or remove, from the source's current
/// ownership and the document it just fetched.
fn changed_ports(record: &SourceRecord, fetched: &[ImposterConfig]) -> Vec<u16> {
    let declared: std::collections::BTreeSet<u16> = fetched.iter().filter_map(|c| c.port).collect();
    let owned: std::collections::BTreeSet<u16> = record.ports.iter().copied().collect();
    declared.union(&owned).copied().collect()
}

fn mint(principal: Option<String>, op: ControlOp) -> ControlRequest {
    ControlRequest {
        op_id: Uuid::new_v4(),
        principal,
        // A pre-epoch clock mints 0, which only makes this op read as already
        // old to the cluster's logical clock — it weakens this op's dedup TTL
        // and nothing else, so it is not worth a panic path.
        issued_at_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        expected_revision: None,
        op,
    }
}

/// The content digest of a fetched config set: SHA-256 over a canonical JSON
/// encoding, as lowercase hex.
///
/// The encoding is written out here rather than delegated to `serde_json`'s
/// default map ordering. `serde_json::Map` is a sorted `BTreeMap` only while
/// nothing in the dependency graph enables its `preserve_order` feature — and a
/// digest whose value depends on Cargo feature unification is exactly the
/// cross-node disagreement this whole module exists to prevent. Configs are
/// ordered by port first, so two fetches that differ only in document order
/// hash the same and the short circuit still fires.
///
/// Fallible on purpose. A config that will not encode cannot be hashed *as
/// itself*, and the tempting fallback — folding the error text into the hash —
/// is a data-path swallow with teeth: two different broken documents would
/// collapse to one digest, and the no-change short circuit would then read a
/// genuinely changed document as unchanged and quietly stop applying it. This
/// runs on bytes an external source served, before validation, so "it cannot
/// happen" is not an assumption worth encoding.
pub fn digest_of(configs: &[ImposterConfig]) -> Result<Digest, serde_json::Error> {
    use sha2::{Digest as _, Sha256};

    let mut values: Vec<(Option<u16>, String)> = configs
        .iter()
        .map(|config| Ok((config.port, canonical(&serde_json::to_value(config)?))))
        .collect::<Result<_, serde_json::Error>>()?;
    values.sort();

    let mut hasher = Sha256::new();
    for (port, encoded) in &values {
        // The port is hashed, not merely used for ordering: it is the identity
        // the apply arm diffs on, so two documents differing only there must
        // not collide.
        hasher.update(port.unwrap_or_default().to_be_bytes());
        hasher.update(encoded.as_bytes());
        hasher.update([0]);
    }
    Ok(Digest::new(format!("{:x}", hasher.finalize())))
}

/// A stable, readable, charset-safe source id derived from a URI — what
/// `--imposters` names the sources it declares at bootstrap.
///
/// Deterministic from the URI, which is what "idempotent by id" needs: the same
/// command on a restart, or on a second node booting with the same flags,
/// upserts the same record instead of accumulating near-duplicates. The hash
/// suffix keeps two URIs that slugify alike (`.../a/mocks.json` and
/// `.../b/mocks.json`) off the same id, which would otherwise leave them
/// silently fighting over the same ports.
#[must_use]
pub fn bootstrap_id(uri: &str) -> String {
    use sha2::{Digest as _, Sha256};

    let mut slug = String::with_capacity(uri.len());
    let mut last_dash = false;
    for c in uri.chars() {
        if c.is_ascii_alphanumeric() {
            slug.extend(c.to_lowercase());
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    let digest = format!("{:x}", Sha256::digest(uri.as_bytes()));
    // Bounded well under the 128-character id limit `validate` enforces.
    let head: String = slug.chars().take(48).collect();
    let head = head.trim_matches('-');
    format!("{head}-{}", &digest[..8])
}

/// A JSON value rendered with object keys in sorted order, recursively.
fn canonical(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let body: Vec<String> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::Value::String(key.clone()),
                        canonical(&map[key])
                    )
                })
                .collect();
            format!("{{{}}}", body.join(","))
        }
        serde_json::Value::Array(items) => {
            let body: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", body.join(","))
        }
        scalar => scalar.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Admin surface
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceBody {
    id: String,
    uri: String,
    #[serde(default)]
    mode: SourceMode,
    #[serde(default)]
    auth_ref: Option<String>,
    #[serde(default)]
    on_drift: OnDrift,
    /// Poll cadence for a `tracking` source (#135). `validate` enforces the
    /// mode/interval pairing, so this is carried through verbatim rather than
    /// second-guessed here.
    #[serde(default)]
    poll_secs: Option<u64>,
}

/// Register the source endpoints onto `base` (see the module doc for why they
/// ride the cluster port):
///
/// ```text
/// POST   /admin/sources            { id, uri, mode?, authRef?, onDrift? }
/// GET    /admin/sources            → every source, with drift + last pull
/// GET    /admin/sources/:id        · DELETE /admin/sources/:id
/// POST   /admin/sources/:id/pull   → { revision, version, changed: [...] }
/// ```
#[must_use]
pub fn routes(base: Router, puller: Arc<SourcePuller>) -> Router {
    let create = Arc::clone(&puller);
    let list = Arc::clone(&puller);
    let by_id = Arc::clone(&puller);
    let delete = Arc::clone(&puller);
    let pull = puller;

    base.route(
        "POST",
        "/admin/sources",
        Arc::new(move |body: Vec<u8>| -> HandlerFuture {
            let puller = Arc::clone(&create);
            Box::pin(async move { create_source(&puller, &body).await })
        }),
    )
    .route(
        "GET",
        "/admin/sources",
        Arc::new(move |_body: Vec<u8>| -> HandlerFuture {
            let puller = Arc::clone(&list);
            Box::pin(async move {
                let node = puller.node().map_err(pull_error)?;
                let sources = node.sources().map_err(handler_error)?;
                serde_json::to_vec(&serde_json::json!({ "sources": sources }))
                    .map_err(handler_error)
            })
        }),
    )
    // One prefix handler for both `/admin/sources/:id` and
    // `/admin/sources/:id/pull`: the router dispatches on method + prefix, so
    // the trailing `/pull` is the handler's to read.
    .route_prefix(
        "GET",
        "/admin/sources/",
        Arc::new(move |suffix: String, _body: Vec<u8>| -> HandlerFuture {
            let puller = Arc::clone(&by_id);
            Box::pin(async move { read_source(&puller, &suffix).await })
        }),
    )
    .route_prefix(
        "DELETE",
        "/admin/sources/",
        Arc::new(move |suffix: String, _body: Vec<u8>| -> HandlerFuture {
            let puller = Arc::clone(&delete);
            Box::pin(async move { delete_source(&puller, &suffix).await })
        }),
    )
    .route_prefix(
        "POST",
        "/admin/sources/",
        Arc::new(move |suffix: String, _body: Vec<u8>| -> HandlerFuture {
            let puller = Arc::clone(&pull);
            Box::pin(async move { pull_source(&puller, &suffix).await })
        }),
    )
}

/// Refuse an `auth_ref` that names a credential no provider for `uri`'s scheme
/// would ever consume (issue #136 review, B4).
///
/// `SourceProviders::fetch` only ever hands `auth_ref` to a scheme in the
/// credentialed map; a scheme served only through the upstream
/// [`rift_ee::seams::ImposterSource`] path has no seam to receive it at all.
/// Before this check, `POST /admin/sources` with `{ uri: "https://…",
/// authRef: "tok" }` was accepted and then fetched **anonymously forever** —
/// silently, since nothing about the write or a subsequent pull ever fails.
/// Refused rather than ignored: that silent drop is exactly how an operator
/// ends up believing a source is authenticated when it never was, right up
/// until whatever it serves anonymously stops matching what the credentialed
/// path would have returned.
///
/// Node-local, like [`SourcePuller::serves`] right beside it: which schemes
/// take a credential is per-node provider configuration, so this must not
/// move into [`crate::control::validate`], which has to give the same answer
/// on every replica regardless of which providers that replica happens to
/// have registered.
fn check_credential_use(
    puller: &SourcePuller,
    auth_ref: Option<&str>,
    uri: &str,
) -> Result<(), RpcError> {
    if auth_ref.is_some() && !puller.uses_credential(uri) {
        return Err(RpcError::BadRequest(format!(
            "authRef is set, but the `{}:` scheme does not consume a credential; only these \
             schemes take one: {}",
            SourceRef::new(uri).scheme(),
            puller.credentialed_schemes().join(", ")
        )));
    }
    Ok(())
}

async fn create_source(puller: &SourcePuller, body: &[u8]) -> Result<Vec<u8>, RpcError> {
    let parsed: SourceBody = serde_json::from_slice(body)
        .map_err(|e| RpcError::BadRequest(format!("source body: {e}")))?;
    let node = puller.node().map_err(pull_error)?;

    // Node-local, and therefore *not* part of deterministic op validation:
    // which schemes a node serves is per-node configuration, so checking it
    // inside `apply` would let two replicas disagree about a committed op.
    if !puller.serves(&parsed.uri) {
        return Err(RpcError::BadRequest(format!(
            "no imposter source is registered for the `{}:` scheme; this build serves: {}",
            SourceRef::new(&parsed.uri).scheme(),
            puller.schemes().join(", ")
        )));
    }
    check_credential_use(puller, parsed.auth_ref.as_deref(), &parsed.uri)?;

    let request = mint(
        None,
        ControlOp::SourcePut {
            tenant: TenantId::default(),
            id: parsed.id.clone(),
            uri: parsed.uri,
            mode: parsed.mode,
            auth_ref: parsed.auth_ref,
            on_drift: parsed.on_drift,
            poll_secs: parsed.poll_secs,
        },
    );
    // Refused before the submit, so a credential-bearing URI never reaches the
    // log at all — not even as a committed `Failed` entry, which would keep the
    // secret on every replica's disk and in every snapshot.
    if let Err(reason) = crate::control::validate(&request.op) {
        return Err(RpcError::BadRequest(reason));
    }

    let op_id = request.op_id;
    let response = match node.submit(request).await {
        Ok(response) => response,
        Err(NodeError::Unavailable(detail)) => {
            return Err(RpcError::Unavailable {
                detail: format!("no quorum / leader unreachable: {detail}"),
                op_id: Some(op_id.to_string()),
            });
        }
        Err(e) => return Err(RpcError::Handler(e.to_string())),
    };
    if let ControlOutcome::Failed { reason } = response.outcome {
        return Err(RpcError::BadRequest(reason));
    }
    // Render by reading back what was committed — so wait for *this* node to
    // apply it first. The same #99 reasoning as the admin front's write
    // barrier: a 404 for a write that just succeeded is indistinguishable from
    // "no such source". The `Unavailable` below is the honest answer if the
    // apply genuinely did not land in time, and it names the op to poll.
    node.await_local_applied(response.revision, LOCAL_APPLY_TIMEOUT)
        .await;
    let record = node
        .source(&parsed.id)
        .map_err(handler_error)?
        .ok_or_else(|| RpcError::Unavailable {
            detail: "source committed but not yet applied on this node".to_owned(),
            op_id: Some(op_id.to_string()),
        })?;
    serde_json::to_vec(&record).map_err(handler_error)
}

async fn read_source(puller: &SourcePuller, suffix: &str) -> Result<Vec<u8>, RpcError> {
    let id = path_id(suffix)?;
    let node = puller.node().map_err(pull_error)?;
    let record = node
        .source(id)
        .map_err(handler_error)?
        .ok_or_else(|| unknown_route("GET", id))?;
    // The durable record says what the fleet holds; the poll status says why a
    // tracking source might not be advancing. An operator looking at a stale
    // `lastVersion` needs the second to interpret the first — and a poll
    // failure is deliberately never written to the log (#135), so this is the
    // only place it surfaces.
    let mut rendered = serde_json::to_value(&record).map_err(handler_error)?;
    if let Some(error) = puller.last_poll_error(id)
        && let Some(object) = rendered.as_object_mut()
    {
        object.insert("lastPollError".to_owned(), serde_json::Value::String(error));
    }
    serde_json::to_vec(&rendered).map_err(handler_error)
}

async fn delete_source(puller: &SourcePuller, suffix: &str) -> Result<Vec<u8>, RpcError> {
    let id = path_id(suffix)?;
    let node = puller.node().map_err(pull_error)?;
    let request = mint(
        None,
        ControlOp::SourceDelete {
            tenant: TenantId::default(),
            id: id.to_owned(),
        },
    );
    if let Err(reason) = crate::control::validate(&request.op) {
        return Err(RpcError::BadRequest(reason));
    }
    let op_id = request.op_id;
    let response = match node.submit(request).await {
        Ok(response) => response,
        Err(NodeError::Unavailable(detail)) => {
            return Err(RpcError::Unavailable {
                detail: format!("no quorum / leader unreachable: {detail}"),
                op_id: Some(op_id.to_string()),
            });
        }
        Err(e) => return Err(RpcError::Handler(e.to_string())),
    };
    if let ControlOutcome::Failed { reason } = response.outcome {
        return Err(RpcError::BadRequest(reason));
    }
    serde_json::to_vec(&serde_json::json!({ "revision": response.revision })).map_err(handler_error)
}

async fn pull_source(puller: &SourcePuller, suffix: &str) -> Result<Vec<u8>, RpcError> {
    let id = suffix
        .split('?')
        .next()
        .unwrap_or_default()
        .strip_suffix("/pull")
        .ok_or_else(|| unknown_route("POST", suffix))?;
    if id.is_empty() || id.contains('/') {
        return Err(unknown_route("POST", suffix));
    }
    let report = puller.pull(id, None).await.map_err(pull_error)?;
    serde_json::to_vec(&report).map_err(handler_error)
}

/// The id segment of a `/admin/sources/<id>` suffix, rejecting anything with a
/// further path element — `/admin/sources/a/b` names no source.
fn path_id(suffix: &str) -> Result<&str, RpcError> {
    let id = suffix.split('?').next().unwrap_or_default();
    if id.is_empty() || id.contains('/') {
        return Err(unknown_route("GET", suffix));
    }
    Ok(id)
}

fn unknown_route(method: &str, id: &str) -> RpcError {
    RpcError::UnknownRoute {
        method: method.to_owned(),
        path: format!("/admin/sources/{id}"),
    }
}

fn handler_error(e: impl std::fmt::Display) -> RpcError {
    RpcError::Handler(e.to_string())
}

/// Map a pull failure onto the transport's status classes. The distinction is
/// the point: a `BadRequest` will fail identically on every retry, while an
/// `Unavailable` is the cluster's problem and carries the op id to poll.
fn pull_error(e: PullError) -> RpcError {
    match e {
        PullError::UnknownSource(id) => unknown_route("POST", &id),
        PullError::BadRequest(detail) => RpcError::BadRequest(detail),
        // A failed fetch is the *upstream* source's fault, not this cluster's,
        // and retrying can genuinely succeed — so it is reported as a bad
        // gateway rather than as the operator's malformed request.
        PullError::Fetch { id, detail } => {
            RpcError::Transport(format!("fetching source {id:?}: {detail}"))
        }
        PullError::Unavailable { detail, op_id } => RpcError::Unavailable {
            detail,
            op_id: Some(op_id.to_string()),
        },
        PullError::Internal(detail) => RpcError::Handler(detail),
    }
}

pub mod auth;
mod common;
pub mod git;
pub mod registry;
pub mod s3;
pub mod scheduler;

// `provider_tests.rs` is the immutable gate for issue #136 (see its own module
// doc) and is not edited to satisfy lints — its stub HTTP server's callback
// type is exactly as complex as a hand-rolled fixture needs to be, so the lint
// is silenced at this declaration rather than by reshaping test-only code that
// must stay byte-for-byte the gate it was reviewed as.
#[cfg(test)]
#[allow(clippy::type_complexity)]
mod provider_tests;

#[cfg(test)]
mod tests;
