//! The MCP server's client half: a thin typed wrapper over `reqwest` pointed at a
//! remote admin front.
//!
//! This process holds no node state and embeds no engine — it is a client, and every
//! tool is one admin API call (RFC-006 §8.1).

use std::time::Duration;

use reqwest::{Method, StatusCode, Url};
use serde::{Serialize, Serializer};

use super::args::{ApiKey, McpArgs, StartupError};

// The admin front's own header names, not re-spelled literals: `decorate` is where
// they are defined and documented, and a second copy here is a drift point that
// would fail silently (a dropped marker reads exactly like a complete answer).
use rift_cluster::decorate::{
    HEADER_NEXT_INDEX, HEADER_OP_ID, HEADER_PARTIAL, HEADER_REVISION, HEADER_TRUNCATED,
    HEADER_WARNINGS,
};
use uuid::Uuid;

/// Whether an answer describes the whole fleet or only the node that served it.
///
/// An enum rather than a bare string because the distinction is conditional
/// (see [`Answer`]) and a call site that spells it wrong tells an agent a node
/// read is a fleet read — the exact dishonesty #292 was written to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ReadScope {
    /// Merged across the fleet (`savedRequests` merge-on-read, #223/#225/#229).
    Fleet,
    /// Served by the answering node alone.
    Node,
}

/// A tool answer plus the provenance an agent needs to trust it.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct Answer<T> {
    pub data: T,
    /// Absent for reads where the fleet/node distinction does not arise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<ReadScope>,
    /// The `Rift-Cluster-Partial` header, verbatim, when the front stamped one.
    ///
    /// `Option` rather than a defaulted empty string: the header is defined as
    /// present-only-when-partial, so its absence is a domain value ("complete"),
    /// not a missing field to paper over.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<String>,
    /// `x-rift-next-index` — the opaque cursor to pass back as `since`.
    ///
    /// Without this the `since` parameter is undocumentable in practice: an agent
    /// has no other way to obtain a valid cursor, so a journal larger than one page
    /// simply cannot be paged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_index: Option<String>,
    /// `x-rift-truncated` — set when retention dropped entries from this answer.
    ///
    /// Additive-only upstream (present or absent, never `false`). Dropping it would
    /// present a short answer as a complete one, which is the same silence `partial`
    /// exists to prevent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
    /// The revision this record is at, from `Rift-Cluster-Revision` (issue #293).
    ///
    /// The number an agent feeds back as `expected_revision` on its next write. Without
    /// it the revision loop is undocumentable in practice: the agent has no other way to
    /// obtain a value to condition on, exactly as `next_index` is the only way to obtain
    /// a cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_revision: Option<u64>,
}

impl<T> Answer<T> {
    /// An answer with no provenance to report — nothing the front stamped, and no
    /// fleet/node distinction to draw.
    pub fn plain(data: T) -> Self {
        Self {
            data,
            scope: None,
            partial: None,
            next_index: None,
            truncated: None,
            current_revision: None,
        }
    }

    /// An answer carrying every marker the front stamped.
    ///
    /// The single construction path for tool answers, so a new marker cannot be
    /// added to [`Markers`] and then silently forgotten at six of seven call sites.
    ///
    /// [`Markers::warnings`] is the deliberate exception: the front stamps
    /// `Rift-Cluster-Warnings` only on the mutation path, so it belongs to
    /// [`WriteOutcome::Applied`] and never to a read. Adding it here would advertise a
    /// field that is always absent.
    pub fn new(data: T, scope: Option<ReadScope>, markers: Markers) -> Self {
        Self {
            data,
            scope,
            current_revision: markers.revision.as_deref().and_then(revision_of),
            partial: markers.partial,
            next_index: markers.next_index,
            truncated: markers.truncated,
        }
    }
}

/// A tool call that failed. Becomes a structured MCP tool error, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum ToolFailure {
    /// The admin front could not be reached at all.
    #[error("could not reach the admin API at {url}: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// The credential was rejected.
    ///
    /// Split out from [`Self::Api`] only so the message can name the two causes a
    /// human actually hits. The body is the API's own and never echoes credentials.
    #[error(
        "the admin API rejected this key (401). Check the key file's contents, and note that \
         the header is the raw key — a `Bearer ` prefix is a different credential. Response: {body}"
    )]
    Unauthorized { body: String },

    /// Any other non-2xx. The body is relayed **verbatim** (RFC-006 §9.4): the API's
    /// error bodies never contain credentials, and summarising them here would strip
    /// the detail the agent needs to correct itself.
    #[error("the admin API answered {status}: {body}")]
    Api { status: u16, body: String },

    /// A 2xx whose body was not the JSON the schema promises.
    ///
    /// Propagated rather than defaulted: a silent `{}` here would surface as a
    /// confusing empty answer in the agent with nothing to correlate server-side.
    ///
    /// The body is carried for the same reason [`Self::Api`] carries one — a serde
    /// position ("expected value at line 1 column 1") does not tell anyone that a
    /// reverse proxy returned an HTML page.
    #[error(
        "the admin API answered {status} with a body that is not valid JSON: {source}. Body: {body}"
    )]
    MalformedBody {
        status: u16,
        body: String,
        #[source]
        source: serde_json::Error,
    },

    /// An endpoint path that does not resolve against the base URL.
    ///
    /// Unreachable for the fixed literals and `u16` ports every call site passes,
    /// and it stays that way only because this is an error rather than a fallback:
    /// resolving to the base URL instead would send a real request to the wrong
    /// endpoint, and a root path that happens to answer `200` would then be
    /// returned to the agent as though it were the resource it asked for.
    #[error("could not resolve the admin path {path:?} against the base URL")]
    BadEndpoint { path: String },

    /// A `202 Accepted` that identified no op.
    ///
    /// A 202 *means* the write is parked for replay, so the one thing the agent must
    /// be able to do is poll it — and that needs an op id. Our own front stamps
    /// `Rift-Cluster-Op-Id` on every 202 and repeats it in the body, so this is not
    /// reachable against it; it becomes reachable the moment anything in between
    /// (a reverse proxy, a load balancer normalising unknown headers) drops the header
    /// and rewrites the body.
    ///
    /// This is an error rather than a fallback because the only available fallback is
    /// the wrong one. Letting a 202 fall through to the generic success path reports a
    /// queued write as **applied**: the agent stops, never polls, and believes a change
    /// landed that is still sitting in the replay queue. Loud beats wrong-but-quiet —
    /// and unlike an applied/parked mix-up, this failure is one an operator can act on.
    #[error(
        "the admin API answered 202 (parked for replay) but identified no op: neither the \
         Rift-Cluster-Op-Id header nor an `opId` in the body. The write may still be queued — \
         check the fleet's parked intents rather than re-sending it. Body: {body}"
    )]
    AcceptedWithoutOpId { body: String },

    /// The write **committed**, and then this node could not render the result.
    ///
    /// The front answers the failed post-commit re-read's status rather than dressing it in
    /// a success code, but still stamps the committed revision and op id. The canonical case
    /// is a config that applies while the engine refuses the op — an `imposter_create` whose
    /// port is already held commits the document and then 404s on the read-back (§7.4.6).
    ///
    /// This is its own failure precisely because the obvious reactions to a bare `404` are
    /// both wrong: the write must **not** be retried (it is already in the log, and a retry
    /// under a fresh tool-call id would apply it twice), and it must not be polled as a
    /// parked op (it is terminal). The committed revision is carried so nothing the front
    /// took the trouble to report is lost on the way to the agent.
    #[error(
        "the admin API answered {status} for a write that had already committed at revision \
         {revision}: the change is in the log but this node could not read it back. Do NOT \
         retry — re-read the record to see the current state. Response: {body}"
    )]
    CommittedButNotRendered {
        status: u16,
        revision: String,
        current_revision: Option<u64>,
        body: String,
    },
}

/// A JSON field that is `true` and cannot be anything else.
///
/// The issue specifies the wire shapes `{conflict: true, ...}` and `{parked: true, ...}`:
/// a flag an agent can branch on without knowing this enum's Rust shape. A `bool` field
/// would make `conflict: false` representable — a state with no meaning, since the variant
/// already decides it — so the type carries the constant instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, schemars::JsonSchema)]
pub struct True;

impl Serialize for True {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(true)
    }
}

/// The per-process namespace for [`idempotency_key`].
///
/// **Load-bearing, not decoration.** JSON-RPC request ids are per-connection and start at
/// 1, so every MCP session issues a tool call numbered 1. A key derived from the tool-call
/// id *alone* would therefore be identical across two unrelated agents' first writes, and
/// the front — which dedups on exactly that key — would fold the second into the first's
/// committed op and answer success. That is a silently lost write. Namespacing by a nonce
/// minted once per server process makes the collision unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionNonce(Uuid);

impl SessionNonce {
    /// A fresh nonce for one server process.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// A fixed nonce. Test-only: the derivation's properties (same id ⇒ same key, and
    /// the cross-session distinctness that is the whole point of the type) are only
    /// assertable against nonces the test chose, and production has no business
    /// pinning one.
    #[cfg(test)]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for SessionNonce {
    fn default() -> Self {
        Self::new()
    }
}

/// The `Idempotency-Key` for one tool call: deterministic in the tool-call id, distinct
/// across sessions.
///
/// Rendered as a UUID because the front adopts a UUID key *verbatim* as the op id
/// (`admin_front::base_op_id`) rather than deriving one — which is what makes the `op_id`
/// in a parked answer predictable, and what lets `op_status` be pointed at it.
///
/// The id is tagged by variant before hashing: `NumberOrString::Number(1)` and
/// `String("1")` are different request ids and must not share a key.
#[must_use]
pub fn idempotency_key(nonce: &SessionNonce, id: &rmcp::model::RequestId) -> Uuid {
    let tagged = match id {
        rmcp::model::NumberOrString::Number(n) => format!("n:{n}"),
        rmcp::model::NumberOrString::String(s) => format!("s:{s}"),
    };
    Uuid::new_v5(&nonce.0, tagged.as_bytes())
}

/// The bare revision integer inside a `Rift-Cluster-Revision` token.
///
/// The token is `default:<port>@<revision>` for an imposter and `default@<revision>` for a
/// route table; the front accepts a bare integer as `If-Match` for both, so the integer is
/// the one form that round-trips everywhere and is what the agent passes back.
///
/// A token this does not understand yields `None` rather than a guess: a fabricated
/// revision would be conditioned on by the agent's next write, and `0` in particular is a
/// real revision (a route table that was never written), so it is not available as a
/// stand-in for "unknown".
#[must_use]
pub fn revision_of(token: &str) -> Option<u64> {
    token.rsplit_once('@')?.1.parse().ok()
}

/// What a write actually did — the three answers an agent has to tell apart.
///
/// A single flat "success or error string" cannot express this: two of the three are
/// neither. A conflict means *rebase and retry*, a park means *the write is durable but
/// not yet applied, go poll it* — and an agent that cannot distinguish them from a failure
/// either gives up on work that will land, or retries work that already did.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WriteOutcome {
    /// Committed. `data` is the front's own post-commit render of the record.
    Applied {
        data: serde_json::Value,
        /// The revision to condition the *next* write on.
        #[serde(skip_serializing_if = "Option::is_none")]
        current_revision: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        op_id: Option<String>,
        /// `Rift-Cluster-Warnings`, verbatim: the commit is fleet truth but a named node
        /// or this node's engine did not realize it. Present-only-when-warned, so its
        /// absence is the domain value "fully applied" — see [`Markers::warnings`].
        #[serde(skip_serializing_if = "Option::is_none")]
        warnings: Option<String>,
    },
    /// Refused: the record moved since the revision this write was conditioned on.
    Conflict {
        conflict: True,
        /// The record's revision now, from a re-read — absent when the record does not
        /// exist, or when the re-read could not be made. Never scraped from the message.
        #[serde(skip_serializing_if = "Option::is_none")]
        current_revision: Option<u64>,
        /// The API's own refusal, verbatim.
        message: String,
    },
    /// Durable but not yet applied: parked for replay. Poll `op_status` with `op_id`.
    Parked { parked: True, op_id: String },
}

/// Turn a write's status + markers + body into the outcome an agent acts on.
///
/// Pure, so the three-way decision is assertable without a fleet — and it is a decision
/// worth pinning, because two of its arms are distinguished by a *header* rather than a
/// status:
///
/// * `202` is the `--cluster-admin-async` accept: parked by construction.
/// * A failure status carrying `Rift-Cluster-Op-Id` **and no revision** parked. The front
///   stamps the op id after `park_intent` succeeded (503/504/500 alike) and returns before
///   it has any revision to report, so that combination is the fact that the intent is
///   durable and the replay loop owns it.
/// * A failure status carrying **both** the op id and a revision did not park — it
///   committed, and then the post-commit re-read failed. See
///   [`ToolFailure::CommittedButNotRendered`]; calling it parked inverts a terminal write
///   into a pending one.
/// * A failure status *without* the op-id header never reached the park. Reporting that as
///   `Parked` would send the agent to poll an op that does not exist, and would present a
///   write that was never queued as one that is on its way.
pub fn classify_write(
    status: StatusCode,
    body: String,
    markers: Markers,
) -> Result<WriteOutcome, ToolFailure> {
    // A non-2xx carrying an op id is *not* enough to call it parked. The front downgrades a
    // committed write's status to that of a failed post-commit re-read (`admin_front.rs`: "the
    // render must not dress a non-2xx re-read in the success code"), and then stamps the op id
    // on it anyway — an `imposter_create` that commits and then fails to bind answers
    // `404 + Rift-Cluster-Op-Id`. Reporting that as parked is an inversion: the write is
    // already terminal, so the agent would poll an op that is done and never see the bind
    // failure that is the whole point of the answer.
    //
    // `Rift-Cluster-Revision` is the exact discriminator. Every park path returns before the
    // revision header is set — it has no committed revision to report, which is precisely what
    // being parked means — while the committed path always sets it.
    let parked_failure =
        !status.is_success() && markers.op_id.is_some() && markers.revision.is_none();
    if !status.is_success()
        && markers.op_id.is_some()
        && let Some(revision) = markers.revision.as_deref()
    {
        return Err(ToolFailure::CommittedButNotRendered {
            status: status.as_u16(),
            revision: revision.to_owned(),
            current_revision: revision_of(revision),
            body,
        });
    }
    if status == StatusCode::ACCEPTED || parked_failure {
        // Falling back to the body's `opId` keeps the 202 working if the header is ever
        // dropped in transit.
        let op_id = markers.op_id.clone().or_else(|| {
            serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["opId"].as_str().map(str::to_owned))
        });
        match op_id {
            Some(op_id) => {
                return Ok(WriteOutcome::Parked {
                    parked: True,
                    op_id,
                });
            }
            // The failure arm of the condition above is gated on the header, so it can
            // never land here — only a 202 can, and only when the header and the body have
            // *both* failed to identify the op. That is a malformed 202, not a success:
            // see [`ToolFailure::AcceptedWithoutOpId`] for why it must not fall through to
            // the generic 2xx path below, which would report a parked write as applied.
            None => return Err(ToolFailure::AcceptedWithoutOpId { body }),
        }
    }
    if status == StatusCode::CONFLICT {
        return Ok(WriteOutcome::Conflict {
            conflict: True,
            // Filled in by the caller's re-read. Deliberately not parsed out of `body`:
            // the number lives only inside the state machine's prose ("stored revision N"),
            // which is free to be reworded and would then yield a wrong revision silently.
            current_revision: None,
            message: body,
        });
    }
    let (data, markers) = classify(status, body, markers)?;
    Ok(WriteOutcome::Applied {
        current_revision: markers.revision.as_deref().and_then(revision_of),
        op_id: markers.op_id,
        warnings: markers.warnings,
        data,
    })
}

/// An authenticated client for one admin front.
#[derive(Debug, Clone)]
pub struct AdminClient {
    base: Url,
    key: ApiKey,
    http: reqwest::Client,
}

impl AdminClient {
    pub fn new(args: &McpArgs) -> Result<Self, StartupError> {
        check_url(&args.url)?;
        let key = ApiKey::load(&args.api_key_file)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(args.timeout_secs))
            .build()
            .map_err(StartupError::Client)?;
        Ok(Self {
            base: normalize_base(args.url.clone()),
            key,
            http,
        })
    }

    /// The absolute URL for an admin path.
    ///
    /// `path` is relative and must not start with `/`: a leading slash makes
    /// `Url::join` discard any path prefix on the base, so
    /// `https://host/rift` + `/imposters` would silently become
    /// `https://host/imposters` and hit nothing.
    pub fn endpoint(&self, path: &str) -> Result<Url, ToolFailure> {
        self.base.join(path).map_err(|_| ToolFailure::BadEndpoint {
            path: path.to_owned(),
        })
    }

    /// `GET` an admin path, returning its JSON body and the front's markers.
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let url = self.endpoint(path)?;
        let request = self
            .http
            .get(url.clone())
            .header(reqwest::header::AUTHORIZATION, self.key.header_value())
            .query(query);
        self.send(request, &url).await
    }

    /// `POST` a JSON body to an admin path.
    pub async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let url = self.endpoint(path)?;
        let request = self
            .http
            .post(url.clone())
            .header(reqwest::header::AUTHORIZATION, self.key.header_value())
            .json(body);
        self.send(request, &url).await
    }

    /// A mutation, carrying the two headers that make the cluster's concurrency
    /// semantics reachable from a tool (issue #293).
    ///
    /// `key` is always sent: every write is idempotent-by-key, so a retry of the same tool
    /// call dedups instead of double-applying. `expected_revision` is sent only when the
    /// agent supplied one — absent means last-writer-wins, which is the front's own
    /// default, and synthesising an `If-Match` here would silently turn every write into a
    /// conditional one and refuse writes the agent expected to land.
    ///
    /// `record_path` is the **readable** path of the record the write targets, which is not
    /// the write's own path: a stub edit posts to `imposters/{port}/stubs/by-id/{id}` but
    /// the revision belongs to `imposters/{port}`, and there is no GET on the stub path at
    /// all. It is only used to re-read a revision after a conflict.
    pub async fn write(
        &self,
        method: Method,
        path: &str,
        record_path: &str,
        body: Option<&serde_json::Value>,
        key: Uuid,
        expected_revision: Option<u64>,
    ) -> Result<WriteOutcome, ToolFailure> {
        let url = self.endpoint(path)?;
        let mut request = self
            .http
            .request(method, url.clone())
            .header(reqwest::header::AUTHORIZATION, self.key.header_value())
            .header("idempotency-key", key.to_string());
        if let Some(revision) = expected_revision {
            request = request.header(reqwest::header::IF_MATCH, revision.to_string());
        }
        if let Some(body) = body {
            request = request.json(body);
        }

        let response = request
            .send()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;
        let status = response.status();
        let markers = Markers::read(response.headers());
        let body = response
            .text()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;

        let outcome = classify_write(status, body, markers)?;
        // The conflict's `current_revision` is the one thing `classify_write` cannot know:
        // the 409 carries no revision header and states the stored revision only in prose.
        // Re-reading is what turns "you are stale" into a number the agent can retry with.
        if let WriteOutcome::Conflict {
            conflict,
            current_revision: None,
            message,
        } = outcome
        {
            return Ok(WriteOutcome::Conflict {
                conflict,
                current_revision: self.revision_at(record_path).await,
                message,
            });
        }
        Ok(outcome)
    }

    /// The record's revision now, for a conflict's `current_revision`.
    ///
    /// `None` covers three real cases and invents nothing: the record does not exist (the
    /// absent-target conflict — there is no revision to report), the token did not parse,
    /// or the re-read itself failed. The conflict is reported either way; this only decides
    /// whether the agent is handed a number to retry with or has to read for itself. The
    /// failure is logged rather than swallowed silently, because a re-read that keeps
    /// failing is a real symptom with nothing else to correlate it.
    ///
    /// Reports the revision as of *now*, which may already be past the one that refused the
    /// write. That is the right value to condition the retry on, and if it moves again the
    /// retry earns another conflict — which is the loop working, not a bug.
    async fn revision_at(&self, path: &str) -> Option<u64> {
        match self.get_json(path, &[]).await {
            Ok((_, markers)) => markers.revision.as_deref().and_then(revision_of),
            Err(failure) => {
                // `warn`, not `debug`: the MCP subcommand pins its subscriber to INFO by
                // default (see `run_mcp`), so a `debug` line here would be filtered out in
                // every deployment that has not set `RUST_LOG` — leaving exactly the silent
                // symptom this log exists to prevent. The operator would see conflicts
                // arriving without a revision to retry against and nothing to correlate.
                tracing::warn!(
                    path,
                    error = %failure,
                    "could not re-read the record for its current revision after a conflict"
                );
                None
            }
        }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        url: &Url,
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let response = request
            .send()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;

        let status = response.status();
        let markers = Markers::read(response.headers());

        let body = response
            .text()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;

        classify(status, body, markers)
    }
}

/// The provenance headers the admin front stamps on a read.
///
/// A struct over the headers, and separate from [`classify`] on purpose: the first
/// version of this gate tested only that `classify` *passes a marker through*, which
/// left the code that actually reads the headers uncovered — deleting the read
/// entirely kept the suite green. Extracting it is what makes the read assertable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Markers {
    pub partial: Option<String>,
    pub next_index: Option<String>,
    pub truncated: Option<String>,
    /// `Rift-Cluster-Revision` — `default:<port>@<rev>` or `default@<rev>`.
    pub revision: Option<String>,
    /// `Rift-Cluster-Op-Id`. On a *failure* status this is the tell that the write
    /// was parked for replay rather than lost — see [`classify_write`].
    pub op_id: Option<String>,
    /// `Rift-Cluster-Warnings` — `unapplied=<node,…>` and/or `local-engine=<failure>`.
    ///
    /// Stamped on a **successful** write that the fleet committed but that some node did
    /// not realize: a write barrier that timed out on named peers, or an engine here that
    /// refused the op. §7.4.6 calls this "success with a named warning, never a silent
    /// divergence the client cannot see" — so dropping it is exactly that silent
    /// divergence. An agent that creates an imposter and is told, unqualified, that it
    /// worked will then get connection-refused on the port with nothing to explain it.
    pub warnings: Option<String>,
}

impl Markers {
    fn read(headers: &reqwest::header::HeaderMap) -> Self {
        Self {
            partial: header_str(headers, HEADER_PARTIAL),
            next_index: header_str(headers, HEADER_NEXT_INDEX),
            truncated: header_str(headers, HEADER_TRUNCATED),
            revision: header_str(headers, HEADER_REVISION),
            op_id: header_str(headers, HEADER_OP_ID),
            warnings: header_str(headers, HEADER_WARNINGS),
        }
    }
}

/// One header as a string, or `None`.
///
/// A value whose bytes are not UTF-8 yields `None`. That is a **domain-optional**
/// absence, not a swallowed error: all three of these headers are additive markers
/// whose absence already means something definite, and none of them gates the
/// answer's data. An unreadable one is no more informative than a missing one.
fn header_str(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Turn a status + body into either a parsed answer or a typed failure.
///
/// Split out from [`AdminClient::send`] as a pure function so the status handling
/// is testable without a live server — the branch that decides whether an agent
/// sees the API's real error is not one to leave covered only by integration tests.
fn classify(
    status: StatusCode,
    body: String,
    markers: Markers,
) -> Result<(serde_json::Value, Markers), ToolFailure> {
    if status == StatusCode::UNAUTHORIZED {
        return Err(ToolFailure::Unauthorized { body });
    }
    if !status.is_success() {
        return Err(ToolFailure::Api {
            status: status.as_u16(),
            body,
        });
    }
    // A 2xx with an empty body is a legitimate shape (204, and some deletes), and
    // `null` is its honest JSON rendering — not an error, and not a fabricated `{}`.
    if body.trim().is_empty() {
        return Ok((serde_json::Value::Null, markers));
    }
    // A `match` rather than `map_err`: the failure arm needs to *move* `body` into
    // the error, which it cannot do while `from_str` still borrows it.
    match serde_json::from_str(&body) {
        Ok(value) => Ok((value, markers)),
        Err(source) => Err(ToolFailure::MalformedBody {
            status: status.as_u16(),
            body,
            source,
        }),
    }
}

/// Refuse a base URL this process must not carry a credential to.
///
/// Two checks, both about the key rather than about correctness:
///
/// * **Userinfo is refused.** `Url::to_string()` renders `user:password@host`, and
///   [`ToolFailure::Transport`] interpolates the URL — so a password in `--url`
///   would be printed straight into the agent's transcript. It is the one credential
///   path the [`ApiKey`] redaction cannot cover, because it never becomes an `ApiKey`.
/// * **Plaintext to a non-loopback host warns.** The whole point of `--api-key-file`
///   is that the key is hard to leak; sending it over cleartext HTTP to a remote host
///   undoes that. Loopback is exempt — it is what the tests and a local fleet use.
fn check_url(url: &Url) -> Result<(), StartupError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(StartupError::UrlHasUserinfo);
    }
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    );
    if url.scheme() != "https" && !loopback {
        tracing::warn!(
            host = url.host_str().unwrap_or("<none>"),
            "sending the API key over plaintext HTTP to a non-loopback host; prefer https"
        );
    }
    Ok(())
}

/// Make the base URL a directory URL, so `join` extends its path instead of
/// replacing the last segment.
///
/// `https://host:2525` and `https://host:2525/` must address the same fleet; without
/// this they differ, and the second form is what people paste from a browser.
fn normalize_base(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(raw: &str) -> Url {
        normalize_base(Url::parse(raw).expect("test URL must parse"))
    }

    /// E6 — the two spellings a human produces must address the same endpoint.
    #[test]
    fn base_url_with_and_without_trailing_slash_agree() {
        assert_eq!(
            base("https://fleet.example:2525")
                .join("imposters")
                .unwrap(),
            base("https://fleet.example:2525/")
                .join("imposters")
                .unwrap()
        );
    }

    /// E6 — a base carrying a path prefix must keep it. This is the case the
    /// normalization exists for; without it, `join` replaces `rift` instead of
    /// extending it and every request silently misses the mount point.
    #[test]
    fn base_url_path_prefix_is_preserved() {
        let joined = base("https://fleet.example/rift")
            .join("imposters")
            .unwrap();
        assert_eq!(joined.as_str(), "https://fleet.example/rift/imposters");
    }

    /// A URL carrying userinfo is refused rather than normalized.
    ///
    /// `Url::to_string()` renders `user:password@host`, and the transport error
    /// message interpolates the URL — so a password in `--url` would be printed
    /// straight into the agent's transcript. That is the one credential path the
    /// `ApiKey` redaction does not cover, so it is closed at the door.
    #[test]
    fn a_url_with_userinfo_is_refused() {
        let err = check_url(&Url::parse("https://agent:s3cret@fleet.example:2525").unwrap())
            .expect_err("userinfo must be refused");
        assert!(
            matches!(err, StartupError::UrlHasUserinfo),
            "expected UrlHasUserinfo, got {err:?}"
        );
        let rendered = err.to_string();
        assert!(
            !rendered.contains("s3cret"),
            "the refusal must not echo the password: {rendered}"
        );
    }

    #[test]
    fn a_url_without_userinfo_is_accepted() {
        assert!(check_url(&Url::parse("https://fleet.example:2525").unwrap()).is_ok());
        assert!(check_url(&Url::parse("http://127.0.0.1:2525").unwrap()).is_ok());
    }

    /// E12 — a 401 is its own variant so the message can explain it...
    #[test]
    fn unauthorized_status_is_its_own_failure() {
        let err = classify(
            StatusCode::UNAUTHORIZED,
            r#"{"error":"unauthorized"}"#.to_owned(),
            Markers::default(),
        )
        .expect_err("401 must fail");
        assert!(
            matches!(err, ToolFailure::Unauthorized { .. }),
            "expected Unauthorized, got {err:?}"
        );
    }

    /// E12 — ...and that message must not be a place a key could appear. The only
    /// interpolated value is the API's own body.
    #[test]
    fn unauthorized_message_carries_only_the_api_body() {
        let err = ToolFailure::Unauthorized {
            body: r#"{"error":"unauthorized"}"#.to_owned(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains(r#"{"error":"unauthorized"}"#));
        assert!(
            rendered.contains("Bearer "),
            "the 401 message should name the Bearer-prefix trap: {rendered}"
        );
    }

    /// E10 — the API's error body reaches the agent unaltered. Summarising it here
    /// would strip exactly the detail an agent needs to correct its next call.
    #[test]
    fn api_error_body_is_relayed_verbatim() {
        let body = r#"{"code":"invalid_predicate","detail":"unknown operator 'startsWith'"}"#;
        let err = classify(StatusCode::BAD_REQUEST, body.to_owned(), Markers::default())
            .expect_err("400 must fail");
        match err {
            ToolFailure::Api { status, body: got } => {
                assert_eq!(status, 400);
                assert_eq!(got, body, "the body must be relayed byte-for-byte");
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    /// E11 — a non-2xx with an empty body still reports its status rather than
    /// becoming a parse error or a panic.
    #[test]
    fn empty_error_body_still_relays_status() {
        let err = classify(StatusCode::BAD_GATEWAY, String::new(), Markers::default())
            .expect_err("502 must fail");
        match err {
            ToolFailure::Api { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body, "");
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    /// E11 — an HTML error page from a proxy in front of the fleet is a realistic
    /// non-JSON body, and must not be mistaken for a malformed *success*.
    #[test]
    fn non_json_error_body_is_an_api_failure_not_a_parse_failure() {
        let err = classify(
            StatusCode::SERVICE_UNAVAILABLE,
            "<html><body>503 Service Unavailable</body></html>".to_owned(),
            Markers::default(),
        )
        .expect_err("503 must fail");
        assert!(
            matches!(err, ToolFailure::Api { status: 503, .. }),
            "expected Api 503, got {err:?}"
        );
    }

    /// A 2xx whose body is not JSON is a distinct, named failure — never a
    /// silently-defaulted empty object — and it carries the body, because a serde
    /// column number does not tell anyone a proxy returned an HTML page.
    #[test]
    fn malformed_success_body_is_reported_with_its_body() {
        let err = classify(
            StatusCode::OK,
            "<html>not json at all</html>".to_owned(),
            Markers::default(),
        )
        .expect_err("a non-JSON 200 must fail");
        // The rendered message must carry the body — a serde column number does not
        // tell anyone a proxy returned an HTML page.
        assert!(
            err.to_string().contains("<html>not json at all</html>"),
            "the rendered message must carry the body: {err}"
        );
        match err {
            ToolFailure::MalformedBody { status, body, .. } => {
                assert_eq!(status, 200);
                assert_eq!(body, "<html>not json at all</html>");
            }
            other => panic!("expected MalformedBody, got {other:?}"),
        }
    }

    /// A 2xx with no body is legitimate and renders as JSON `null`, not as an error.
    #[test]
    fn empty_success_body_is_json_null() {
        let (value, _) = classify(StatusCode::NO_CONTENT, String::new(), Markers::default())
            .expect("an empty 2xx is not a failure");
        assert_eq!(value, serde_json::Value::Null);
    }

    /// E9 — the headers are actually *read* off the response.
    ///
    /// This is the assertion the first draft of this gate was missing: it tested
    /// only that `classify` forwards a marker it is handed, so deleting the read
    /// itself left the suite green.
    #[test]
    fn markers_are_read_from_the_response_headers() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HEADER_PARTIAL,
            reqwest::header::HeaderValue::from_static("node-3 unreachable"),
        );
        headers.insert(
            HEADER_NEXT_INDEX,
            reqwest::header::HeaderValue::from_static("cursor-42"),
        );
        headers.insert(
            HEADER_TRUNCATED,
            reqwest::header::HeaderValue::from_static("true"),
        );

        let markers = Markers::read(&headers);
        assert_eq!(markers.partial.as_deref(), Some("node-3 unreachable"));
        assert_eq!(markers.next_index.as_deref(), Some("cursor-42"));
        assert_eq!(markers.truncated.as_deref(), Some("true"));
    }

    /// E9 — no headers means a complete, unpaged, untruncated answer, and every
    /// marker is `None` rather than an empty string that would read as "partial".
    #[test]
    fn markers_are_none_when_no_header_is_present() {
        assert_eq!(
            Markers::read(&reqwest::header::HeaderMap::new()),
            Markers::default()
        );
    }

    /// The three markers are independent: a paged answer is not a partial one.
    #[test]
    fn next_index_alone_does_not_imply_partial() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HEADER_NEXT_INDEX,
            reqwest::header::HeaderValue::from_static("cursor-42"),
        );
        let markers = Markers::read(&headers);
        assert_eq!(markers.next_index.as_deref(), Some("cursor-42"));
        assert_eq!(markers.partial, None);
        assert_eq!(markers.truncated, None);
    }

    /// E9 — markers survive `classify` on the success path.
    #[test]
    fn markers_reach_the_answer() {
        let markers = Markers {
            partial: Some("node-3 unreachable".to_owned()),
            next_index: Some("cursor-7".to_owned()),
            truncated: Some("true".to_owned()),
            ..Markers::default()
        };
        let (_, got) =
            classify(StatusCode::OK, "[]".to_owned(), markers.clone()).expect("200 must succeed");
        assert_eq!(got, markers);
    }

    /// The scope enum's wire spelling is what an agent reads; pin it literally.
    #[test]
    fn read_scope_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&ReadScope::Fleet).unwrap(),
            r#""fleet""#
        );
        assert_eq!(
            serde_json::to_string(&ReadScope::Node).unwrap(),
            r#""node""#
        );
    }

    /// An answer with nothing to qualify carries none of the optional keys, rather
    /// than nulls an agent would have to learn to ignore.
    #[test]
    fn plain_answer_omits_every_optional_field() {
        let json = serde_json::to_string(&Answer::plain(serde_json::json!({"ok": true})))
            .expect("serialize");
        assert_eq!(json, r#"{"data":{"ok":true}}"#);
    }

    /// And an answer that has markers renders all of them, under the names the
    /// tool descriptions tell an agent to look for.
    #[test]
    fn answer_renders_every_marker_it_carries() {
        let answer = Answer::new(
            serde_json::json!([]),
            Some(ReadScope::Fleet),
            Markers {
                partial: Some("node-3 unreachable".to_owned()),
                next_index: Some("cursor-7".to_owned()),
                truncated: Some("true".to_owned()),
                ..Markers::default()
            },
        );
        let json = serde_json::to_value(&answer).expect("serialize");
        assert_eq!(json["scope"], "fleet");
        assert_eq!(json["partial"], "node-3 unreachable");
        assert_eq!(json["next_index"], "cursor-7");
        assert_eq!(json["truncated"], "true");
    }

    // ---- #293 gate: write semantics -------------------------------------------------

    fn nonce(raw: &str) -> SessionNonce {
        SessionNonce::from_uuid(uuid::Uuid::parse_str(raw).expect("test nonce must parse"))
    }

    const NONCE_A: &str = "11111111-1111-4111-8111-111111111111";
    const NONCE_B: &str = "22222222-2222-4222-8222-222222222222";

    fn num(id: i64) -> rmcp::model::RequestId {
        rmcp::model::NumberOrString::Number(id)
    }

    fn text(id: &str) -> rmcp::model::RequestId {
        rmcp::model::NumberOrString::String(id.into())
    }

    /// E1 — the criterion itself: the key is a deterministic function of the tool-call
    /// id, so a retry of the *same* call carries the *same* key and dedups.
    #[test]
    fn the_same_tool_call_id_derives_the_same_key() {
        let session = nonce(NONCE_A);
        assert_eq!(
            idempotency_key(&session, &num(7)),
            idempotency_key(&session, &num(7))
        );
    }

    /// E1 — and two different calls must not dedup into each other.
    #[test]
    fn different_tool_call_ids_derive_different_keys() {
        let session = nonce(NONCE_A);
        assert_ne!(
            idempotency_key(&session, &num(7)),
            idempotency_key(&session, &num(8))
        );
    }

    /// E2 — the collision this whole newtype exists to prevent.
    ///
    /// JSON-RPC request ids are per-connection and start at 1, so *every* MCP session
    /// issues a tool call numbered 1. Keyed on the id alone, two unrelated agents'
    /// first writes would carry the same `Idempotency-Key`, and the front would dedup
    /// the second into the first's committed op — a silently lost write, reported to
    /// the second agent as success.
    #[test]
    fn the_same_tool_call_id_in_two_sessions_derives_different_keys() {
        assert_ne!(
            idempotency_key(&nonce(NONCE_A), &num(1)),
            idempotency_key(&nonce(NONCE_B), &num(1)),
            "a per-session nonce is what stops two agents' call #1 from deduping together"
        );
    }

    /// E3 — the id is a `NumberOrString`; the numeric `1` and the string `"1"` are
    /// different ids and must not share a key.
    #[test]
    fn numeric_and_string_tool_call_ids_do_not_collide() {
        let session = nonce(NONCE_A);
        assert_ne!(
            idempotency_key(&session, &num(1)),
            idempotency_key(&session, &text("1"))
        );
    }

    /// The key is rendered as a UUID so the front adopts it verbatim as the op id
    /// (`base_op_id`), which is what makes the parked `op_id` predictable.
    #[test]
    fn the_key_renders_as_a_uuid() {
        let rendered = idempotency_key(&nonce(NONCE_A), &num(1)).to_string();
        assert!(
            uuid::Uuid::parse_str(&rendered).is_ok(),
            "the key must be a UUID so the front uses it verbatim: {rendered}"
        );
    }

    /// E10 — the revision token's two shapes, ported and portless, both yield the
    /// bare integer the agent passes back as `expected_revision`.
    #[test]
    fn revision_is_parsed_from_both_token_shapes() {
        assert_eq!(revision_of("default:4545@7"), Some(7));
        assert_eq!(revision_of("default@3"), Some(3));
    }

    /// E10 — a token this code does not understand yields `None` rather than a
    /// fabricated number an agent would then condition a write on.
    #[test]
    fn an_unparseable_revision_token_is_none() {
        assert_eq!(revision_of("not-a-token"), None);
        assert_eq!(revision_of("default:4545@"), None);
        assert_eq!(revision_of(""), None);
    }

    fn parked_markers(op_id: &str) -> Markers {
        Markers {
            op_id: Some(op_id.to_owned()),
            ..Markers::default()
        }
    }

    /// E4 — `--cluster-admin-async` answers 202 with the op id to poll.
    #[test]
    fn an_accepted_write_is_parked() {
        let outcome = classify_write(
            StatusCode::ACCEPTED,
            r#"{"opId":"0189dcf0-0454-4e0b-a10c-8a8f8dccce1f","opIds":[]}"#.to_owned(),
            parked_markers("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
        )
        .expect("a 202 is an outcome, not a failure");
        match outcome {
            WriteOutcome::Parked { op_id, .. } => {
                assert_eq!(op_id, "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f");
            }
            other => panic!("202 must be Parked, got {other:?}"),
        }
    }

    /// E4 — the header is the primary source, but the body's `opId` stands in when the
    /// header is missing.
    ///
    /// Without this the fallback is dead code: every other park test supplies the header,
    /// so `or_else` never runs and an implementation that ignored the body entirely would
    /// pass the whole suite. The scenario is a proxy that drops an unrecognised header
    /// while relaying the body untouched.
    #[test]
    fn a_park_identified_only_by_its_body_is_still_parked() {
        let outcome = classify_write(
            StatusCode::ACCEPTED,
            r#"{"opId":"0189dcf0-0454-4e0b-a10c-8a8f8dccce1f","opIds":[]}"#.to_owned(),
            Markers::default(),
        )
        .expect("a 202 whose op id is in the body is still an outcome, not a failure");
        match outcome {
            WriteOutcome::Parked { op_id, .. } => {
                assert_eq!(op_id, "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f");
            }
            other => panic!("a body-identified 202 must be Parked, got {other:?}"),
        }
    }

    /// E4 — the header wins when the two disagree.
    ///
    /// It is the fresher of the two: the front stamps it after `park_intent` returned,
    /// whereas the body is rendered from whatever the submit path had in hand. Pinning
    /// the precedence keeps a future refactor from silently reversing it.
    #[test]
    fn the_op_id_header_wins_over_a_disagreeing_body() {
        let outcome = classify_write(
            StatusCode::ACCEPTED,
            r#"{"opId":"body-0000-0000-0000-000000000000","opIds":[]}"#.to_owned(),
            parked_markers("header-0000-0000-0000-000000000000"),
        )
        .expect("a 202 is an outcome, not a failure");
        match outcome {
            WriteOutcome::Parked { op_id, .. } => {
                assert_eq!(op_id, "header-0000-0000-0000-000000000000");
            }
            other => panic!("202 must be Parked, got {other:?}"),
        }
    }

    /// A 202 that identified no op at all is a failure, **not** an applied write.
    ///
    /// This is the one that matters most in this group. A 202 means the write is parked
    /// for replay; if it falls through to the generic 2xx path it is reported as
    /// `Applied`, and the agent stops — never polling `op_status`, believing a queued
    /// change has landed. An error is the only honest answer, because the agent must not
    /// blindly re-send either (the write may well be queued).
    #[test]
    fn an_accepted_write_identifying_no_op_is_a_failure_not_an_applied_write() {
        let err = classify_write(
            StatusCode::ACCEPTED,
            r#"{"status":"accepted"}"#.to_owned(),
            Markers::default(),
        )
        .expect_err("a 202 with no op id anywhere cannot be reported as applied");
        assert!(
            matches!(err, ToolFailure::AcceptedWithoutOpId { .. }),
            "expected AcceptedWithoutOpId, got {err:?}"
        );
    }

    /// The same, for a 202 whose body is not even JSON — the `.ok()` in the body
    /// fallback is a domain-optional parse, and must not become a silent success.
    #[test]
    fn an_accepted_write_with_an_unparseable_body_is_a_failure() {
        let err = classify_write(
            StatusCode::ACCEPTED,
            "<html>502 Bad Gateway</html>".to_owned(),
            Markers::default(),
        )
        .expect_err("a 202 with no op id anywhere cannot be reported as applied");
        assert!(
            matches!(err, ToolFailure::AcceptedWithoutOpId { .. }),
            "expected AcceptedWithoutOpId, got {err:?}"
        );
    }

    /// A committed write whose post-commit re-read failed is **not** parked.
    ///
    /// This is the arm the op-id-alone rule got wrong. The front downgrades the status to
    /// the failed re-read's and still stamps the op id, so an `imposter_create` that
    /// commits and then cannot bind answers `404 + op-id + revision`. Calling that parked
    /// tells the agent a terminal write is still on its way, and buries the bind failure
    /// that is the only useful thing in the response.
    ///
    /// The revision header is the discriminator: a park has no committed revision to name.
    #[test]
    fn a_committed_write_whose_render_failed_is_not_parked() {
        let err = classify_write(
            StatusCode::NOT_FOUND,
            r#"{"errors":[{"message":"no imposter on port 4545"}]}"#.to_owned(),
            Markers {
                op_id: Some("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f".to_owned()),
                revision: Some("default:4545@8".to_owned()),
                ..Markers::default()
            },
        )
        .expect_err("a committed-but-unrendered write is not an outcome the agent can act on");
        match err {
            ToolFailure::CommittedButNotRendered {
                status,
                current_revision,
                ref body,
                ..
            } => {
                assert_eq!(status, 404);
                assert_eq!(
                    current_revision,
                    Some(8),
                    "the committed revision must survive; it is what the front took the \
                     trouble to report"
                );
                assert!(
                    body.contains("no imposter on port 4545"),
                    "the front's own explanation must reach the agent; got {body}"
                );
            }
            other => panic!("expected CommittedButNotRendered, got {other:?}"),
        }
    }

    /// The complement, and the one that keeps the discriminator honest: the *same*
    /// status and op id, with no revision, is still a park.
    ///
    /// Without this pair an implementation could satisfy either test alone by keying on
    /// the status — and the two cases differ only by a header.
    #[test]
    fn the_same_failure_without_a_revision_is_still_parked() {
        let outcome = classify_write(
            StatusCode::NOT_FOUND,
            r#"{"errors":[{"message":"parked for replay"}]}"#.to_owned(),
            parked_markers("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
        )
        .expect("a park is an outcome, not a failure");
        assert!(
            matches!(outcome, WriteOutcome::Parked { .. }),
            "an op id with no revision is a park: {outcome:?}"
        );
    }

    /// §7.4.6 — a successful write can still carry a warning, and it must reach the agent.
    ///
    /// The commit is fleet truth, but a named peer never applied it, or this node's engine
    /// refused the op. Dropping the header renders that as an unqualified success, and the
    /// agent's next act is to use a port that is not listening.
    #[test]
    fn an_applied_write_carries_its_warnings() {
        let outcome = classify_write(
            StatusCode::CREATED,
            r#"{"port":4545}"#.to_owned(),
            Markers {
                revision: Some("default:4545@8".to_owned()),
                warnings: Some("local-engine=address in use".to_owned()),
                ..Markers::default()
            },
        )
        .expect("a warned write still applied");
        match outcome {
            WriteOutcome::Applied { warnings, .. } => {
                assert_eq!(warnings.as_deref(), Some("local-engine=address in use"));
            }
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    /// E5 — a 503 that parked stamps the op id, and that is what distinguishes it
    /// from a 503 that did not.
    #[test]
    fn an_unavailable_write_that_parked_is_parked() {
        let outcome = classify_write(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"errors":[{"message":"no quorum / leader unreachable (parked for replay)"}]}"#
                .to_owned(),
            parked_markers("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
        )
        .expect("a parked 503 is an outcome, not a failure");
        assert!(
            matches!(outcome, WriteOutcome::Parked { .. }),
            "a 503 carrying an op id parked the write: {outcome:?}"
        );
    }

    /// E5 — the other half, and the one that must not become a false "parked".
    ///
    /// A 503 with no op-id header never reached the park: nothing is queued, nothing
    /// will replay, and telling an agent to poll `op_status` would send it after an
    /// op that does not exist.
    #[test]
    fn an_unavailable_write_that_did_not_park_is_a_failure() {
        let err = classify_write(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"errors":[{"message":"cluster node is shutting down"}]}"#.to_owned(),
            Markers::default(),
        )
        .expect_err("a 503 with no op id is a real failure");
        assert!(
            matches!(err, ToolFailure::Api { status: 503, .. }),
            "expected an Api failure, got {err:?}"
        );
    }

    /// E6 — a submit that timed out (504) or failed internally (500) parked too, and
    /// both stamp the op id for the same reason the 503 does.
    #[test]
    fn a_timed_out_or_failed_submit_that_parked_is_parked() {
        for status in [
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let outcome = classify_write(
                status,
                r#"{"errors":[{"message":"parked for replay"}]}"#.to_owned(),
                parked_markers("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
            )
            .unwrap_or_else(|e| panic!("{status} carrying an op id must be Parked, got {e:?}"));
            assert!(
                matches!(outcome, WriteOutcome::Parked { .. }),
                "{status} carrying an op id must be Parked, got {outcome:?}"
            );
        }
    }

    /// E7 — a 409 is a conflict outcome, never a bare failure string, and it carries
    /// the API's own message so the agent can see what refused it.
    #[test]
    fn a_revision_conflict_is_a_conflict_outcome() {
        let body = r#"{"errors":[{"code":"resource_conflict","message":"revision conflict: expected revision 3, stored revision 5 on port 4545"}]}"#;
        let outcome = classify_write(StatusCode::CONFLICT, body.to_owned(), Markers::default())
            .expect("a 409 is an outcome, not a failure");
        match outcome {
            WriteOutcome::Conflict {
                current_revision,
                message,
                ..
            } => {
                // `classify_write` is pure: the revision arrives from the re-read the
                // tool does afterwards, never from scraping this prose.
                assert_eq!(current_revision, None);
                assert!(
                    message.contains("revision conflict"),
                    "the API's own message must reach the agent: {message}"
                );
            }
            other => panic!("409 must be Conflict, got {other:?}"),
        }
    }

    /// A conflict serializes with the literal flag the issue specifies, so an agent
    /// can branch on one key without knowing this enum's Rust shape.
    #[test]
    fn a_conflict_serializes_with_its_flag() {
        let json = serde_json::to_value(WriteOutcome::Conflict {
            conflict: True,
            current_revision: Some(5),
            message: "revision conflict".to_owned(),
        })
        .expect("serialize");
        assert_eq!(json["conflict"], true);
        assert_eq!(json["current_revision"], 5);
    }

    /// And so does a parked answer.
    #[test]
    fn a_parked_answer_serializes_with_its_flag() {
        let json = serde_json::to_value(WriteOutcome::Parked {
            parked: True,
            op_id: "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f".to_owned(),
        })
        .expect("serialize");
        assert_eq!(json["parked"], true);
        assert_eq!(json["op_id"], "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f");
    }

    /// An applied write hands back the new revision, which is the value the agent
    /// conditions its *next* write on.
    #[test]
    fn an_applied_write_carries_its_new_revision() {
        let markers = Markers {
            revision: Some("default:4545@8".to_owned()),
            op_id: Some("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f".to_owned()),
            ..Markers::default()
        };
        let outcome = classify_write(StatusCode::OK, r#"{"port":4545}"#.to_owned(), markers)
            .expect("200 must succeed");
        match outcome {
            WriteOutcome::Applied {
                current_revision,
                op_id,
                ..
            } => {
                assert_eq!(current_revision, Some(8));
                assert_eq!(
                    op_id.as_deref(),
                    Some("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f")
                );
            }
            other => panic!("200 must be Applied, got {other:?}"),
        }
    }

    /// E15 — a 2xx write whose body is not JSON is the same named failure a read's is.
    /// Fabricating an `Applied {}` here would report a write as done on the strength of
    /// a proxy's HTML error page.
    #[test]
    fn a_malformed_write_body_is_not_a_silent_applied() {
        let err = classify_write(
            StatusCode::OK,
            "<html>not json</html>".to_owned(),
            Markers::default(),
        )
        .expect_err("a non-JSON 200 must fail");
        assert!(
            matches!(err, ToolFailure::MalformedBody { status: 200, .. }),
            "expected MalformedBody, got {err:?}"
        );
    }

    /// A 401 keeps its own variant on the write path too — the message that explains
    /// the `Bearer ` trap is just as needed here as on a read.
    #[test]
    fn an_unauthorized_write_keeps_its_variant() {
        let err = classify_write(
            StatusCode::UNAUTHORIZED,
            r#"{"error":"unauthorized"}"#.to_owned(),
            Markers::default(),
        )
        .expect_err("401 must fail");
        assert!(matches!(err, ToolFailure::Unauthorized { .. }), "{err:?}");
    }

    /// E11 / AC5 — the revision header is actually read off the response. Without this
    /// the whole revision loop is undocumentable: the agent has no way to obtain a value
    /// to pass back as `expected_revision`.
    #[test]
    fn the_revision_and_op_id_headers_are_read() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HEADER_REVISION,
            reqwest::header::HeaderValue::from_static("default:4545@7"),
        );
        headers.insert(
            HEADER_OP_ID,
            reqwest::header::HeaderValue::from_static("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f"),
        );
        let markers = Markers::read(&headers);
        assert_eq!(markers.revision.as_deref(), Some("default:4545@7"));
        assert_eq!(
            markers.op_id.as_deref(),
            Some("0189dcf0-0454-4e0b-a10c-8a8f8dccce1f")
        );
    }

    /// AC5 — a read's answer surfaces the parsed revision, so `imposter_get` hands the
    /// agent something it can pass straight back.
    #[test]
    fn a_read_answer_carries_its_current_revision() {
        let answer = Answer::new(
            serde_json::json!({"port": 4545}),
            None,
            Markers {
                revision: Some("default:4545@7".to_owned()),
                ..Markers::default()
            },
        );
        let json = serde_json::to_value(&answer).expect("serialize");
        assert_eq!(json["current_revision"], 7);
    }

    /// And an answer with no revision omits the key rather than sending a null the
    /// agent would have to learn to ignore.
    #[test]
    fn a_read_answer_without_a_revision_omits_the_key() {
        let json = serde_json::to_value(Answer::plain(serde_json::json!({}))).expect("serialize");
        assert_eq!(json.get("current_revision"), None);
    }
}
