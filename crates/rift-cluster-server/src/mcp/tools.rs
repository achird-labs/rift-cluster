//! The tool set: nine reads (#292) and eight writes (#293) over the clustered admin API.
//!
//! The MCP surface is the admin API **re-projected, not re-specified** (RFC-006 §8.2) —
//! every tool is one call to an endpoint that already exists and is already schema'd in
//! `docs/api/openapi-ee.yaml`. Nothing here re-implements server behaviour; where a
//! judgement could be made in two places (predicate matching, most obviously), it is
//! made on the server.
//!
//! The writes carry the cluster's concurrency semantics as **tool behaviour** rather than
//! as description prose an agent would ignore (issue #293): an `Idempotency-Key` derived
//! from the tool-call id, an optional `expected_revision` that becomes `If-Match`, and a
//! three-way answer — applied, conflicted, or parked — so an agent can tell "rebase and
//! retry" and "durable, go poll it" apart from a failure. All three live in
//! [`RiftMcp::write`], not in the nine tool bodies, so a tool cannot forget one.

use reqwest::Method;
use rmcp::handler::server::wrapper::Json;
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use super::client::{
    AdminClient, Answer, ReadScope, SessionNonce, ToolFailure, WriteOutcome, idempotency_key,
};

/// A tool failure becomes a structured MCP error rather than a dropped connection.
impl From<ToolFailure> for ErrorData {
    fn from(failure: ToolFailure) -> Self {
        ErrorData::internal_error(failure.to_string(), None)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PortParams {
    /// The imposter's port.
    pub port: u16,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RequestsParams {
    /// The imposter's port.
    pub port: u16,
    /// Opaque cursor from a previous call's `x-rift-next-index`; returns only
    /// requests recorded after it.
    #[serde(default)]
    pub since: Option<String>,
    /// A predicate set to filter by.
    ///
    /// Supplying this narrows the answer to the **answering node**: predicates are
    /// evaluated by the local engine, which the fleet merge-on-read path never does.
    /// The answer's `scope` field reports which of the two you got.
    #[serde(default)]
    pub r#match: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct VerifyParams {
    /// The imposter's port.
    pub port: u16,
    /// The verification body: predicate set and expected counts. Every field is
    /// optional, but the object itself is required — `{}` is the empty verification.
    pub options: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LintParams {
    /// An imposter or stub document, as JSON text.
    pub json: String,
    /// A name for the document, used in the findings' locations.
    #[serde(default = "default_source_name")]
    pub source_name: String,
}

fn default_source_name() -> String {
    "<mcp>".to_owned()
}

/// A write's optional precondition (issue #293).
///
/// Flattened into each write tool's params rather than repeated field-by-field, so every
/// mutating tool offers the identical `expected_revision` and one of them cannot quietly
/// drop it. Absent means last-writer-wins — the front's own default.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct Precondition {
    /// The revision this write expects, from a prior read's `current_revision`. Sent as
    /// `If-Match`. When the record has moved on, the write is refused with
    /// `{conflict: true, current_revision}` instead of overwriting the other writer.
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateParams {
    /// The imposter's port. Required — the front will not choose one for you.
    pub port: u16,
    /// The imposter document, as upstream defines it (`protocol`, `stubs`, …). If it
    /// carries a `port`, it must agree with the `port` above.
    pub imposter: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteImposterParams {
    /// The imposter's port.
    pub port: u16,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SetEnabledParams {
    /// The imposter's port.
    pub port: u16,
    /// `true` to enable, `false` to disable.
    pub enabled: bool,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StubAddParams {
    /// The imposter's port.
    pub port: u16,
    /// The stub document: `predicates` and `responses`, as upstream defines them.
    pub stub: Value,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StubByIdParams {
    /// The imposter's port.
    pub port: u16,
    /// The stub's **id** — never its index. Read one from `imposter_get`.
    pub stub_id: String,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StubReplaceParams {
    /// The imposter's port.
    pub port: u16,
    /// The stub's **id** — never its index. Read one from `imposter_get`.
    pub stub_id: String,
    /// The replacement stub document.
    pub stub: Value,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RoutesPutParams {
    /// The whole route table, replaced as a unit.
    pub routes: Value,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RouteDeleteParams {
    /// The route's id.
    pub route_id: String,
    #[serde(flatten)]
    pub precondition: Precondition,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct OpStatusParams {
    /// The `op_id` from a `{parked: true, op_id}` answer.
    pub op_id: String,
}

/// The MCP server: an authenticated client of one admin front.
#[derive(Debug, Clone)]
pub struct RiftMcp {
    client: AdminClient,
    /// Namespaces this process's idempotency keys — see [`SessionNonce`].
    nonce: SessionNonce,
}

// `vis = pub` so the gate can enumerate the registered tools without going through
// a live MCP session — the tool list is an acceptance criterion, not an internal.
#[tool_router(vis = "pub")]
impl RiftMcp {
    pub fn new(client: AdminClient) -> Self {
        Self {
            client,
            nonce: SessionNonce::new(),
        }
    }

    #[tool(
        description = "List all imposters visible to this principal.",
        annotations(read_only_hint = true)
    )]
    pub async fn imposter_list(&self) -> Result<Json<Answer<Value>>, ErrorData> {
        let (data, markers) = self.client.get_json("imposters", &[]).await?;
        Ok(Json(Answer::new(data, None, markers)))
    }

    #[tool(
        description = "Get one imposter by port, including its stubs.",
        annotations(read_only_hint = true)
    )]
    pub async fn imposter_get(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<PortParams>,
    ) -> Result<Json<Answer<Value>>, ErrorData> {
        let path = format!("imposters/{}", params.0.port);
        let (data, markers) = self.client.get_json(&path, &[]).await?;
        Ok(Json(Answer::new(data, None, markers)))
    }

    /// Read the requests an imposter recorded.
    ///
    /// Without `match` this is a fleet-merged read; with `match` it is the answering
    /// node only. The answer says which, in `scope` — never assume it is fleet-wide.
    #[tool(
        description = "Read recorded requests for an imposter. Without `match` the answer is \
                       merged across the fleet; with `match` it covers only the answering node. \
                       The `scope` field reports which, and `partial` is set when some peer did \
                       not answer.",
        annotations(read_only_hint = true)
    )]
    pub async fn requests_query(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<RequestsParams>,
    ) -> Result<Json<Answer<Value>>, ErrorData> {
        let params = params.0;
        let path = format!("imposters/{}/requests", params.port);
        let query = requests_query_params(&params);
        let (data, markers) = self.client.get_json(&path, &query).await?;
        Ok(Json(Answer::new(
            data,
            Some(scope_for(params.r#match.as_deref())),
            markers,
        )))
    }

    #[tool(
        description = "Get the front door's route table.",
        annotations(read_only_hint = true)
    )]
    pub async fn routes_get(&self) -> Result<Json<Answer<Value>>, ErrorData> {
        let (data, markers) = self.client.get_json("front-door/routes", &[]).await?;
        Ok(Json(Answer::new(data, None, markers)))
    }

    #[tool(
        description = "Get cluster health — node readiness and ring membership.",
        annotations(read_only_hint = true)
    )]
    pub async fn fleet_health(&self) -> Result<Json<Answer<Value>>, ErrorData> {
        let (data, markers) = self.client.get_json("_fleet/health", &[]).await?;
        Ok(Json(Answer::new(data, Some(ReadScope::Fleet), markers)))
    }

    #[tool(
        description = "Report the principal this MCP server is authenticated as, and its tenant \
                       bindings. Call this first to learn what you are allowed to do.",
        annotations(read_only_hint = true)
    )]
    pub async fn whoami(&self) -> Result<Json<Answer<Value>>, ErrorData> {
        let (data, markers) = self.client.get_json("admin/whoami", &[]).await?;
        Ok(Json(Answer::new(data, None, markers)))
    }

    #[tool(
        description = "Verify request counts and predicate matches for an imposter. Evaluated by \
                       the engine's own matcher, so results agree with how stubs actually match. \
                       Counts cover the ANSWERING NODE only, not the fleet — the answer's `scope` \
                       says so. On a multi-node fleet, a count can be lower than the true total.",
        annotations(read_only_hint = true)
    )]
    pub async fn verify(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<VerifyParams>,
    ) -> Result<Json<Answer<Value>>, ErrorData> {
        let params = params.0;
        let path = format!("imposters/{}/verify", params.port);
        let (data, markers) = self.client.post_json(&path, &params.options).await?;
        // Node-scoped, not undecided: the front proxies this POST to the local
        // engine and the fleet-count decoration only ever applies to a list read or
        // a single-imposter GET, so the counts are this node's journal alone.
        Ok(Json(Answer::new(data, Some(ReadScope::Node), markers)))
    }

    // ---- write tools (issue #293) --------------------------------------------------
    //
    // Every one of them goes through `AdminClient::write`, so the three semantics are
    // wired in once rather than per-tool: an `Idempotency-Key` derived from this call's
    // id, an optional `If-Match`, and the three-way `WriteOutcome`. A tool that assembled
    // its own request would be a tool that could forget one of them.

    #[tool(
        description = "Create an imposter on an explicit port. The port is required — the front \
                       will not allocate one. Retrying this exact tool call is safe: the write \
                       carries an idempotency key derived from the call id, so a retry after a \
                       timeout dedups instead of creating twice."
    )]
    pub async fn imposter_create(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<CreateParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        let body = create_body(params.port, params.imposter)
            .map_err(|message| ErrorData::invalid_params(message, None))?;
        // No `expected_revision`: `POST /imposters` is not port-addressed, so the front
        // refuses an `If-Match` on it outright ("applies to single-imposter and route-table
        // operations only"). Offering the parameter here would be advertising a precondition
        // that can only ever 400.
        self.write(
            Method::POST,
            "imposters".to_owned(),
            format!("imposters/{}", params.port),
            Some(body),
            &request_id,
            None,
        )
        .await
    }

    #[tool(description = "Delete an imposter by port.")]
    pub async fn imposter_delete(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<DeleteImposterParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        let path = format!("imposters/{}", params.port);
        self.write(
            Method::DELETE,
            path.clone(),
            path,
            None,
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(
        description = "Enable or disable an imposter without deleting it. A disabled imposter \
                       keeps its stubs and its recorded requests."
    )]
    pub async fn imposter_set_enabled(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<SetEnabledParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::POST,
            enabled_path(params.port, params.enabled),
            format!("imposters/{}", params.port),
            None,
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(description = "Append a stub to an imposter.")]
    pub async fn stub_add(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<StubAddParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::POST,
            format!("imposters/{}/stubs", params.port),
            format!("imposters/{}", params.port),
            Some(add_stub_body(params.stub)),
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    /// Replace one stub, addressed by id.
    ///
    /// By id and not by index, deliberately (RFC-006 §8.2). An index-addressed edit is a
    /// read-modify-write of the whole imposter on the answering node, so two agents editing
    /// different stubs concurrently silently clobber each other — the lost-update window the
    /// console also refuses to offer. The front still serves the index routes; this surface
    /// does not project them.
    #[tool(
        description = "Replace one stub, addressed by its id (never by index). Get ids from \
                       imposter_get. Pass expected_revision to make the write conditional: a \
                       stale one is refused with {conflict: true, current_revision} so you can \
                       re-read and retry rather than overwrite someone else's edit."
    )]
    pub async fn stub_replace(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<StubReplaceParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::PUT,
            stub_by_id_path(params.port, &params.stub_id),
            format!("imposters/{}", params.port),
            Some(params.stub),
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(
        description = "Delete one stub, addressed by its id (never by index). Get ids from \
                       imposter_get."
    )]
    pub async fn stub_delete(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<StubByIdParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::DELETE,
            stub_by_id_path(params.port, &params.stub_id),
            format!("imposters/{}", params.port),
            None,
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(
        description = "Replace the front door's whole route table. This is a set-replace, not a \
                       merge: routes absent from `routes` are removed. The table has one revision \
                       per tenant, so any write invalidates an outstanding expected_revision."
    )]
    pub async fn routes_put(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<RoutesPutParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::PUT,
            "front-door/routes".to_owned(),
            "front-door/routes".to_owned(),
            Some(params.routes),
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(description = "Delete one front-door route by id.")]
    pub async fn route_delete(
        &self,
        request_id: rmcp::handler::server::common::RequestId,
        params: rmcp::handler::server::wrapper::Parameters<RouteDeleteParams>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let params = params.0;
        self.write(
            Method::DELETE,
            route_path(&params.route_id),
            "front-door/routes".to_owned(),
            None,
            &request_id,
            params.precondition.expected_revision,
        )
        .await
    }

    #[tool(
        description = "Poll a parked write. When a write answers {parked: true, op_id}, it is \
                       durable but not yet applied — the fleet will replay it. Call this with \
                       that op_id to see whether it has committed. `state` is `pending` while \
                       it is still parked, then `applied` or `failed`; keep polling until it \
                       is one of the latter two.",
        annotations(read_only_hint = true)
    )]
    pub async fn op_status(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<OpStatusParams>,
    ) -> Result<Json<Answer<Value>>, ErrorData> {
        let path = format!("_fleet/ops/{}", encode_segment(&params.0.op_id));
        let (data, markers) = self.client.get_json(&path, &[]).await?;
        Ok(Json(Answer::new(data, Some(ReadScope::Fleet), markers)))
    }

    #[tool(
        description = "Lint an imposter or stub document for mistakes. Runs in this process — no \
                       network call, no side effects, nothing is created or changed.",
        annotations(read_only_hint = true)
    )]
    pub async fn lint(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<LintParams>,
    ) -> Result<Json<Answer<Value>>, ErrorData> {
        let params = params.0;
        Ok(Json(Answer::plain(lint_document(
            &params.json,
            &params.source_name,
        ))))
    }
}

#[tool_handler]
impl ServerHandler for RiftMcp {}

impl RiftMcp {
    /// The one path every write tool takes.
    ///
    /// Not a `#[tool]`: it is the shared body of the nine above, and keeping it here is
    /// what makes "every write carries an idempotency key" a property of the code rather
    /// than of nine call sites remembering to do the same thing.
    async fn write(
        &self,
        method: Method,
        path: String,
        record_path: String,
        body: Option<Value>,
        request_id: &rmcp::handler::server::common::RequestId,
        expected_revision: Option<u64>,
    ) -> Result<Json<WriteOutcome>, ErrorData> {
        let key = idempotency_key(&self.nonce, &request_id.0);
        let outcome = self
            .client
            .write(
                method,
                &path,
                &record_path,
                body.as_ref(),
                key,
                expected_revision,
            )
            .await?;
        Ok(Json(outcome))
    }
}

/// The body for a create: upstream's own imposter document with the port settled.
///
/// The port is a required tool parameter *and* a document field upstream defines, so the
/// two can disagree. Neither is silently preferred: creating an imposter on a port the
/// agent did not name is precisely the mistake this surface exists to make impossible, and
/// the front cannot catch it because by then only one port is left.
fn create_body(port: u16, imposter: Value) -> Result<Value, String> {
    let Value::Object(mut fields) = imposter else {
        return Err(format!(
            "`imposter` must be a JSON object (the imposter document), got {}",
            kind_of(&imposter)
        ));
    };
    match fields.get("port").and_then(Value::as_u64) {
        Some(declared) if declared != u64::from(port) => {
            return Err(format!(
                "`port` is {port} but the imposter document declares port {declared}; \
                 remove one of them or make them agree"
            ));
        }
        _ => {}
    }
    fields.insert("port".to_owned(), Value::from(port));
    Ok(Value::Object(fields))
}

/// The JSON type name, for a refusal that says what was actually passed.
fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// The body for a stub append.
///
/// The two stub-writing routes take **different shapes**, which is upstream's design and
/// not ours to normalize: an append posts `{"stub": …}` (the envelope also carries an
/// optional insert `index`, which this surface never sends — see [`RiftMcp::stub_replace`]),
/// while a by-id replace puts the bare stub. Wrapping both, or neither, earns a `400` from
/// one of them.
fn add_stub_body(stub: Value) -> Value {
    serde_json::json!({ "stub": stub })
}

/// The front's two enable/disable paths. It has no "set enabled" body flag — the verb is
/// the path — so this is a projection of the route, not a re-specification of it.
fn enabled_path(port: u16, enabled: bool) -> String {
    let verb = if enabled { "enable" } else { "disable" };
    format!("imposters/{port}/{verb}")
}

/// The by-id stub path. See [`RiftMcp::stub_replace`] for why there is no by-index sibling.
fn stub_by_id_path(port: u16, stub_id: &str) -> String {
    format!("imposters/{port}/stubs/by-id/{}", encode_segment(stub_id))
}

fn route_path(route_id: &str) -> String {
    format!("front-door/routes/{}", encode_segment(route_id))
}

/// Percent-encode one path segment.
///
/// An id reaches us from the agent, so `/` and `..` in it must stay data. Interpolated raw,
/// `a/../b` would address a different route entirely — and the front deliberately does not
/// percent-decode these segments, so an encoded id is also the only spelling it reads back
/// as the id it was given.
fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

/// Which half of the read path answered.
///
/// Extracted as a pure function so the rule is pinned by a unit test rather than
/// only reachable through a live fleet: this is the assertion #292 cares most about,
/// because getting it wrong tells an agent a one-node answer is the whole cluster's.
fn scope_for(r#match: Option<&str>) -> ReadScope {
    match r#match {
        Some(_) => ReadScope::Node,
        None => ReadScope::Fleet,
    }
}

/// The query string for a requests read. Absent options are omitted rather than
/// sent empty — an empty `match` is a predicate the engine would try to parse.
///
/// Borrows from `params`: `reqwest`'s `query` serializes the slice before the
/// request is ever awaited, so owning the values here would be two allocations
/// per call for nothing.
fn requests_query_params(params: &RequestsParams) -> Vec<(&'static str, &str)> {
    let mut query = Vec::new();
    if let Some(since) = &params.since {
        query.push(("since", since.as_str()));
    }
    if let Some(m) = &params.r#match {
        query.push(("match", m.as_str()));
    }
    query
}

/// Lint in-process through the `rift-cluster-base` facade.
///
/// Invalid JSON is a *finding*, not an error: an agent asking "is this document ok?"
/// about a malformed document has asked a question the linter can answer.
fn lint_document(json: &str, source_name: &str) -> Value {
    use rift_cluster_base::rift_lint::{LintOptions, lint_json};

    let result = lint_json(json, source_name, &LintOptions {});
    serde_json::json!({
        "issues": result.issues,
        "errors": result.errors,
        "warnings": result.warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// E7 — a `match` predicate is evaluated by the local engine, so the answer is
    /// that node's alone. Saying "fleet" here would be a lie an agent cannot detect.
    #[test]
    fn requests_scope_is_node_with_match() {
        assert_eq!(
            scope_for(Some(r#"{"equals":{"method":"GET"}}"#)),
            ReadScope::Node
        );
    }

    /// E8 — without a predicate the read is the fleet merge-on-read path
    /// (#223/#225/#229, all shipped). Reporting "node" would understate it.
    #[test]
    fn requests_scope_is_fleet_without_match() {
        assert_eq!(scope_for(None), ReadScope::Fleet);
    }

    /// An empty predicate string is still a predicate — it is sent, and it still
    /// makes the read node-scoped.
    #[test]
    fn empty_match_string_is_still_node_scoped() {
        assert_eq!(scope_for(Some("")), ReadScope::Node);
    }

    #[test]
    fn requests_query_omits_absent_options() {
        let params = RequestsParams {
            port: 4545,
            since: None,
            r#match: None,
        };
        assert!(requests_query_params(&params).is_empty());
    }

    #[test]
    fn requests_query_carries_both_options() {
        let params = RequestsParams {
            port: 4545,
            since: Some("cursor-7".to_owned()),
            r#match: Some(r#"{"equals":{"method":"GET"}}"#.to_owned()),
        };
        assert_eq!(
            requests_query_params(&params),
            vec![
                ("since", "cursor-7"),
                ("match", r#"{"equals":{"method":"GET"}}"#),
            ]
        );
    }

    /// E13 — a malformed document produces findings, not an error and not a panic.
    #[test]
    fn lint_reports_findings_for_invalid_json() {
        let result = lint_document("{ this is not json", "bad.json");
        let errors = result["errors"].as_u64().expect("errors must be a number");
        assert!(
            errors >= 1,
            "invalid JSON must produce at least one error finding, got {result}"
        );
    }

    /// A well-formed, valid imposter lints clean — otherwise the tool would report
    /// noise on every correct document and agents would learn to ignore it.
    #[test]
    fn lint_is_clean_for_a_valid_imposter() {
        let doc = r#"{"port":4545,"protocol":"http","stubs":[]}"#;
        let result = lint_document(doc, "good.json");
        assert_eq!(
            result["errors"].as_u64(),
            Some(0),
            "a valid imposter must lint without errors, got {result}"
        );
    }

    /// The lint answer always carries all three keys, so an agent can read it
    /// without probing for optional fields.
    #[test]
    fn lint_answer_shape_is_stable() {
        let result = lint_document("{}", "empty.json");
        for key in ["issues", "errors", "warnings"] {
            assert!(
                result.get(key).is_some(),
                "lint answer must carry `{key}`: {result}"
            );
        }
    }

    // ---- #293 gate: write tools ------------------------------------------------------

    /// E14 — `imposter_create` takes the port as its own required parameter (so the
    /// schema states the front's rule) *and* accepts the document upstream defines.
    /// When both name a port and they disagree, that is a mistake to report, not a
    /// coin to flip: picking either silently creates an imposter on a port the agent
    /// did not ask for.
    #[test]
    fn a_create_body_with_a_disagreeing_port_is_refused() {
        let err = create_body(4545, serde_json::json!({"port": 9999, "protocol": "http"}))
            .expect_err("a disagreeing port must be refused");
        assert!(
            err.contains("4545") && err.contains("9999"),
            "the refusal must name both ports: {err}"
        );
    }

    /// The port parameter is authoritative when the document omits it — which is the
    /// ordinary case, since the schema already made the agent supply it.
    #[test]
    fn a_create_body_takes_its_port_from_the_parameter() {
        let body = create_body(4545, serde_json::json!({"protocol": "http"}))
            .expect("a document without a port is fine");
        assert_eq!(body["port"], 4545);
        assert_eq!(body["protocol"], "http");
    }

    /// Agreement is not a conflict.
    #[test]
    fn a_create_body_accepts_a_port_that_agrees() {
        let body = create_body(4545, serde_json::json!({"port": 4545, "protocol": "http"}))
            .expect("an agreeing port is fine");
        assert_eq!(body["port"], 4545);
    }

    /// A non-object document is refused rather than wrapped: `{"port": N}` grafted
    /// onto an array would be a request shape the API never defined.
    #[test]
    fn a_create_body_must_be_an_object() {
        assert!(create_body(4545, serde_json::json!([1, 2, 3])).is_err());
        assert!(create_body(4545, serde_json::json!("nope")).is_err());
    }

    /// The append envelope. Pinned because the two stub-writing routes disagree about it
    /// and the compiler cannot tell: `POST .../stubs` wants `{"stub": …}` while
    /// `PUT .../stubs/by-id/{id}` wants the bare stub, so getting this backwards is a
    /// `400 missing field \`stub\`` from a live front and nothing sooner. That is exactly
    /// how it was caught.
    #[test]
    fn an_appended_stub_is_wrapped_but_a_replacement_is_not() {
        let stub = serde_json::json!({ "responses": [] });
        assert_eq!(
            add_stub_body(stub.clone()),
            serde_json::json!({ "stub": stub })
        );
    }

    /// The append envelope never carries an insert `index`: position-addressing is the
    /// thing this surface refuses to offer.
    #[test]
    fn an_appended_stub_carries_no_index() {
        let body = add_stub_body(serde_json::json!({ "responses": [] }));
        assert_eq!(
            body.get("index"),
            None,
            "an append must not position-address"
        );
    }

    /// E13 — the enable/disable verb is the front's own two paths, not a body flag.
    #[test]
    fn set_enabled_maps_to_the_two_front_paths() {
        assert_eq!(enabled_path(4545, true), "imposters/4545/enable");
        assert_eq!(enabled_path(4545, false), "imposters/4545/disable");
    }

    /// E13 — stub edits address a stub by id, never by index. The index-addressed
    /// routes exist on the front and are deliberately not projected here: they are a
    /// read-modify-write of the whole imposter, which is the lost-update window the
    /// console also refuses to offer.
    #[test]
    fn stub_paths_are_by_id() {
        assert_eq!(
            stub_by_id_path(4545, "abc-123"),
            "imposters/4545/stubs/by-id/abc-123"
        );
    }

    /// A stub id is percent-encoded into the path: an id containing `/` must not
    /// smuggle an extra path segment and address a different route entirely.
    #[test]
    fn a_stub_id_cannot_smuggle_a_path_segment() {
        let path = stub_by_id_path(4545, "a/../b");
        assert!(
            !path.contains("a/../b"),
            "the id must be encoded, not interpolated raw: {path}"
        );
        assert!(path.starts_with("imposters/4545/stubs/by-id/"), "{path}");
    }

    /// Same rule for a route id on the delete path.
    #[test]
    fn a_route_id_cannot_smuggle_a_path_segment() {
        let path = route_path("a/b");
        assert!(!path.contains("a/b"), "the id must be encoded: {path}");
        assert!(path.starts_with("front-door/routes/"), "{path}");
    }
}
