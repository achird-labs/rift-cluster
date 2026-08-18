//! The v1 tool set: eight read-only tools over the clustered admin API.
//!
//! The MCP surface is the admin API **re-projected, not re-specified** (RFC-006 §8.2) —
//! every tool is one call to an endpoint that already exists and is already schema'd in
//! `docs/api/openapi-ee.yaml`. Nothing here re-implements server behaviour; where a
//! judgement could be made in two places (predicate matching, most obviously), it is
//! made on the server.

use rmcp::handler::server::wrapper::Json;
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use super::client::{AdminClient, Answer, ReadScope, ToolFailure};

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

/// The MCP server: an authenticated client of one admin front.
#[derive(Debug, Clone)]
pub struct RiftMcp {
    client: AdminClient,
}

// `vis = pub` so the gate can enumerate the registered tools without going through
// a live MCP session — the tool list is an acceptance criterion, not an internal.
#[tool_router(vis = "pub")]
impl RiftMcp {
    pub fn new(client: AdminClient) -> Self {
        Self { client }
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
}
