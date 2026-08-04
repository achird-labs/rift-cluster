//! The OpenAPI 3.0 → imposter-JSON compiler (RFC-004 §3.1–§3.2).
//!
//! `compile` is a pure function of `(spec bytes, options)`: no clock, no network, no filesystem, no
//! global state. That is load-bearing rather than tidy — the output is committed through consensus,
//! so two nodes compiling the same bytes must agree exactly, and a re-import diff is only meaningful
//! if "same input, same output" holds without qualification.
//!
//! It emits the same JSON a client would `PUT /imposters`, and deliberately depends on neither
//! `rift-cluster-base` nor anything vendored. Type safety is enforced where it is load-bearing —
//! at admission, where the JSON passes the same gate as any other write — which keeps this crate a
//! text-to-text function that golden files can pin.

mod digest;
mod error;
mod path;
mod schema;
mod synth;
mod validate;

use std::collections::BTreeSet;

use openapiv3::{
    Components, MediaType, OpenAPI, Operation, Parameter, PathItem, ReferenceOr, Response,
    StatusCode,
};
use serde_json::{Map, Value};

pub use digest::SpecDigest;
pub use error::CompileError;
pub use schema::{SchemaNode, SchemaShape, TextFormat};
pub use validate::{Violation, ViolationKind, validate_stub_response};

use digest::Rng;

/// The pre-commit size cap (RFC-004 §4.1). A spec is attacker-influenceable input on its way to the
/// Raft log, so it is bounded before it is parsed rather than after.
pub const MAX_SPEC_BYTES: usize = 4 * 1024 * 1024;

/// The header a test sets to reach a declared non-default status without editing the imposter.
pub const STATUS_DISCRIMINATOR_HEADER: &str = "X-Rift-Spec-Status";

/// Prefix on every generated stub id. Drift classification keys on it to tell generated stubs from
/// hand-added ones, so it is part of the contract, not decoration.
pub const STUB_ID_PREFIX: &str = "spec";

/// Knobs the caller supplies; everything else about the output is a function of the spec bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileOptions {
    /// Port for the compiled imposter. `None` lets the engine auto-assign.
    pub port: Option<u16>,
    pub name: Option<String>,
    pub max_bytes: usize,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            port: None,
            name: None,
            max_bytes: MAX_SPEC_BYTES,
        }
    }
}

/// `operationId`, or one synthesized from the method and path when the spec omits it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId(String);

impl OperationId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which declared response a stub implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatusKey {
    /// The spec's `default` response.
    Default,
    Code(u16),
    /// A `2XX`-style range.
    Range(u16),
}

impl StatusKey {
    /// The status component of a stub id, and the value of the discriminator header.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Default => "default".to_string(),
            Self::Code(code) => code.to_string(),
            Self::Range(leading) => format!("{leading}XX"),
        }
    }

    /// The status code the stub actually serves. A `default` response names no code; it is the
    /// operation's unconditional answer, so it serves 200.
    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::Default => 200,
            Self::Code(code) => *code,
            Self::Range(leading) => leading.saturating_mul(100),
        }
    }
}

impl std::fmt::Display for StatusKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

/// The compiler's output: what to deploy, plus the index used to diff and validate it.
#[derive(Debug, Clone)]
pub struct CompiledSpec {
    /// Canonical `ImposterConfig` JSON — exactly what a client would `PUT`.
    pub imposter: Value,
    pub operations: Vec<CompiledOperation>,
    pub digest: SpecDigest,
}

impl CompiledSpec {
    #[must_use]
    pub fn operation(&self, id: &str) -> Option<&CompiledOperation> {
        self.operations.iter().find(|op| op.id.as_str() == id)
    }
}

/// One operation, in emitted (most-literal-first) order.
#[derive(Debug, Clone)]
pub struct CompiledOperation {
    pub id: OperationId,
    pub method: String,
    /// `/users/{id}` — the template as written in the spec.
    pub path_template: String,
    pub stub_ids: Vec<String>,
    /// The per-status response contracts. Beyond RFC-004 §3.1's sketch, and necessarily so: the
    /// RFC's own `validate_stub_response(op, status, body)` has nothing to validate against
    /// without them.
    pub responses: Vec<CompiledResponse>,
}

impl CompiledOperation {
    #[must_use]
    pub fn response(&self, status: &StatusKey) -> Option<&CompiledResponse> {
        self.responses.iter().find(|r| r.status == *status)
    }
}

/// One declared response, and the stub that serves it.
#[derive(Debug, Clone)]
pub struct CompiledResponse {
    pub status: StatusKey,
    pub stub_id: String,
    /// The media type key the body was taken from, `None` when the response declares no content.
    pub content_type: Option<String>,
    pub schema: SchemaNode,
}

/// Compile an OpenAPI 3.0.x document (JSON or YAML) into imposter JSON.
///
/// # Errors
///
/// Refuses, by name, anything it will not silently half-understand: a version other than 3.0.x, a
/// document over `options.max_bytes`, an external `$ref`, a document it cannot parse, and — via
/// [`CompileError::SelfCheck`] — its own output failing the contract it just emitted.
pub fn compile(spec_bytes: &[u8], options: &CompileOptions) -> Result<CompiledSpec, CompileError> {
    if spec_bytes.len() > options.max_bytes {
        return Err(CompileError::TooLarge {
            max: options.max_bytes,
        });
    }

    // YAML is a superset of JSON, so one parse covers both input forms. This `Value` is only for
    // the pre-flight gates below, which need to see the document as written.
    let document: Value =
        serde_yaml::from_slice(spec_bytes).map_err(|e| CompileError::Parse(e.to_string()))?;

    require_supported_version(&document)?;
    validate_refs(&document)?;
    validate_status_keys(&document)?;

    // Deserialized from the original bytes rather than from `document`: `serde_json::Value` holds
    // its objects in a `BTreeMap`, so round-tripping through it would hand `openapiv3` every map
    // in *alphabetical* order. Response order decides which status answers unconditionally and
    // media-type order decides the response's Content-Type, so that would quietly replace "spec
    // order" — which RFC-004 §3.2 specifies — with "alphabetical order".
    let api: OpenAPI =
        serde_yaml::from_slice(spec_bytes).map_err(|e| CompileError::Parse(e.to_string()))?;
    let digest = SpecDigest::of(spec_bytes);
    let components = api.components.as_ref();

    // One budget per document, not per response — see `schema::MAX_SCHEMA_NODES`.
    let mut schema_budget = schema::MAX_SCHEMA_NODES;
    let mut body_budget = synth::MAX_BODY_NODES;

    let mut planned = plan_operations(&api, components, &mut schema_budget)?;
    planned.sort_by(|a, b| a.order.cmp(&b.order));
    reject_duplicate_ids(&planned)?;

    let mut stubs = Vec::new();
    let mut operations = Vec::new();
    for plan in planned {
        let (stub_group, compiled) = emit(plan, &digest, &mut body_budget)?;
        stubs.extend(stub_group);
        operations.push(compiled);
    }

    let mut imposter = Map::new();
    if let Some(port) = options.port {
        imposter.insert("port".to_string(), Value::from(port));
    }
    imposter.insert("protocol".to_string(), Value::from("http"));
    if let Some(name) = &options.name {
        imposter.insert("name".to_string(), Value::from(name.clone()));
    }
    imposter.insert("stubs".to_string(), Value::Array(stubs));

    Ok(CompiledSpec {
        imposter: canonicalize(&Value::Object(imposter)),
        operations,
        digest,
    })
}

/// Serialize to the canonical byte form: object keys sorted at every depth.
///
/// Sorting explicitly rather than relying on `serde_json`'s map backing — which is sorted today but
/// becomes insertion-ordered the moment anything in the dependency graph enables `preserve_order` —
/// keeps "same bytes in, same bytes out" a property of this crate rather than of feature unification.
#[must_use]
pub fn to_canonical_string(value: &Value) -> String {
    // Infallible by construction: a `serde_json::Value` contains no type that can fail to
    // serialize (`Number` rejects NaN/Infinity at construction, so a non-finite float cannot be
    // held here in the first place). Deliberately NOT `unwrap_or_default()` — this function's whole
    // contract is byte-identity, so a silent `""` would make two different documents compare equal
    // and report "no drift" to the one caller that must never be told that wrongly.
    serde_json::to_string(&canonicalize(value)).expect("a serde_json::Value is always serializable")
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: Vec<(&String, &Value)> = map.iter().collect();
            sorted.sort_by_key(|(key, _)| *key);
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

fn require_supported_version(document: &Value) -> Result<(), CompileError> {
    match document.get("openapi").and_then(Value::as_str) {
        Some(version) if version.starts_with("3.0.") || version == "3.0" => Ok(()),
        Some(version) => Err(CompileError::UnsupportedVersion {
            found: version.to_string(),
        }),
        None => Err(CompileError::UnsupportedVersion {
            found: match document.get("swagger").and_then(Value::as_str) {
                Some(version) => format!("swagger {version}"),
                None => "unknown (no `openapi` or `swagger` field)".to_string(),
            },
        }),
    }
}

/// Check every `$ref` in the document before anything is compiled.
///
/// This is a refusal gate, so it **fails closed**: a `$ref` it cannot positively verify as a
/// resolvable in-document pointer is refused, never waved through. That matters because every way
/// of getting this wrong is quiet — an external ref that slips past becomes an SSRF/one-fetch-rule
/// violation (RFC-004 §3.1, §8); a non-string `$ref` deserializes into a schema with no constraints
/// at all, so the mock silently answers `null` where the author specified a type; and a dangling
/// in-document pointer does the same. All three produce a mock that looks authoritative and is
/// wrong, which is the single failure this compiler exists to prevent.
///
/// Doing it here, over the raw document, is what makes the `Unconstrained` fallbacks downstream
/// unreachable rather than merely silent.
fn validate_refs(document: &Value) -> Result<(), CompileError> {
    fn walk(node: &Value, root: &Value) -> Result<(), CompileError> {
        match node {
            Value::Object(map) => {
                if let Some(reference) = map.get("$ref") {
                    let Value::String(reference) = reference else {
                        return Err(CompileError::Parse(format!(
                            "$ref must be a string, found {reference}"
                        )));
                    };
                    if !reference.starts_with('#') {
                        return Err(CompileError::ExternalRef {
                            reference: reference.clone(),
                        });
                    }
                    if resolve_pointer(root, reference).is_none() {
                        return Err(CompileError::Parse(format!(
                            "$ref {reference:?} does not resolve within the document"
                        )));
                    }
                    // Resolving is not enough: this compiler only reads three component tables, and
                    // a pointer aimed anywhere else would fall through to a schema with no
                    // constraints — a response compiled with no contract at all, reported as success.
                    if !RESOLVABLE_COMPONENTS
                        .iter()
                        .any(|kind| reference.starts_with(&format!("#/components/{kind}/")))
                    {
                        return Err(CompileError::Parse(format!(
                            "$ref {reference:?} points outside components.{{{}}}, which is all this \
                             compiler resolves",
                            RESOLVABLE_COMPONENTS.join(",")
                        )));
                    }
                }
                for value in map.values() {
                    walk(value, root)?;
                }
                Ok(())
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, root)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    walk(document, document)
}

/// The component tables this compiler reads. A `$ref` into anything else is refused by
/// `validate_refs` rather than silently compiling to an unconstrained response.
const RESOLVABLE_COMPONENTS: [&str; 3] = ["schemas", "parameters", "responses"];

/// Reject a response key `openapiv3` would drop on the floor.
///
/// A key like `"99"`, `"1000"` or `"abc"` does not deserialize into `StatusCode`, and the whole
/// entry vanishes before the compiler sees it — yielding an operation with no stubs at all,
/// returned as a successful compile. `"0XX"` parses but would emit `statusCode: 0`, which is not a
/// valid HTTP status.
fn validate_status_keys(document: &Value) -> Result<(), CompileError> {
    let Some(paths) = document.get("paths").and_then(Value::as_object) else {
        return Ok(());
    };
    for (template, item) in paths {
        let Some(item) = item.as_object() else {
            continue;
        };
        for (method, operation) in item {
            let Some(responses) = operation.get("responses").and_then(Value::as_object) else {
                continue;
            };
            for key in responses.keys() {
                if key == "default" {
                    continue;
                }
                let recognised = match key.strip_suffix("XX") {
                    Some(leading) => matches!(leading.parse::<u16>(), Ok(1..=5)),
                    None => matches!(key.parse::<u16>(), Ok(100..=599)),
                };
                if !recognised {
                    return Err(CompileError::Parse(format!(
                        "{method} {template}: response key {key:?} is not `default`, an HTTP \
                         status 100-599, or a 1XX-5XX range"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Resolve an RFC 6901 JSON pointer written as an OpenAPI local reference (`#/components/...`).
fn resolve_pointer<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root);
    }
    let mut current = root;
    for raw in pointer.strip_prefix('/')?.split('/') {
        // RFC 6901 unescaping, `~1` before `~0` so an encoded `~1` is not re-read as a separator.
        let token = raw.replace("~1", "/").replace("~0", "~");
        current = match current {
            Value::Object(map) => map.get(&token)?,
            Value::Array(items) => items.get(token.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// An operation with everything needed to emit it, plus its sort key.
struct Plan {
    id: OperationId,
    method: &'static str,
    template: String,
    path: path::CompiledPath,
    required_query: Vec<String>,
    required_headers: Vec<String>,
    responses: Vec<PlannedResponse>,
    order: (
        std::cmp::Reverse<usize>,
        std::cmp::Reverse<usize>,
        std::cmp::Reverse<usize>,
        String,
        String,
    ),
}

struct PlannedResponse {
    status: StatusKey,
    content_type: Option<String>,
    schema: SchemaNode,
    example: Option<Value>,
}

const METHODS: [&str; 8] = [
    "GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH", "TRACE",
];

fn method_operation<'a>(item: &'a PathItem, method: &str) -> Option<&'a Operation> {
    match method {
        "GET" => item.get.as_ref(),
        "PUT" => item.put.as_ref(),
        "POST" => item.post.as_ref(),
        "DELETE" => item.delete.as_ref(),
        "OPTIONS" => item.options.as_ref(),
        "HEAD" => item.head.as_ref(),
        "PATCH" => item.patch.as_ref(),
        "TRACE" => item.trace.as_ref(),
        _ => None,
    }
}

fn plan_operations(
    api: &OpenAPI,
    components: Option<&Components>,
    schema_budget: &mut usize,
) -> Result<Vec<Plan>, CompileError> {
    let mut plans = Vec::new();
    for (template, item) in &api.paths.paths {
        let ReferenceOr::Item(item) = item else {
            // OpenAPI 3.0 has no `components.pathItems` to resolve against, so this is refused
            // rather than skipped: dropping it would delete a route from the compiled mock with a
            // successful return, and a route that silently does not exist is indistinguishable
            // from one the spec never declared.
            return Err(CompileError::Parse(format!(
                "path {template:?} is declared as a $ref, which this compiler does not resolve"
            )));
        };
        let compiled_path = path::compile_path(template);

        for method in METHODS {
            let Some(operation) = method_operation(item, method) else {
                continue;
            };
            let id = OperationId(match &operation.operation_id {
                Some(id) => id.clone(),
                None => synthesize_operation_id(method, template),
            });

            let mut required_query = Vec::new();
            let mut required_headers = Vec::new();
            for parameter in item.parameters.iter().chain(operation.parameters.iter()) {
                // Resolved, not skipped. Sharing parameters through `components.parameters` is the
                // ordinary way real specs are written, and dropping one silently would make the
                // compiled mock *more permissive* than the contract: a client omitting a required
                // header would get the 200 the real API answers with 400.
                let parameter = resolve_parameter(parameter, components)?;
                // Optional parameters compile to no predicate at all: the mock must accept what the
                // spec permits, not only what it illustrates.
                match parameter {
                    Parameter::Query { parameter_data, .. } if parameter_data.required => {
                        required_query.push(parameter_data.name.clone());
                    }
                    Parameter::Header { parameter_data, .. } if parameter_data.required => {
                        required_headers.push(parameter_data.name.clone());
                    }
                    // A required path parameter is already enforced by the path regex, and cookies
                    // have no predicate field in the engine.
                    _ => {}
                }
            }

            plans.push(Plan {
                id,
                method,
                template: template.clone(),
                order: path::order_key(&compiled_path, template, method),
                path: compiled_path.clone(),
                required_query,
                required_headers,
                responses: plan_responses(operation, components, schema_budget)?,
            });
        }
    }
    Ok(plans)
}

/// Follow a `$ref`'d component one level. `validate_refs` has already proved every reference
/// resolves within the document, so failure here means the pointer aims somewhere this compiler
/// does not read from — refused by name, because the alternative is a dropped constraint.
fn resolve_component<'a, T>(
    reference: &str,
    kind: &str,
    table: Option<&'a indexmap::IndexMap<String, ReferenceOr<T>>>,
) -> Result<&'a T, CompileError> {
    let target = reference
        .strip_prefix(&format!("#/components/{kind}/"))
        .and_then(|name| table.and_then(|table| table.get(name)));
    match target {
        Some(ReferenceOr::Item(item)) => Ok(item),
        // A reference chain is legal OpenAPI but vanishingly rare; refusing is honest, and
        // following it blindly risks a cycle.
        Some(ReferenceOr::Reference { .. }) => Err(CompileError::Parse(format!(
            "$ref {reference:?} points at another $ref, which this compiler does not follow"
        ))),
        None => Err(CompileError::Parse(format!(
            "$ref {reference:?} does not name an entry in components.{kind}"
        ))),
    }
}

fn resolve_parameter<'a>(
    parameter: &'a ReferenceOr<Parameter>,
    components: Option<&'a Components>,
) -> Result<&'a Parameter, CompileError> {
    match parameter {
        ReferenceOr::Item(parameter) => Ok(parameter),
        ReferenceOr::Reference { reference } => {
            resolve_component(reference, "parameters", components.map(|c| &c.parameters))
        }
    }
}

fn resolve_response<'a>(
    response: &'a ReferenceOr<Response>,
    components: Option<&'a Components>,
) -> Result<&'a Response, CompileError> {
    match response {
        ReferenceOr::Item(response) => Ok(response),
        ReferenceOr::Reference { reference } => {
            resolve_component(reference, "responses", components.map(|c| &c.responses))
        }
    }
}

/// `default` first — it is the stub that answers unconditionally — then declared statuses in spec
/// order.
///
/// `$ref`'d responses are resolved rather than skipped: `'404': { $ref: '#/components/responses/
/// NotFound' }` is an ordinary way to write a spec, and dropping it would leave the status with no
/// stub at all while `compile` still returned `Ok`.
fn plan_responses(
    operation: &Operation,
    components: Option<&Components>,
    schema_budget: &mut usize,
) -> Result<Vec<PlannedResponse>, CompileError> {
    let mut out = Vec::new();
    if let Some(default) = &operation.responses.default {
        let response = resolve_response(default, components)?;
        out.push(plan_response(
            StatusKey::Default,
            response,
            components,
            schema_budget,
        ));
    }
    for (code, response) in &operation.responses.responses {
        let response = resolve_response(response, components)?;
        let status = match code {
            StatusCode::Code(code) => StatusKey::Code(*code),
            StatusCode::Range(leading) => StatusKey::Range(*leading),
        };
        out.push(plan_response(status, response, components, schema_budget));
    }
    Ok(out)
}

fn plan_response(
    status: StatusKey,
    response: &Response,
    components: Option<&Components>,
    schema_budget: &mut usize,
) -> PlannedResponse {
    // First media type in spec order: the author's ordering is the only signal available, and
    // picking a favourite content type would silently override it.
    let Some((content_type, media)) = response.content.iter().next() else {
        return PlannedResponse {
            status,
            content_type: None,
            schema: SchemaNode::unconstrained(),
            example: None,
        };
    };
    PlannedResponse {
        status,
        content_type: Some(content_type.clone()),
        schema: match &media.schema {
            Some(schema) => schema::normalize(schema, components, schema_budget),
            None => SchemaNode::unconstrained(),
        },
        example: declared_example(media),
    }
}

fn declared_example(media: &MediaType) -> Option<Value> {
    if let Some(example) = &media.example {
        return Some(example.clone());
    }
    media.examples.values().find_map(|example| match example {
        ReferenceOr::Item(example) => example.value.clone(),
        ReferenceOr::Reference { .. } => None,
    })
}

/// A stable id for an operation the spec left unnamed.
///
/// The trailing digest is not decoration: collapsing every separator to `_` makes `/a-b/c` and
/// `/a/b/c` the same id, and `reject_duplicate_ids` would then refuse a perfectly legal spec.
fn synthesize_operation_id(method: &str, template: &str) -> String {
    let mut out = method.to_lowercase();
    let mut last_was_separator = true;
    for ch in template.chars() {
        if ch.is_ascii_alphanumeric() {
            if last_was_separator {
                out.push('_');
                last_was_separator = false;
            }
            out.push(ch);
        } else {
            last_was_separator = true;
        }
    }
    let mut discriminator = SpecDigest::of(format!("{method} {template}").as_bytes()).to_hex();
    discriminator.truncate(8);
    out.push('_');
    out.push_str(&discriminator);
    out
}

/// Stub ids must be collision-free within one spec, so a spec that reuses an `operationId` is
/// refused here rather than producing two stubs sharing an id for the engine's duplicate-id
/// admission check to reject later and far less legibly.
fn reject_duplicate_ids(plans: &[Plan]) -> Result<(), CompileError> {
    let mut seen = BTreeSet::new();
    for plan in plans {
        // `spec:<operation_id>:<status>` is a grammar that RFC-004 §3.5's drift classifier splits
        // on, so a colon inside the id would make the id parse as a different operation and status.
        if plan.id.as_str().contains(':') {
            return Err(CompileError::Parse(format!(
                "operationId {:?} contains ':', which would break the spec:<id>:<status> stub-id \
                 grammar",
                plan.id.as_str()
            )));
        }
        if !seen.insert(plan.id.as_str()) {
            return Err(CompileError::Parse(format!(
                "duplicate operation id {:?}: stub ids would collide",
                plan.id.as_str()
            )));
        }
    }
    Ok(())
}

fn emit(
    plan: Plan,
    digest: &SpecDigest,
    body_budget: &mut usize,
) -> Result<(Vec<Value>, CompiledOperation), CompileError> {
    let mut stubs = Vec::new();
    let mut responses = Vec::new();
    let mut stub_ids = Vec::new();
    // Mountebank matches first-match-wins, and this stub carries no discriminator — its predicates
    // are a strict subset of every other stub's for this operation. Emitted first it would shadow
    // all of them, so the `X-Rift-Spec-Status` opt-in of RFC-004 §3.2 could never select anything
    // and, for an operation with a `default` response, the mock would answer the *error* body on
    // the happy path. It is therefore held back and appended last, as the catch-all.
    let mut unconditional: Option<(String, Value)> = None;

    for (index, planned) in plan.responses.iter().enumerate() {
        let stub_id = format!(
            "{STUB_ID_PREFIX}:{}:{}",
            plan.id.as_str(),
            planned.status.label()
        );

        let body = match &planned.example {
            // Spec examples first, verbatim (RFC-004 §3.2).
            Some(example) => Some(example.clone()),
            None if planned.content_type.is_some() && !planned.schema.is_unconstrained() => {
                let mut rng = Rng::seeded(digest, plan.id.as_str(), &planned.status.label());
                Some(synth::synthesize(&planned.schema, &mut rng, body_budget))
            }
            None => None,
        };

        // The self-check of RFC-004 §3.2: whatever we are about to emit must satisfy the contract
        // we emit alongside it. Failing compilation is the point — deploying a mock that
        // contradicts its own spec is the silent version of the same bug.
        if let Some(body) = &body
            && let Some(violation) = validate::check_body(&planned.schema, body).first()
        {
            return Err(CompileError::SelfCheck {
                operation: plan.id.as_str().to_string(),
                status: planned.status.label(),
                detail: violation.to_string(),
            });
        }

        let mut is_response = Map::new();
        is_response.insert(
            "statusCode".to_string(),
            Value::from(planned.status.http_status()),
        );
        if let Some(content_type) = &planned.content_type {
            let mut headers = Map::new();
            headers.insert(
                "Content-Type".to_string(),
                Value::from(content_type.clone()),
            );
            is_response.insert("headers".to_string(), Value::Object(headers));
        }
        if let Some(body) = body {
            is_response.insert("body".to_string(), body);
        }

        let mut stub = Map::new();
        stub.insert("id".to_string(), Value::from(stub_id.clone()));
        stub.insert(
            "predicates".to_string(),
            Value::Array(predicates(&plan, planned.status, index == 0)),
        );
        if let Some(route_pattern) = &plan.path.route_pattern {
            stub.insert(
                "routePattern".to_string(),
                Value::from(route_pattern.clone()),
            );
        }
        stub.insert(
            "responses".to_string(),
            Value::Array(vec![Value::Object(
                [("is".to_string(), Value::Object(is_response))]
                    .into_iter()
                    .collect(),
            )]),
        );

        if index == 0 {
            unconditional = Some((stub_id.clone(), Value::Object(stub)));
        } else {
            stubs.push(Value::Object(stub));
            stub_ids.push(stub_id.clone());
        }
        responses.push(CompiledResponse {
            status: planned.status,
            stub_id,
            content_type: planned.content_type.clone(),
            schema: planned.schema.clone(),
        });
    }

    if let Some((stub_id, stub)) = unconditional {
        stubs.push(stub);
        stub_ids.push(stub_id);
    }

    Ok((
        stubs,
        CompiledOperation {
            id: plan.id,
            method: plan.method.to_string(),
            path_template: plan.template,
            stub_ids,
            responses,
        },
    ))
}

fn predicates(plan: &Plan, status: StatusKey, unconditional: bool) -> Vec<Value> {
    let mut out = vec![
        serde_json::json!({ "equals": { "method": plan.method } }),
        serde_json::json!({ "matches": { "path": plan.path.regex }, "caseSensitive": true }),
    ];
    for name in &plan.required_query {
        out.push(serde_json::json!({ "exists": { "query": { name.clone(): true } } }));
    }
    for name in &plan.required_headers {
        out.push(serde_json::json!({ "exists": { "headers": { name.clone(): true } } }));
    }
    // The first stub answers unconditionally; every other declared status is opt-in, so a test can
    // reach a declared error without editing the imposter.
    if !unconditional {
        out.push(serde_json::json!({
            "equals": { "headers": { STATUS_DISCRIMINATOR_HEADER: status.label() } }
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_response_serves_two_hundred_and_a_range_serves_its_floor() {
        assert_eq!(StatusKey::Default.http_status(), 200);
        assert_eq!(StatusKey::Default.label(), "default");
        assert_eq!(StatusKey::Range(4).http_status(), 400);
        assert_eq!(StatusKey::Range(4).label(), "4XX");
        assert_eq!(StatusKey::Code(204).label(), "204");
    }

    #[test]
    fn a_synthesised_operation_id_is_stable_and_readable() {
        assert!(synthesize_operation_id("GET", "/users/{id}").starts_with("get_users_id_"));
        assert!(synthesize_operation_id("POST", "/").starts_with("post_"));
        assert_eq!(
            synthesize_operation_id("GET", "/a-b/c"),
            synthesize_operation_id("GET", "/a-b/c"),
            "the same route always synthesises the same id",
        );
    }

    /// Collapsing every separator to `_` made `/a-b/c` and `/a/b/c` collide, which
    /// `reject_duplicate_ids` then refused — a legal spec that would not compile. The suffix makes
    /// the ids collision-free by construction instead.
    #[test]
    fn synthesised_ids_do_not_collide_across_distinct_routes() {
        assert_ne!(
            synthesize_operation_id("GET", "/a-b/c"),
            synthesize_operation_id("GET", "/a/b/c"),
        );
        assert_ne!(
            synthesize_operation_id("GET", "/a"),
            synthesize_operation_id("PUT", "/a"),
        );
    }

    #[test]
    fn an_external_ref_is_found_wherever_it_hides() {
        let document = serde_json::json!({
            "paths": { "/x": { "get": { "responses": {
                "200": { "content": { "a/b": { "schema": { "$ref": "other.yaml#/X" } } } }
            } } } }
        });
        match validate_refs(&document) {
            Err(CompileError::ExternalRef { reference }) => assert_eq!(reference, "other.yaml#/X"),
            other => panic!("expected ExternalRef, got {other:?}"),
        }
    }

    #[test]
    fn a_resolvable_local_ref_passes_the_gate() {
        let document = serde_json::json!({
            "schema": { "$ref": "#/components/schemas/X" },
            "components": { "schemas": { "X": { "type": "string" } } },
        });
        assert!(validate_refs(&document).is_ok());
    }

    /// The gate must fail *closed*. A `$ref` whose value is not a string is invisible to a
    /// string-only classifier, and `openapiv3` then deserializes the object into a schema carrying
    /// no constraints at all — so the mock answers `null` where the author declared a type, and
    /// nothing anywhere reports it.
    #[test]
    fn a_non_string_ref_is_refused_rather_than_ignored() {
        for value in [
            serde_json::json!(42),
            serde_json::json!(null),
            serde_json::json!({}),
            serde_json::json!([]),
        ] {
            let document = serde_json::json!({ "schema": { "$ref": value } });
            match validate_refs(&document) {
                Err(CompileError::Parse(detail)) => assert!(detail.contains("$ref"), "{detail}"),
                other => panic!("expected Parse for $ref = {value}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_dangling_local_ref_is_refused() {
        let document = serde_json::json!({
            "schema": { "$ref": "#/components/schemas/Typo" },
            "components": { "schemas": { "X": { "type": "string" } } },
        });
        match validate_refs(&document) {
            Err(CompileError::Parse(detail)) => assert!(detail.contains("Typo"), "{detail}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    /// A pointer that resolves but aims at a table this compiler does not read from would reach
    /// `schema::follow`'s `Unconstrained` fallback — i.e. compile to a contract-free response.
    #[test]
    fn a_local_ref_outside_the_resolvable_component_tables_is_refused() {
        let document = serde_json::json!({
            "schema": { "$ref": "#/components/examples/Thing" },
            "components": { "examples": { "Thing": { "value": 1 } } },
        });
        match validate_refs(&document) {
            Err(CompileError::Parse(detail)) => assert!(detail.contains("components"), "{detail}"),
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn pointer_resolution_follows_arrays_and_unescapes_rfc_6901_tokens() {
        let document = serde_json::json!({ "a/b": [{ "c~d": "found" }], "plain": 1 });
        assert_eq!(
            resolve_pointer(&document, "#/a~1b/0/c~0d"),
            Some(&Value::from("found")),
        );
        assert_eq!(resolve_pointer(&document, "#"), Some(&document));
        assert_eq!(resolve_pointer(&document, "#/a~1b/9"), None);
        assert_eq!(resolve_pointer(&document, "#/plain/deeper"), None);
    }

    #[test]
    fn status_keys_outside_the_http_range_are_refused() {
        for bad in ["99", "1000", "abc", "0XX", "6XX"] {
            let document = serde_json::json!({
                "paths": { "/x": { "get": { "responses": { bad: { "description": "d" } } } } }
            });
            match validate_status_keys(&document) {
                Err(CompileError::Parse(detail)) => assert!(detail.contains(bad), "{detail}"),
                other => panic!("expected Parse for status {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn ordinary_status_keys_pass() {
        let document = serde_json::json!({
            "paths": { "/x": { "get": { "responses": {
                "200": { "description": "d" },
                "404": { "description": "d" },
                "5XX": { "description": "d" },
                "default": { "description": "d" },
            } } } }
        });
        assert!(validate_status_keys(&document).is_ok());
    }
}
