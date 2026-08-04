//! Behavioural gate for the compilation rules of RFC-004 §3.2.
//!
//! These assertions are deliberately independent of the golden files: goldens prove that output is
//! *stable*, these prove it is *correct*. A golden alone would happily bless a wrong pattern
//! forever, so every rule the RFC names gets an explicit test here.

use rift_cluster_spec::{CompileOptions, StatusKey, compile, validate_stub_response};
use serde_json::Value;

const TEMPLATES: &[u8] = include_bytes!("fixtures/templates.yaml");
const PETSTORE: &[u8] = include_bytes!("fixtures/petstore.yaml");
const RECURSIVE: &[u8] = include_bytes!("fixtures/recursive.yaml");

fn opts() -> CompileOptions {
    CompileOptions {
        port: Some(4545),
        name: Some("spec".to_string()),
        ..CompileOptions::default()
    }
}

fn stubs(imposter: &Value) -> &Vec<Value> {
    imposter["stubs"].as_array().expect("stubs array")
}

/// The stub carrying `spec:<operation_id>:<status>`.
fn stub_by_id<'a>(imposter: &'a Value, id: &str) -> &'a Value {
    stubs(imposter)
        .iter()
        .find(|s| s["id"].as_str() == Some(id))
        .unwrap_or_else(|| panic!("no stub with id {id}; ids: {:?}", stub_ids(imposter)))
}

fn stub_ids(imposter: &Value) -> Vec<String> {
    stubs(imposter)
        .iter()
        .map(|s| s["id"].as_str().unwrap_or("<none>").to_string())
        .collect()
}

/// Index of a stub id within the emitted (order-significant) stub list.
fn position(imposter: &Value, id: &str) -> usize {
    stub_ids(imposter)
        .iter()
        .position(|s| s == id)
        .unwrap_or_else(|| panic!("no stub with id {id}"))
}

/// The predicates of a stub, as raw JSON objects.
fn predicates<'a>(imposter: &'a Value, id: &str) -> &'a Vec<Value> {
    stub_by_id(imposter, id)["predicates"]
        .as_array()
        .expect("predicates array")
}

fn has_predicate(imposter: &Value, id: &str, expected: &Value) -> bool {
    predicates(imposter, id).iter().any(|p| p == expected)
}

// ---------------------------------------------------------------------------
// AC1 — public surface
// ---------------------------------------------------------------------------

#[test]
fn compiled_spec_exposes_imposter_operations_and_digest() {
    let compiled = compile(TEMPLATES, &opts()).expect("templates.yaml compiles");

    assert_eq!(compiled.imposter["port"], Value::from(4545));
    assert_eq!(compiled.imposter["protocol"], Value::from("http"));
    assert!(!compiled.operations.is_empty());
    assert_eq!(
        compiled.digest.to_hex().len(),
        64,
        "sha256 renders as 64 hex"
    );

    let op = compiled
        .operation("getUser")
        .expect("getUser is in the operation index");
    assert_eq!(op.method, "GET");
    assert_eq!(op.path_template, "/users/{id}");
    assert!(op.stub_ids.contains(&"spec:getUser:default".to_string()));
}

#[test]
fn digest_is_sha256_of_the_input_bytes_and_changes_with_them() {
    let a = compile(TEMPLATES, &opts()).expect("compiles");
    let b = compile(PETSTORE, &opts()).expect("compiles");
    assert_ne!(a.digest, b.digest);

    let again = compile(TEMPLATES, &opts()).expect("compiles");
    assert_eq!(a.digest, again.digest, "same bytes ⇒ same digest");
}

// ---------------------------------------------------------------------------
// AC3 — path templates
// ---------------------------------------------------------------------------

#[test]
fn path_template_compiles_to_an_anchored_regex_and_a_route_pattern() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");

    assert!(
        has_predicate(
            &c.imposter,
            "spec:getUser:default",
            &serde_json::json!({ "matches": { "path": "^/users/[^/]+$" }, "caseSensitive": true }),
        ),
        "predicates were {:#?}",
        predicates(&c.imposter, "spec:getUser:default"),
    );

    assert_eq!(
        stub_by_id(&c.imposter, "spec:getUser:default")["routePattern"],
        Value::from("/users/:id"),
        "routePattern is what makes request.pathParams.id work for free",
    );
}

#[test]
fn multiple_template_segments_each_become_their_own_route_param() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let id = "spec:getPost:200";

    assert!(has_predicate(
        &c.imposter,
        id,
        &serde_json::json!({ "matches": { "path": "^/users/[^/]+/posts/[^/]+$" }, "caseSensitive": true }),
    ));
    assert_eq!(
        stub_by_id(&c.imposter, id)["routePattern"],
        Value::from("/users/:id/posts/:postId"),
    );
}

/// RFC-004 §8 threat item 2: a literal segment carrying regex metacharacters must be escaped, not
/// passed through — `/a(b/{id}` is an invalid regex if emitted raw and a widened one if the paren
/// is merely dropped.
#[test]
fn regex_metacharacters_in_literal_segments_are_escaped() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let pattern = predicates(&c.imposter, "spec:metacharLiteral:200")
        .iter()
        .find_map(|p| {
            p.get("matches")
                .and_then(|m| m.get("path"))
                .and_then(Value::as_str)
        })
        .expect("a matches/path predicate");

    assert_eq!(pattern, r"^/a\(b/[^/]+$");
    regex::Regex::new(pattern).expect("the emitted pattern is a valid regex");
}

#[test]
fn a_literal_only_path_compiles_without_a_route_pattern() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let stub = stub_by_id(&c.imposter, "spec:getSelf:200");

    assert!(has_predicate(
        &c.imposter,
        "spec:getSelf:200",
        &serde_json::json!({ "matches": { "path": "^/users/me$" }, "caseSensitive": true }),
    ));
    assert!(
        stub.get("routePattern").is_none(),
        "a path with no template segments has no params to extract",
    );
}

// ---------------------------------------------------------------------------
// AC4 — method / parameter predicates
// ---------------------------------------------------------------------------

#[test]
fn method_compiles_to_an_equals_predicate() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    assert!(has_predicate(
        &c.imposter,
        "spec:deleteUser:204",
        &serde_json::json!({ "equals": { "method": "DELETE" } }),
    ));
}

#[test]
fn required_query_and_header_parameters_compile_to_exists_predicates() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let id = "spec:getUser:default";

    assert!(
        has_predicate(
            &c.imposter,
            id,
            &serde_json::json!({ "exists": { "query": { "fields": true } } }),
        ),
        "required query param `fields`; predicates were {:#?}",
        predicates(&c.imposter, id),
    );
    assert!(has_predicate(
        &c.imposter,
        id,
        &serde_json::json!({ "exists": { "headers": { "X-Tenant": true } } }),
    ));
}

/// "The mock must accept what the spec permits, not only what it illustrates" — an optional
/// parameter that compiled to a predicate would make the mock *reject* a legal request.
#[test]
fn optional_parameters_compile_to_no_predicate_at_all() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let rendered = serde_json::to_string(predicates(&c.imposter, "spec:getUser:default"))
        .expect("predicates serialize");

    assert!(
        !rendered.contains("expand"),
        "optional query param leaked: {rendered}"
    );
    assert!(
        !rendered.contains("X-Trace"),
        "optional header leaked: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// AC5 — ordering
// ---------------------------------------------------------------------------

/// Mountebank is first-match-wins in stub order, so `/users/me` must precede `/users/{id}` or the
/// literal route is unreachable.
#[test]
fn literal_paths_precede_templated_paths() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    assert!(
        position(&c.imposter, "spec:getSelf:200") < position(&c.imposter, "spec:getUser:default"),
        "order was {:?}",
        stub_ids(&c.imposter),
    );
}

#[test]
fn more_specific_templates_precede_less_specific_ones() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    assert!(
        position(&c.imposter, "spec:getPost:200") < position(&c.imposter, "spec:getUser:default"),
        "/users/{{id}}/posts/{{postId}} must not be shadowed by /users/{{id}}; order was {:?}",
        stub_ids(&c.imposter),
    );
}

// ---------------------------------------------------------------------------
// AC6 — response synthesis
// ---------------------------------------------------------------------------

#[test]
fn a_spec_example_is_used_verbatim() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:getSelf:200")["responses"][0]["is"]["body"];

    assert_eq!(
        body,
        &serde_json::json!({ "id": "self", "name": "Ada", "role": "admin" }),
        "the spec's own example wins over synthesis",
    );
}

#[test]
fn synthesis_is_stable_across_recompiles_of_the_same_bytes() {
    let a = compile(PETSTORE, &opts()).expect("compiles");
    let b = compile(PETSTORE, &opts()).expect("compiles");
    assert_eq!(
        a.imposter, b.imposter,
        "same spec bytes ⇒ same generated bodies"
    );
}

#[test]
fn arrays_render_their_min_items() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    // Pets is `minItems: 2`.
    let body = &stub_by_id(&c.imposter, "spec:listPets:200")["responses"][0]["is"]["body"];
    assert_eq!(body.as_array().expect("an array body").len(), 2);

    let c2 = compile(TEMPLATES, &opts()).expect("compiles");
    // User.tags is `minItems: 3`; the 404 body has no example so it is synthesized.
    let user = &stub_by_id(&c2.imposter, "spec:getUser:200")["responses"][0]["is"]["body"];
    assert_eq!(user["tags"].as_array().expect("tags array").len(), 3);
}

#[test]
fn an_array_without_min_items_renders_exactly_one_element() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /xs:
    get:
      operationId: xs
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { type: array, items: { type: string } }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:xs:200")["responses"][0]["is"]["body"];
    assert_eq!(body.as_array().expect("array").len(), 1);
}

#[test]
fn additional_properties_render_nothing() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:getBag:200")["responses"][0]["is"]["body"];
    assert_eq!(
        body,
        &serde_json::json!({}),
        "an object whose only shape is additionalProperties renders empty",
    );
}

#[test]
fn enum_values_are_drawn_from_the_declared_set() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:getUser:200")["responses"][0]["is"]["body"];
    let role = body["role"].as_str().expect("role is a string");
    assert!(
        ["admin", "member", "guest"].contains(&role),
        "synthesised enum value {role:?} is not in the declared set",
    );
}

#[test]
fn required_properties_are_always_present_in_a_synthesised_body() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let pet = &stub_by_id(&c.imposter, "spec:showPetById:200")["responses"][0]["is"]["body"];
    assert!(pet["id"].is_i64(), "required id present and typed: {pet}");
    assert!(
        pet["name"].is_string(),
        "required name present and typed: {pet}"
    );
}

/// RFC-004 §8 threat item 1: recursion caps at depth 8 with `null` at the floor rather than
/// recursing until the compiler OOMs.
#[test]
fn a_recursive_schema_compiles_bounded_with_null_at_the_floor() {
    let c = compile(RECURSIVE, &opts()).expect("a self-referential schema still compiles");
    let body = &stub_by_id(&c.imposter, "spec:getTree:200")["responses"][0]["is"]["body"];

    // Walk `child` down; it must terminate in a null rather than nest forever.
    let mut node = body;
    let mut depth = 0usize;
    while node.get("child").is_some_and(|c| !c.is_null()) {
        node = &node["child"];
        depth += 1;
        assert!(depth <= 8, "recursion exceeded the depth cap");
    }
    assert!(
        depth >= 1,
        "the recursion should render at least one level: {body}"
    );
    assert!(node["child"].is_null(), "the floor renders null: {node}");
}

// ---------------------------------------------------------------------------
// AC7 / AC8 — one stub per status, the unconditional one last, deterministic ids
// ---------------------------------------------------------------------------

/// One stub per declared status, with the unconditional one **last**.
///
/// Mountebank is first-match-wins, and the unconditional stub's predicates are a strict subset of
/// every other stub's for this operation — so emitting it first would make every discriminated
/// stub unreachable.
///
/// The expected list changed with issue #314: the unconditional answer is now the first declared
/// **2xx** (`200`) rather than `default`, so `200` is the one that sorts last and `default` joins
/// the discriminated stubs in document order.
#[test]
fn one_stub_per_declared_status_with_the_unconditional_one_last() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let ids = stub_ids(&c.imposter);
    let get_user: Vec<&String> = ids
        .iter()
        .filter(|i| i.starts_with("spec:getUser:"))
        .collect();

    assert_eq!(
        get_user,
        vec![
            "spec:getUser:404",
            "spec:getUser:default",
            "spec:getUser:200"
        ],
    );
}

/// The property that ordering exists to provide, asserted the way the engine actually behaves:
/// first-match-wins over the emitted stub array. A "the discriminator predicate is present"
/// assertion cannot make this check — a predicate on an unreachable stub is decoration.
#[test]
fn every_declared_status_is_reachable_by_its_discriminator_header() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let required = [("X-Tenant", "t")];
    let query = [("fields", "a")];

    // `default` joins the discriminated statuses (issue #314): it names no status of its own, so
    // it is no longer privileged as the unconditional answer, but it must stay reachable.
    for status in ["200", "404", "default"] {
        let headers = [("X-Tenant", "t"), ("X-Rift-Spec-Status", status)];
        let selected = first_match(&c.imposter, "GET", "/users/abc", &headers, &query);
        assert_eq!(
            selected.as_deref(),
            Some(format!("spec:getUser:{status}").as_str()),
            "opting into {status} selected {selected:?}",
        );
    }

    // With no discriminator, the catch-all answers — and it must be the one that is last.
    //
    // This assertion is deliberately inverted by issue #314. It previously required
    // `spec:getUser:default`, which *was* the defect: `default` declares no status, so it compiled
    // to `statusCode: 200` carrying the error body, and a bare request got a success status
    // wrapping an error. The unconditional answer is now the first declared 2xx.
    let selected = first_match(&c.imposter, "GET", "/users/abc", &required, &query);
    assert_eq!(selected.as_deref(), Some("spec:getUser:200"));
}

/// The declared success is reachable by its discriminator even when the spec also declares a
/// `default` error. That a bare request *gets* the success is the separate, stronger claim of
/// `a_bare_request_gets_the_declared_success_not_the_default_error`.
#[test]
fn a_declared_success_is_reachable_even_when_the_spec_has_a_default_error() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let headers = [("X-Request-Id", "r"), ("X-Rift-Spec-Status", "200")];

    let selected = first_match(&c.imposter, "GET", "/pets", &headers, &[]);
    assert_eq!(selected.as_deref(), Some("spec:listPets:200"));

    let body = &stub_by_id(&c.imposter, "spec:listPets:200")["responses"][0]["is"]["body"];
    assert!(
        body.is_array(),
        "the 200 serves the pet array, not the error: {body}"
    );
}

/// A minimal first-match-wins evaluator over the predicate subset this compiler emits: `equals` on
/// method and headers, `matches` on path, `exists` on query and headers.
fn first_match(
    imposter: &Value,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    query: &[(&str, &str)],
) -> Option<String> {
    let matches_predicate = |p: &Value| -> bool {
        if let Some(eq) = p.get("equals") {
            if let Some(m) = eq.get("method").and_then(Value::as_str) {
                return m == method;
            }
            if let Some(hs) = eq.get("headers").and_then(Value::as_object) {
                return hs.iter().all(|(k, v)| {
                    headers
                        .iter()
                        .any(|(hk, hv)| hk.eq_ignore_ascii_case(k) && Some(*hv) == v.as_str())
                });
            }
        }
        if let Some(rx) = p
            .get("matches")
            .and_then(|m| m.get("path"))
            .and_then(Value::as_str)
        {
            return regex::Regex::new(rx).is_ok_and(|re| re.is_match(path));
        }
        if let Some(ex) = p.get("exists") {
            if let Some(qs) = ex.get("query").and_then(Value::as_object) {
                return qs.keys().all(|k| query.iter().any(|(qk, _)| qk == k));
            }
            if let Some(hs) = ex.get("headers").and_then(Value::as_object) {
                return hs
                    .keys()
                    .all(|k| headers.iter().any(|(hk, _)| hk.eq_ignore_ascii_case(k)));
            }
        }
        true
    };

    stubs(imposter)
        .iter()
        .find(|stub| {
            stub["predicates"]
                .as_array()
                .is_some_and(|preds| preds.iter().all(&matches_predicate))
        })
        .and_then(|stub| stub["id"].as_str().map(str::to_string))
}

/// Every status but the unconditional one is opt-in via the discriminator, so a test can reach any
/// declared response without editing the imposter.
///
/// Renamed and inverted by issue #314. `default` used to be the unconditional stub and therefore
/// carried no discriminator; it is now gated like any other declared response, and the first
/// declared 2xx is what answers a bare request.
#[test]
fn every_status_but_the_unconditional_one_is_gated_by_the_discriminator_header() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");

    for status in ["404", "default"] {
        let id = format!("spec:getUser:{status}");
        assert!(
            has_predicate(
                &c.imposter,
                &id,
                &serde_json::json!({ "equals": { "headers": { "X-Rift-Spec-Status": status } } }),
            ),
            "{status} must be opt-in; predicates were {:#?}",
            predicates(&c.imposter, &id),
        );
    }

    assert!(
        !predicates(&c.imposter, "spec:getUser:200")
            .iter()
            .any(|p| serde_json::to_string(p)
                .unwrap_or_default()
                .contains("X-Rift-Spec-Status")),
        "the first declared 2xx must answer unconditionally",
    );
}

/// A spec that declares exactly one status and no `default` still gets that status served
/// unconditionally — otherwise the imposter would answer nothing at all without a magic header.
#[test]
fn a_lone_status_answers_unconditionally() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    assert!(
        !predicates(&c.imposter, "spec:getSelf:200")
            .iter()
            .any(|p| serde_json::to_string(p)
                .unwrap_or_default()
                .contains("X-Rift-Spec-Status")),
        "getSelf declares only 200, so it must answer without the discriminator",
    );
    assert_eq!(
        stub_by_id(&c.imposter, "spec:getSelf:200")["responses"][0]["is"]["statusCode"],
        Value::from(200),
    );
}

#[test]
fn stub_ids_are_deterministic_and_carry_the_spec_prefix() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    for id in stub_ids(&c.imposter) {
        assert!(
            id.starts_with("spec:"),
            "{id} lacks the drift-classifier prefix"
        );
    }
    assert!(stub_ids(&c.imposter).contains(&"spec:showPetById:404".to_string()));

    let op = c.operation("showPetById").expect("indexed");
    assert_eq!(
        op.stub_ids,
        vec!["spec:showPetById:404", "spec:showPetById:200"],
        "emitted order: discriminated first, the unconditional 200 last",
    );
}

#[test]
fn an_operation_without_an_operation_id_gets_a_synthesised_one() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /widgets/{id}:
    get:
      responses:
        '200': { description: ok }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let op = &c.operations[0];
    assert_eq!(op.method, "GET");
    assert_eq!(op.path_template, "/widgets/{id}");
    assert!(
        !op.id.as_str().is_empty(),
        "an operation with no operationId still needs a stable identity",
    );
    assert_eq!(op.stub_ids, vec![format!("spec:{}:200", op.id.as_str())]);
    assert!(
        op.id.as_str().starts_with("get_widgets_id_"),
        "readable, with a suffix that keeps distinct routes from colliding: {}",
        op.id.as_str(),
    );
}

// ---------------------------------------------------------------------------
// AC6 — the `is` response shape
// ---------------------------------------------------------------------------

#[test]
fn responses_carry_the_media_type_as_content_type_and_a_numeric_status() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let is = &stub_by_id(&c.imposter, "spec:showPetById:404")["responses"][0]["is"];

    assert_eq!(
        is["statusCode"],
        Value::from(404),
        "IsResponse.status_code is a u16"
    );
    assert_eq!(
        is["headers"]["Content-Type"],
        Value::from("application/json")
    );
    assert!(is["body"].is_object());
}

#[test]
fn a_status_with_no_content_gets_no_body_and_no_content_type() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let is = &stub_by_id(&c.imposter, "spec:createPets:201")["responses"][0]["is"];

    assert_eq!(is["statusCode"], Value::from(201));
    assert!(
        is.get("body").is_none(),
        "a bodyless response must not invent one: {is}"
    );
    assert!(is.get("headers").is_none() || is["headers"].get("Content-Type").is_none());
}

// ---------------------------------------------------------------------------
// AC9 — validate_stub_response
// ---------------------------------------------------------------------------

#[test]
fn validate_stub_response_accepts_the_compilers_own_output() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let op = c.operation("showPetById").expect("indexed");
    let body = &stub_by_id(&c.imposter, "spec:showPetById:200")["responses"][0]["is"]["body"];

    assert_eq!(
        validate_stub_response(op, &StatusKey::Code(200), body),
        vec![],
        "the self-check that gates compilation must pass on what compilation produced",
    );
}

#[test]
fn validate_stub_response_flags_a_missing_required_property() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let op = c.operation("showPetById").expect("indexed");

    let violations = validate_stub_response(
        op,
        &StatusKey::Code(200),
        &serde_json::json!({ "id": 1 }), // `name` is required
    );
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert!(violations[0].detail.contains("name"), "{:?}", violations[0]);
}

#[test]
fn validate_stub_response_flags_a_type_mismatch() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let op = c.operation("showPetById").expect("indexed");

    let violations = validate_stub_response(
        op,
        &StatusKey::Code(200),
        &serde_json::json!({ "id": "not-an-integer", "name": "Rex" }),
    );
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert_eq!(violations[0].pointer, "/id");
}

#[test]
fn validate_stub_response_flags_a_value_outside_a_declared_enum() {
    let c = compile(TEMPLATES, &opts()).expect("compiles");
    let op = c.operation("getUser").expect("indexed");

    let violations = validate_stub_response(
        op,
        &StatusKey::Code(200),
        &serde_json::json!({ "id": "u1", "name": "Ada", "role": "overlord" }),
    );
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert_eq!(violations[0].pointer, "/role");
}

#[test]
fn validate_stub_response_reports_a_status_the_operation_never_declares() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let op = c.operation("showPetById").expect("indexed");

    let violations = validate_stub_response(op, &StatusKey::Code(418), &serde_json::json!({}));
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert!(violations[0].detail.contains("418"), "{:?}", violations[0]);
}

/// A response with no declared schema constrains nothing — validating against it must not invent
/// violations, or every schemaless operation would be unusable.
#[test]
fn validate_stub_response_is_silent_when_the_response_declares_no_schema() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let op = c.operation("createPets").expect("indexed");

    assert_eq!(
        validate_stub_response(
            op,
            &StatusKey::Code(201),
            &serde_json::json!({ "anything": 1 })
        ),
        vec![],
    );
}

// ---------------------------------------------------------------------------
// Composed schemas, ranges, formats, and component references
// ---------------------------------------------------------------------------

/// `allOf` merges every object branch; a later branch redefining a property wins.
#[test]
fn all_of_merges_the_branches_into_one_object() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                allOf:
                  - type: object
                    required: [id]
                    properties: { id: { type: string } }
                  - type: object
                    required: [n]
                    properties: { n: { type: integer } }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:u:200")["responses"][0]["is"]["body"];
    assert!(body["id"].is_string(), "{body}");
    assert!(body["n"].is_i64(), "{body}");
}

/// A choice takes the first branch, always — picking by seed would make the chosen branch an
/// invisible input to every re-import diff.
#[test]
fn one_of_deterministically_takes_the_first_branch() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                oneOf:
                  - type: string
                  - type: integer
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:u:200")["responses"][0]["is"]["body"];
    assert!(body.is_string(), "first branch is `string`, got {body}");
}

/// `2XX` range keys survive parse, stub-id generation and the discriminator end to end — the
/// `StatusKey::Range` unit test alone never exercises the deserialization path.
#[test]
fn a_status_range_compiles_end_to_end() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '200': { description: ok }
        '4XX': { description: client error }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    assert_eq!(stub_ids(&c.imposter), vec!["spec:u:4XX", "spec:u:200"]);
    assert!(has_predicate(
        &c.imposter,
        "spec:u:4XX",
        &serde_json::json!({ "equals": { "headers": { "X-Rift-Spec-Status": "4XX" } } }),
    ));
    assert_eq!(
        stub_by_id(&c.imposter, "spec:u:4XX")["responses"][0]["is"]["statusCode"],
        Value::from(400),
    );
}

/// The `format:` → generator mapping only runs through `schema::normalize`; the synth unit tests
/// build `TextFormat` by hand and would not notice it regressing.
#[test]
fn declared_string_formats_reach_the_generator() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  at: { type: string, format: date-time }
                  on: { type: string, format: date }
                  who: { type: string, format: uuid }
                  mail: { type: string, format: email }
                  plain: { type: string }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:u:200")["responses"][0]["is"]["body"];

    assert_eq!(body["at"], Value::from("2024-01-01T00:00:00Z"));
    assert_eq!(body["on"], Value::from("2024-01-01"));
    let uuid = body["who"].as_str().expect("uuid string");
    assert_eq!(uuid.len(), 36, "{uuid}");
    assert_eq!(uuid.matches('-').count(), 4, "{uuid}");
    assert!(
        body["mail"].as_str().is_some_and(|m| m.contains('@')),
        "{body}"
    );
    assert!(
        body["plain"]
            .as_str()
            .is_some_and(|p| p.starts_with("string-")),
        "{body}"
    );
}

/// A `$ref`'d response is resolved, not dropped. Skipping it would leave the status with no stub
/// while `compile` still reported success.
#[test]
fn a_referenced_response_is_resolved() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '200': { description: ok }
        '404': { $ref: '#/components/responses/NotFound' }
components:
  responses:
    NotFound:
      description: absent
      content:
        application/json:
          schema:
            type: object
            required: [code]
            properties: { code: { type: integer } }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    assert!(stub_ids(&c.imposter).contains(&"spec:u:404".to_string()));
    let is = &stub_by_id(&c.imposter, "spec:u:404")["responses"][0]["is"];
    assert_eq!(is["statusCode"], Value::from(404));
    assert!(is["body"]["code"].is_i64(), "{is}");
}

/// A `$ref`'d *required* parameter must still compile to its `exists` predicate. Dropping it makes
/// the mock strictly more permissive than the contract it claims to implement.
#[test]
fn a_referenced_required_parameter_still_compiles_to_a_predicate() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      parameters:
        - $ref: '#/components/parameters/ApiKey'
      responses:
        '200': { description: ok }
components:
  parameters:
    ApiKey:
      name: X-Api-Key
      in: header
      required: true
      schema: { type: string }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    assert!(
        has_predicate(
            &c.imposter,
            "spec:u:200",
            &serde_json::json!({ "exists": { "headers": { "X-Api-Key": true } } }),
        ),
        "predicates were {:#?}",
        predicates(&c.imposter, "spec:u:200"),
    );
}

#[test]
fn an_explicit_min_items_of_zero_renders_an_empty_array() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /xs:
    get:
      operationId: xs
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { type: array, minItems: 0, items: { type: string } }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    let body = &stub_by_id(&c.imposter, "spec:xs:200")["responses"][0]["is"]["body"];
    assert_eq!(body, &serde_json::json!([]));
}

#[test]
fn a_spec_with_no_paths_compiles_to_an_imposter_with_no_stubs() {
    let spec = b"openapi: 3.0.3\ninfo: { title: t, version: '1' }\npaths: {}\n";
    let c = compile(spec, &opts()).expect("an empty but valid spec is not an error");
    assert_eq!(c.imposter["stubs"], serde_json::json!([]));
    assert!(c.operations.is_empty());
}

/// Response order is the spec's, not the alphabet's: the discriminated stubs are emitted in
/// document order, so parsing through a key-sorted map would reorder them.
///
/// The fixture declares **two** non-2xx statuses in non-alphabetical order on purpose. Issue #314
/// made the unconditional answer the first declared 2xx rather than the first declared response,
/// which cost the original two-response fixture (`'404'` then `'200'`) its power to detect
/// alphabetisation: under the new rule `200` is the unconditional either way, so document and
/// alphabetical parses emit an identical list. `'500'` before `'404'` restores the signal —
/// alphabetically `404` sorts first, so a key-sorted parse would swap them.
#[test]
fn response_order_follows_the_document_not_the_alphabet() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /u:
    get:
      operationId: u
      responses:
        '500': { description: declared first }
        '404': { description: declared second }
        '200': { description: the success }
"#;
    let c = compile(spec, &opts()).expect("compiles");
    assert_eq!(
        stub_ids(&c.imposter),
        vec!["spec:u:500", "spec:u:404", "spec:u:200"],
        "500 is declared before 404, and the 2xx answers unconditionally so it sorts last",
    );
}

// ---------------------------------------------------------------------------
// Issue #314: which response answers unconditionally.
//
// RFC-004 §3.2 originally said "default response first", and the compiler
// implemented it faithfully. But `default` names no HTTP status, so
// `StatusKey::http_status` serves it as 200 — meaning a spec whose `default` is
// an error compiled to a mock that answered a bare request with **200 carrying
// an error body**. A success status wrapping an error is the shape client code
// fails to notice, which is why this is a defect and not merely surprising UX.
// ---------------------------------------------------------------------------

/// The headline fix: a bare request gets the declared success, not the default
/// error. Asserted on the response *body and status* rather than only the stub
/// id, because the defect was precisely that the id said `default` while the
/// status said 200 — an id-only assertion would not have shown the harm.
#[test]
fn a_bare_request_gets_the_declared_success_not_the_default_error() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let required = [("X-Request-Id", "r")];

    let selected = first_match(&c.imposter, "GET", "/pets", &required, &[]);
    assert_eq!(
        selected.as_deref(),
        Some("spec:listPets:200"),
        "a request carrying no discriminator must get the declared 2xx"
    );

    let answer = &stub_by_id(&c.imposter, "spec:listPets:200")["responses"][0]["is"];
    assert_eq!(answer["statusCode"], 200);
    assert!(
        answer["body"].is_array(),
        "the unconditional answer must be the pet array, not the error object: {}",
        answer["body"]
    );
}

/// `default` stays reachable — it is discriminated now, not dropped.
#[test]
fn the_default_response_is_reachable_by_its_own_discriminator() {
    let c = compile(PETSTORE, &opts()).expect("compiles");
    let headers = [("X-Request-Id", "r"), ("X-Rift-Spec-Status", "default")];

    let selected = first_match(&c.imposter, "GET", "/pets", &headers, &[]);
    assert_eq!(selected.as_deref(), Some("spec:listPets:default"));
}

/// "First declared 2xx" means exactly that — not "200". `createPets` declares
/// only `201` alongside its `default`, so the 201 is the unconditional answer.
#[test]
fn the_unconditional_answer_is_the_first_2xx_even_when_it_is_not_200() {
    let c = compile(PETSTORE, &opts()).expect("compiles");

    let selected = first_match(&c.imposter, "POST", "/pets", &[], &[]);
    assert_eq!(selected.as_deref(), Some("spec:createPets:201"));
    assert_eq!(
        stub_by_id(&c.imposter, "spec:createPets:201")["responses"][0]["is"]["statusCode"],
        201
    );
}

/// No 2xx anywhere: the first *declared* status answers. A declared 404 with its
/// own body is more honest than compiling `default` to a fabricated 200, which is
/// why the fallback is spec order rather than `default`.
#[test]
fn an_operation_with_no_2xx_falls_back_to_its_first_declared_status() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /x:
    get:
      operationId: errorsOnly
      responses:
        '404': { description: declared first }
        '500': { description: declared second }
"#;
    let c = compile(spec, &opts()).expect("compiles");

    let selected = first_match(&c.imposter, "GET", "/x", &[], &[]);
    assert_eq!(selected.as_deref(), Some("spec:errorsOnly:404"));
    assert_eq!(
        stub_by_id(&c.imposter, "spec:errorsOnly:404")["responses"][0]["is"]["statusCode"],
        404
    );
}

/// A `default`-only operation still answers unconditionally — now by falling
/// through the preference rule rather than by being special-cased.
#[test]
fn a_default_only_operation_still_answers_unconditionally() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /x:
    get:
      operationId: defaultOnly
      responses:
        default: { description: the only response }
"#;
    let c = compile(spec, &opts()).expect("compiles");

    let selected = first_match(&c.imposter, "GET", "/x", &[], &[]);
    assert_eq!(selected.as_deref(), Some("spec:defaultOnly:default"));
}

/// A `2XX` range counts as a success for the preference rule — the RFC now
/// states this, so it is pinned rather than left to `is_success`'s reading of
/// `openapiv3`'s leading-digit representation.
#[test]
fn a_2xx_range_counts_as_the_declared_success() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /x:
    get:
      operationId: ranged
      responses:
        '404': { description: declared first }
        '2XX': { description: a range, not a code }
"#;
    let c = compile(spec, &opts()).expect("compiles");

    let selected = first_match(&c.imposter, "GET", "/x", &[], &[]);
    assert_eq!(selected.as_deref(), Some("spec:ranged:2XX"));
}

/// Determinism is a shipped guarantee (§3.5, re-compiles must be diffable), and
/// the reordering this issue introduces is the obvious place to break it.
#[test]
fn reordering_the_unconditional_response_stays_deterministic() {
    let first = compile(PETSTORE, &opts()).expect("compiles");
    let second = compile(PETSTORE, &opts()).expect("compiles");
    assert_eq!(stub_ids(&first.imposter), stub_ids(&second.imposter));
    assert_eq!(first.imposter, second.imposter);
}
