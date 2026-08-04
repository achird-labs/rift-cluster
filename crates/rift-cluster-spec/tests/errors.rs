//! Refusal paths (RFC-004 §3.1, §8). Every one of these must fail *loudly and by name* — a spec
//! that is quietly half-understood is worse than one that is rejected, because the mock it produces
//! looks authoritative.

use rift_cluster_spec::{CompileError, CompileOptions, compile};

const OPENAPI31: &[u8] = include_bytes!("fixtures/openapi31.yaml");
const SWAGGER2: &[u8] = include_bytes!("fixtures/swagger2.json");
const EXTERNAL_REF: &[u8] = include_bytes!("fixtures/external-ref.yaml");

fn opts() -> CompileOptions {
    CompileOptions {
        port: Some(4545),
        ..CompileOptions::default()
    }
}

#[test]
fn openapi_31_is_refused_and_the_error_names_the_version_found() {
    match compile(OPENAPI31, &opts()) {
        Err(CompileError::UnsupportedVersion { found }) => {
            assert!(
                found.contains("3.1"),
                "the error must name what it found, got {found:?}"
            );
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn swagger_20_is_refused_and_the_error_names_the_version_found() {
    match compile(SWAGGER2, &opts()) {
        Err(CompileError::UnsupportedVersion { found }) => {
            assert!(
                found.contains("2.0"),
                "the error must name what it found, got {found:?}"
            );
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn a_document_with_no_version_marker_at_all_is_refused() {
    match compile(b"info: { title: t, version: '1' }\npaths: {}\n", &opts()) {
        Err(CompileError::UnsupportedVersion { .. }) => {}
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

/// Refused, never resolved: resolving is a second fetch outside the one-fetch rule, and resolving
/// differently on re-import would make drift reports lie (RFC-004 §3.1). It is also the SSRF
/// mitigation in §8 — the compiler must never be a fetcher.
#[test]
fn an_external_ref_is_refused_by_name_and_never_resolved() {
    match compile(EXTERNAL_REF, &opts()) {
        Err(CompileError::ExternalRef { reference }) => {
            assert_eq!(
                reference,
                "https://example.invalid/schemas/widget.yaml#/Widget"
            );
        }
        other => panic!("expected ExternalRef, got {other:?}"),
    }
}

#[test]
fn a_relative_file_ref_is_an_external_ref_too() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /w:
    get:
      operationId: w
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { $ref: './shared/widget.yaml#/Widget' }
"#;
    match compile(spec, &opts()) {
        Err(CompileError::ExternalRef { reference }) => {
            assert_eq!(reference, "./shared/widget.yaml#/Widget");
        }
        other => panic!("expected ExternalRef, got {other:?}"),
    }
}

#[test]
fn an_internal_ref_is_not_mistaken_for_an_external_one() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /w:
    get:
      operationId: w
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { $ref: '#/components/schemas/W' }
components:
  schemas:
    W: { type: object, properties: { a: { type: string } } }
"#;
    compile(spec, &opts()).expect("a local $ref resolves in-document");
}

#[test]
fn an_oversized_spec_is_refused_before_it_is_parsed() {
    let options = CompileOptions {
        port: Some(4545),
        max_bytes: 64,
        ..CompileOptions::default()
    };
    match compile(OPENAPI31, &options) {
        Err(CompileError::TooLarge { max }) => assert_eq!(max, 64),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[test]
fn the_default_size_cap_is_the_four_mebibyte_pre_commit_cap() {
    assert_eq!(CompileOptions::default().max_bytes, 4 * 1024 * 1024);
}

#[test]
fn a_syntactically_broken_document_is_a_parse_error() {
    match compile(b"openapi: 3.0.3\npaths: [ this is not a map", &opts()) {
        Err(CompileError::Parse(_)) => {}
        other => panic!("expected Parse, got {other:?}"),
    }
}

/// Stub ids must be "collision-free by construction within one spec". Two operations sharing an
/// `operationId` would produce two stubs with one id, which the engine's duplicate-id admission
/// check rejects later and much less legibly.
#[test]
fn duplicate_operation_ids_are_refused() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /a:
    get:
      operationId: same
      responses: { '200': { description: ok } }
  /b:
    get:
      operationId: same
      responses: { '200': { description: ok } }
"#;
    match compile(spec, &opts()) {
        Err(CompileError::Parse(detail)) => {
            assert!(
                detail.contains("same"),
                "the error must name the id: {detail}"
            );
        }
        other => panic!("expected Parse naming the duplicate id, got {other:?}"),
    }
}

/// The self-check of RFC-004 §3.2: the compiler validates its own emitted bodies against the very
/// schemas it emits the contract for, and *fails compilation* rather than deploying a mock that
/// contradicts the spec it claims to implement. Here the spec's own example is the inconsistency.
#[test]
fn an_example_that_contradicts_its_schema_fails_compilation() {
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
                required: [id]
                properties:
                  id: { type: integer }
              example:
                id: 'not-an-integer'
"#;
    match compile(spec, &opts()) {
        Err(CompileError::SelfCheck {
            operation,
            status,
            detail,
        }) => {
            assert_eq!(operation, "u");
            assert_eq!(status, "200");
            assert!(
                detail.contains("/id"),
                "the violation should point at the field: {detail}"
            );
        }
        other => panic!("expected SelfCheck, got {other:?}"),
    }
}

#[test]
fn compile_errors_render_a_message_that_names_the_problem() {
    let err = compile(OPENAPI31, &opts()).expect_err("refused");
    let rendered = err.to_string();
    assert!(rendered.contains("3.1"), "{rendered}");
    assert!(
        rendered.contains("3.0"),
        "the message should say what IS supported: {rendered}"
    );
}

/// A `$ref`'d path item would silently delete a whole route from the compiled mock while `compile`
/// returned `Ok`. OpenAPI 3.0 has no `components.pathItems` to resolve it against, so it is refused.
#[test]
fn a_referenced_path_item_is_refused_rather_than_dropped() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /w:
    $ref: '#/components/schemas/Nope'
components:
  schemas:
    Nope: { type: object }
"#;
    match compile(spec, &opts()) {
        Err(CompileError::Parse(detail)) => assert!(detail.contains("/w"), "{detail}"),
        other => panic!("expected Parse naming the path, got {other:?}"),
    }
}

#[test]
fn a_dangling_local_ref_is_refused_by_name() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /w:
    get:
      operationId: w
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { $ref: '#/components/schemas/Typo' }
components:
  schemas:
    Widget: { type: object }
"#;
    match compile(spec, &opts()) {
        Err(CompileError::Parse(detail)) => assert!(detail.contains("Typo"), "{detail}"),
        other => panic!("expected Parse naming the dangling ref, got {other:?}"),
    }
}

/// `openapiv3` drops a response key it cannot parse into `StatusCode`, so the entry vanishes before
/// the compiler sees it — leaving an operation that answers nothing, reported as a clean compile.
#[test]
fn an_unrecognised_status_key_is_refused() {
    for bad in ["99", "1000", "abc"] {
        let spec = format!(
            "openapi: 3.0.3\ninfo: {{ title: t, version: '1' }}\npaths:\n  /w:\n    get:\n      \
             operationId: w\n      responses:\n        '{bad}': {{ description: ok }}\n"
        );
        match compile(spec.as_bytes(), &opts()) {
            Err(CompileError::Parse(detail)) => assert!(detail.contains(bad), "{detail}"),
            other => panic!("expected Parse for status {bad:?}, got {other:?}"),
        }
    }
}

/// A number schema with bounds near `f64::MAX` overflows the two-decimal scaling to infinity;
/// `Value::from` would turn that into `null` and the self-check would then fail complaining about
/// the wrong thing. A legal spec must compile.
#[test]
fn extreme_numeric_bounds_still_compile_to_a_finite_number() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /n:
    get:
      operationId: n
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema: { type: number, minimum: -1.7e308, maximum: 1.7e308 }
"#;
    let compiled = compile(spec, &opts()).expect("a legal number schema must compile");
    let body = &compiled.imposter["stubs"][0]["responses"][0]["is"]["body"];
    assert!(
        body.as_f64().is_some_and(f64::is_finite),
        "expected a finite number, got {body}",
    );
}

/// An `operationId` containing `:` would break the `spec:<operation_id>:<status>` grammar that
/// drift classification parses.
#[test]
fn an_operation_id_containing_a_colon_is_refused() {
    let spec = br#"
openapi: 3.0.3
info: { title: t, version: '1' }
paths:
  /w:
    get:
      operationId: 'a:b'
      responses:
        '200': { description: ok }
"#;
    match compile(spec, &opts()) {
        Err(CompileError::Parse(detail)) => assert!(detail.contains("a:b"), "{detail}"),
        other => panic!("expected Parse, got {other:?}"),
    }
}
