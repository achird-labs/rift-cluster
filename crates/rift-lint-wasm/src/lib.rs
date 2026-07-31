//! `rift-lint` in the browser (RFC-006 §12 Q1, issue #188).
//!
//! One export, and a deliberately dumb boundary: JSON text in, JSON text out. Passing structured
//! values across the wasm ABI would mean `serde-wasm-bindgen` and a second serialization format to
//! keep in step with `LintIssue`; a JSON string is the format the console already parses, and it
//! costs one `serde_json::to_string`.
//!
//! **Advisory only.** The server validates every save and its refusal is the authority; this exists
//! so an operator sees an obvious mistake before sending it. Nothing here can approve a stub — the
//! console's write path treats a clean result and an unavailable linter identically.

use rift_lint::{LintOptions, LintResult, validate_stub};
use std::path::Path;
use wasm_bindgen::prelude::wasm_bindgen;

/// Lint one stub, returning its findings as a JSON array string.
///
/// The array is `LintIssue`'s own serialization, which is what `web/src/features/stubs/lint.ts`
/// decodes. Unparseable input is itself a finding rather than an error return: "this is not JSON"
/// is exactly the thing an operator wants the pane to tell them, and modelling it as a failure of
/// the linter would send it down the "unavailable" path, where it would be silent.
#[wasm_bindgen]
#[must_use]
pub fn lint_stub(json: &str) -> String {
    lint_stub_json(json)
}

/// The whole linting contract, minus the wasm ABI — so the host test suite (and CI's PR lane) can
/// exercise it without a wasm runtime. `lint_stub` above is a pure delegation; a behavior change
/// here IS the export's behavior change.
#[must_use]
fn lint_stub_json(json: &str) -> String {
    let file = Path::new("stub");
    let mut result = LintResult::new();

    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(stub) => {
            /*
             * `registry` is `validate_stub`'s view of the sibling stubs a scenario reference could
             * resolve against. The console edits one stub at a time and does not have that context
             * here, so an empty object is passed: it makes scenario cross-references unverifiable,
             * not wrongly reported, and the server still checks them on save.
             */
            let registry = serde_json::Value::Object(serde_json::Map::new());
            validate_stub(file, &stub, 0, &mut result, &LintOptions::default(), &registry);
        }
        Err(error) => {
            result.add_issue(rift_lint::LintIssue::error(
                "E002",
                format!("Invalid JSON: {error}"),
                file.to_path_buf(),
            ));
        }
    }

    // Infallible by construction: `LintIssue` is a plain struct of strings and an enum, all with
    // derived `Serialize`, so there is no map with non-string keys and no float that could be NaN.
    // The fallback is a literal empty array rather than a default built by the same serializer.
    serde_json::to_string(&result.issues).unwrap_or_else(|_| "[]".to_owned())
}

#[cfg(test)]
mod tests {
    use super::lint_stub_json;

    #[test]
    fn findings_come_back_as_a_json_array() {
        let out = lint_stub_json(r#"{"responses":[{"is":{"statusCode":200}}]}"#);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("findings parse");
        assert!(parsed.is_array(), "the pane decodes an array, got: {out}");
    }

    #[test]
    fn unparseable_input_is_a_finding_not_a_failure() {
        let out = lint_stub_json("{not json");
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("findings parse");
        let first = &parsed[0];
        assert_eq!(first["code"], "E002", "{out}");
        assert!(
            first["message"].as_str().unwrap_or_default().contains("Invalid JSON"),
            "the operator is told WHAT is wrong, not sent to the unavailable path: {out}"
        );
    }

    #[test]
    fn a_real_lint_problem_is_reported() {
        // A stub with no responses is the canonical lint catch; if this stops producing findings,
        // the pane has gone quietly blind and the bundled artifact is dead weight.
        let out = lint_stub_json(r#"{"predicates":[{"equals":{"path":"/x"}}]}"#);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("findings parse");
        assert!(
            !parsed.as_array().map(Vec::is_empty).unwrap_or(true),
            "expected at least one finding for a response-less stub: {out}"
        );
    }
}
