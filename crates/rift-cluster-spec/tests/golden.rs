//! Golden files: the same spec bytes must produce byte-identical imposter JSON, run after run and
//! node after node. These prove *stability*; `compile.rs` proves *correctness*. Keeping the two
//! apart is deliberate — a golden on its own would bless a wrong pattern forever.
//!
//! Regenerate deliberately, never reflexively, after reading the diff:
//!
//!     UPDATE_GOLDEN=1 cargo test -p rift-cluster-spec --test golden

use rift_cluster_spec::{CompileOptions, compile, to_canonical_string};

const PETSTORE: &[u8] = include_bytes!("fixtures/petstore.yaml");
const TEMPLATES: &[u8] = include_bytes!("fixtures/templates.yaml");

fn opts() -> CompileOptions {
    CompileOptions {
        port: Some(4545),
        name: Some("spec".to_string()),
        ..CompileOptions::default()
    }
}

fn check_golden(name: &str, spec: &[u8]) {
    let compiled = compile(spec, &opts()).expect("fixture compiles");
    let actual = to_canonical_string(&compiled.imposter);

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("golden dir")).expect("create golden dir");
        std::fs::write(&path, &actual).expect("write golden");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing golden {}: {e}. Regenerate with UPDATE_GOLDEN=1.",
            path.display()
        )
    });

    assert_eq!(
        actual,
        expected,
        "compiled output drifted from {}. If the change is intended, review the diff then \
         regenerate with UPDATE_GOLDEN=1.",
        path.display(),
    );
}

#[test]
fn golden_petstore() {
    check_golden("petstore.imposter.json", PETSTORE);
}

#[test]
fn golden_templates() {
    check_golden("templates.imposter.json", TEMPLATES);
}

/// The property the goldens exist to protect, asserted without a file so it holds even while a
/// golden is being regenerated.
#[test]
fn recompiling_the_same_bytes_is_byte_identical() {
    for spec in [PETSTORE, TEMPLATES] {
        let a = to_canonical_string(&compile(spec, &opts()).expect("compiles").imposter);
        let b = to_canonical_string(&compile(spec, &opts()).expect("compiles").imposter);
        assert_eq!(a, b);
    }
}

/// Canonical form must not depend on the order keys happen to arrive in, or two nodes with
/// different `serde_json` map backings would disagree about identical output.
#[test]
fn canonical_form_sorts_object_keys_at_every_depth() {
    let value = serde_json::json!({ "b": 1, "a": { "d": 2, "c": [ { "f": 3, "e": 4 } ] } });
    assert_eq!(
        to_canonical_string(&value),
        r#"{"a":{"c":[{"e":4,"f":3}],"d":2},"b":1}"#,
    );
}
