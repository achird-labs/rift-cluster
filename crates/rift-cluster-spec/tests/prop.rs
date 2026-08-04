//! End-to-end determinism properties, driven through the public `compile` entry point so they hold
//! over whatever the internals do. The escaping and ordering properties over the path compiler
//! itself live beside it in `src/path.rs`.

use proptest::prelude::*;
use rift_cluster_spec::{CompileOptions, compile, to_canonical_string};

/// Literal path characters, deliberately including regex metacharacters — `(`, `[`, `.`, `*`, `+`,
/// `?`, `^`, `$`, `|`, `\` — because escaping them is the §8 threat-item-2 mitigation. `{` and `}`
/// are excluded: in a path they are template syntax, not literals.
const LITERAL_CHARS: &str = "abcxyz019().[]*+?^$|\\-_~";

fn literal_segment() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        proptest::sample::select(LITERAL_CHARS.chars().collect::<Vec<_>>()),
        1..6,
    )
    .prop_map(|cs| cs.into_iter().collect())
}

/// A path template as (literal segments, whether a trailing `{id}` template segment is present).
fn path_template() -> impl Strategy<Value = (Vec<String>, bool)> {
    (
        proptest::collection::vec(literal_segment(), 1..4),
        any::<bool>(),
    )
}

fn render_template(segments: &[String], templated: bool) -> String {
    let mut path = String::new();
    for s in segments {
        path.push('/');
        path.push_str(s);
    }
    if templated {
        path.push_str("/{id}");
    }
    path
}

/// The concrete request path a client would send against that template.
fn render_concrete(segments: &[String], templated: bool) -> String {
    let mut path = String::new();
    for s in segments {
        path.push('/');
        path.push_str(s);
    }
    if templated {
        path.push_str("/42");
    }
    path
}

fn spec_for(paths: &[String]) -> Vec<u8> {
    let mut doc = String::from("openapi: 3.0.3\ninfo: { title: t, version: '1' }\npaths:\n");
    for (i, p) in paths.iter().enumerate() {
        // Single-quoted YAML: the charset excludes `'`, so no escaping is needed.
        doc.push_str(&format!(
            "  '{p}':\n    get:\n      operationId: op{i}\n      responses:\n        '200': {{ description: ok }}\n"
        ));
    }
    doc.into_bytes()
}

fn opts() -> CompileOptions {
    CompileOptions {
        port: Some(4545),
        ..CompileOptions::default()
    }
}

fn emitted_patterns(imposter: &serde_json::Value) -> Vec<String> {
    imposter["stubs"]
        .as_array()
        .expect("stubs")
        .iter()
        .filter_map(|s| {
            s["predicates"]
                .as_array()?
                .iter()
                .find_map(|p| Some(p.get("matches")?.get("path")?.as_str()?.to_string()))
        })
        .collect()
}

proptest! {
    /// RFC-004 §8 threat item 2. Three things must hold at once, and the interesting failures break
    /// exactly one: the pattern is a *valid* regex (not broken by a raw metacharacter), it *matches*
    /// the path it was compiled from (not over-escaped), and it is anchored (not widened).
    #[test]
    fn compiled_patterns_are_valid_anchored_and_match_their_own_path(
        (segments, templated) in path_template()
    ) {
        let template = render_template(&segments, templated);
        let compiled = compile(&spec_for(std::slice::from_ref(&template)), &opts())
            .expect("a well-formed single-operation spec compiles");

        let patterns = emitted_patterns(&compiled.imposter);
        prop_assert_eq!(patterns.len(), 1);
        let pattern = &patterns[0];

        prop_assert!(pattern.starts_with('^') && pattern.ends_with('$'), "unanchored: {}", pattern);

        let re = regex::Regex::new(pattern)
            .map_err(|e| TestCaseError::fail(format!("invalid regex {pattern:?}: {e}")))?;
        let concrete = render_concrete(&segments, templated);
        prop_assert!(re.is_match(&concrete), "{} did not match {}", pattern, concrete);

        // Widening check: a template segment must not swallow a `/`, so an extra path segment
        // beyond the template must not match.
        prop_assert!(!re.is_match(&format!("{concrete}/extra")), "{} is widened", pattern);
    }

    /// Same bytes ⇒ same output, which is what makes re-imports diffable and makes every node in
    /// the fleet agree without coordinating.
    #[test]
    fn compilation_is_a_pure_function_of_its_input_bytes(
        templates in proptest::collection::vec(path_template(), 1..5)
    ) {
        let paths: Vec<String> = templates
            .iter()
            .map(|(s, t)| render_template(s, *t))
            .collect();
        // Distinct paths only: a duplicate key is a different (and separately tested) input.
        let mut unique = paths.clone();
        unique.sort();
        unique.dedup();
        prop_assume!(unique.len() == paths.len());

        let spec = spec_for(&paths);
        let a = compile(&spec, &opts()).expect("compiles");
        let b = compile(&spec, &opts()).expect("compiles");

        prop_assert_eq!(to_canonical_string(&a.imposter), to_canonical_string(&b.imposter));
        prop_assert_eq!(a.digest, b.digest);
    }

    /// The emitted stub order must not depend on the order the paths happened to be written in —
    /// otherwise a cosmetic reordering of the spec would read as drift on re-import.
    #[test]
    fn stub_order_is_independent_of_the_order_paths_are_declared(
        templates in proptest::collection::vec(path_template(), 2..5)
    ) {
        let paths: Vec<String> = templates
            .iter()
            .map(|(s, t)| render_template(s, *t))
            .collect();
        let mut unique = paths.clone();
        unique.sort();
        unique.dedup();
        prop_assume!(unique.len() == paths.len());

        let forward = compile(&spec_for(&paths), &opts()).expect("compiles");
        let reversed: Vec<String> = paths.iter().rev().cloned().collect();
        let backward = compile(&spec_for(&reversed), &opts()).expect("compiles");

        // The operationIds are assigned by declaration order, so compare the *paths* in emitted
        // stub order rather than the ids.
        let order = |c: &rift_cluster_spec::CompiledSpec| -> Vec<String> {
            emitted_patterns(&c.imposter)
        };
        prop_assert_eq!(order(&forward), order(&backward));
    }
}
