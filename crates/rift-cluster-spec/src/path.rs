//! Path-template compilation and the stub ordering that makes first-match-wins correct.

/// A path template compiled into the two things a stub needs: a pattern to match on, and a route
/// pattern to extract params with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompiledPath {
    /// Anchored regex for the `matches` predicate.
    pub(crate) regex: String,
    /// `/users/:id`, or `None` when the template has no params to extract — or when it has one the
    /// route syntax cannot express (see `compile_path`).
    pub(crate) route_pattern: Option<String>,
    /// Segments containing no template placeholder. The primary ordering key: more literal segments
    /// means a more specific route.
    pub(crate) literal_segments: usize,
    pub(crate) total_segments: usize,
}

/// Compile `/users/{id}` into `^/users/[^/]+$` plus `/users/:id`.
///
/// Literal text is regex-escaped (RFC-004 §8 threat item 2): a path of `/a(b/{id}` must produce
/// `^/a\(b/[^/]+$`, never a pattern that fails to compile or one that quietly matches more than the
/// path it came from.
pub(crate) fn compile_path(template: &str) -> CompiledPath {
    let mut regex = String::from("^");
    let mut route = String::new();
    let mut literal_segments = 0usize;
    let mut total_segments = 0usize;
    // A placeholder that is only *part* of a segment (`/v{n}.json`) matches fine as a regex but has
    // no `:name` spelling, since route patterns name whole segments. Emitting a partial pattern
    // would extract the wrong text, so the whole stub goes without params rather than with bad ones.
    let mut route_expressible = true;

    for (index, segment) in template.split('/').enumerate() {
        if index > 0 {
            regex.push('/');
            route.push('/');
        }
        if index == 0 {
            // Everything before the leading `/`; empty for a well-formed OpenAPI path.
            regex.push_str(&regex::escape(segment));
            route.push_str(segment);
            continue;
        }

        total_segments += 1;
        match whole_segment_placeholder(segment) {
            Some(name) => {
                regex.push_str("[^/]+");
                route.push(':');
                route.push_str(name);
            }
            None => {
                // "contains a brace" is not the same as "contains a placeholder": `/a{b` has an
                // unterminated brace, so it is literal text and must be *counted* as literal or it
                // sorts as though it were a wildcard.
                let (rendered, has_placeholder) = partial_segment_regex(segment);
                regex.push_str(&rendered);
                route.push_str(segment);
                if has_placeholder {
                    route_expressible = false;
                } else {
                    literal_segments += 1;
                }
            }
        }
    }
    regex.push('$');

    let has_params = total_segments > literal_segments;
    CompiledPath {
        regex,
        route_pattern: (has_params && route_expressible).then_some(route),
        literal_segments,
        total_segments,
    }
}

/// `{id}` → `Some("id")`; anything else → `None`.
fn whole_segment_placeholder(segment: &str) -> Option<&str> {
    let inner = segment.strip_prefix('{')?.strip_suffix('}')?;
    (!inner.is_empty() && !inner.contains('{') && !inner.contains('}')).then_some(inner)
}

/// A segment that is not wholly a placeholder: each literal run is escaped, each *complete* `{…}`
/// becomes `[^/]+`. Returns the pattern and whether any placeholder was found — an unterminated `{`
/// is literal text, not a placeholder.
fn partial_segment_regex(segment: &str) -> (String, bool) {
    let mut out = String::new();
    let mut rest = segment;
    let mut has_placeholder = false;
    while let Some(open) = rest.find('{') {
        let Some(close_offset) = rest[open..].find('}') else {
            break;
        };
        has_placeholder = true;
        out.push_str(&regex::escape(&rest[..open]));
        out.push_str("[^/]+");
        rest = &rest[open + close_offset + 1..];
    }
    out.push_str(&regex::escape(rest));
    (out, has_placeholder)
}

/// Sort key placing the most specific route first, because Mountebank semantics are
/// first-match-wins in stub order: `/users/me` must precede `/users/{id}` or the literal route is
/// unreachable. Every component is a deterministic function of the operation, so the sort is a
/// total order and re-compiles stay diffable.
pub(crate) fn order_key(
    path: &CompiledPath,
    template: &str,
    method: &str,
) -> (
    std::cmp::Reverse<usize>,
    std::cmp::Reverse<usize>,
    std::cmp::Reverse<usize>,
    String,
    String,
) {
    (
        std::cmp::Reverse(path.literal_segments),
        std::cmp::Reverse(path.total_segments),
        std::cmp::Reverse(template.len()),
        template.to_string(),
        method.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_literal_path_compiles_to_itself_anchored() {
        let compiled = compile_path("/users/me");
        assert_eq!(compiled.regex, "^/users/me$");
        assert_eq!(compiled.route_pattern, None);
        assert_eq!(compiled.literal_segments, 2);
        assert_eq!(compiled.total_segments, 2);
    }

    #[test]
    fn a_template_segment_becomes_a_bounded_wildcard_and_a_route_param() {
        let compiled = compile_path("/users/{id}");
        assert_eq!(compiled.regex, "^/users/[^/]+$");
        assert_eq!(compiled.route_pattern.as_deref(), Some("/users/:id"));
        assert_eq!(compiled.literal_segments, 1);
    }

    #[test]
    fn the_root_path_compiles() {
        let compiled = compile_path("/");
        assert_eq!(compiled.regex, "^/$");
        assert_eq!(compiled.route_pattern, None);
    }

    #[test]
    fn regex_metacharacters_in_a_literal_segment_are_escaped() {
        assert_eq!(compile_path("/a(b/{id}").regex, r"^/a\(b/[^/]+$");
        assert_eq!(compile_path("/a.b").regex, r"^/a\.b$");
    }

    /// A partial placeholder still matches correctly, but cannot name a whole segment — so it gets
    /// no route pattern rather than one that would extract the wrong substring.
    #[test]
    fn a_partial_placeholder_matches_but_yields_no_route_pattern() {
        let compiled = compile_path("/v{n}.json");
        assert_eq!(compiled.regex, r"^/v[^/]+\.json$");
        assert_eq!(compiled.route_pattern, None);
        assert_eq!(compiled.literal_segments, 0);
        assert_eq!(compiled.total_segments, 1);
    }

    #[test]
    fn an_unterminated_brace_is_literal_text() {
        let compiled = compile_path("/a{b");
        assert_eq!(compiled.regex, r"^/a\{b$");
        assert_eq!(compiled.literal_segments, 1);
    }

    #[test]
    fn more_literal_segments_sort_first() {
        let mut routes = [
            ("/users/{id}", "GET"),
            ("/users/me", "GET"),
            ("/users/{id}/posts/{postId}", "GET"),
        ];
        routes.sort_by_key(|(t, m)| order_key(&compile_path(t), t, m));
        assert_eq!(
            routes.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            ["/users/{id}/posts/{postId}", "/users/me", "/users/{id}"],
        );
    }

    #[test]
    fn the_same_path_with_different_methods_still_has_a_total_order() {
        let mut routes = [("/a", "POST"), ("/a", "DELETE"), ("/a", "GET")];
        routes.sort_by_key(|(t, m)| order_key(&compile_path(t), t, m));
        assert_eq!(
            routes.iter().map(|(_, m)| *m).collect::<Vec<_>>(),
            ["DELETE", "GET", "POST"],
        );
    }

    // Literal characters chosen to include every regex metacharacter that could break or widen a
    // pattern. `{` and `}` are excluded: in a path they are template syntax.
    fn literal_segment() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            proptest::sample::select("abz09().[]*+?^$|\\-_~#& ".chars().collect::<Vec<_>>()),
            1..6,
        )
        .prop_map(|cs| cs.into_iter().collect())
    }

    proptest! {
        /// The escaping invariant of RFC-004 §8: whatever the literal text, the emitted pattern must
        /// compile, must match the literal it came from, and must not match anything longer.
        #[test]
        fn escaping_never_breaks_or_widens_a_pattern(segments in proptest::collection::vec(literal_segment(), 1..4)) {
            let template = format!("/{}", segments.join("/"));
            let compiled = compile_path(&template);

            let re = regex::Regex::new(&compiled.regex)
                .map_err(|e| TestCaseError::fail(format!("{:?} is not a regex: {e}", compiled.regex)))?;
            prop_assert!(re.is_match(&template), "{} did not match its own path {}", compiled.regex, template);
            prop_assert!(!re.is_match(&format!("{template}/extra")), "{} is widened", compiled.regex);
            prop_assert_eq!(compiled.literal_segments, segments.len());
        }

        /// Sorting must not depend on the order the routes arrived in, or a cosmetic reordering of
        /// the spec would read as drift.
        #[test]
        fn the_order_key_is_independent_of_input_order(
            templates in proptest::collection::vec(literal_segment(), 2..6)
        ) {
            let mut paths: Vec<String> = templates.iter().map(|s| format!("/{s}")).collect();
            paths.sort();
            paths.dedup();

            let sorted = |input: &[String]| -> Vec<String> {
                let mut v = input.to_vec();
                v.sort_by_key(|t| order_key(&compile_path(t), t, "GET"));
                v
            };
            let reversed: Vec<String> = paths.iter().rev().cloned().collect();
            prop_assert_eq!(sorted(&paths), sorted(&reversed));
        }
    }
}
