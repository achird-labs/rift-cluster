//! The cluster [`ResponseDecorator`]: cluster op notes → `Rift-Cluster-*` headers.
//!
//! Cluster-aware code inside an OSS handler cannot reach the response — the
//! handlers are deliberately cluster-ignorant. Instead it calls
//! [`rift_cluster_base::seams::annotate`] with a `cluster.*` key, and this decorator turns
//! whatever accumulated on the request into response headers at the boundary.
//!
//! The mapping is structural rather than a per-key registry (`cluster.revision`
//! → `Rift-Cluster-Revision`), so a later phase adds a note by annotating it and
//! nothing here has to change.

use rift_cluster_base::seams::{ResponseDecorator, ResponsePhase};

/// The annotation-key prefix this decorator claims. Anything else on the request
/// belongs to another subsystem and is left alone.
pub const CLUSTER_NOTE_PREFIX: &str = "cluster.";

/// The response-header prefix cluster notes surface under.
pub const CLUSTER_HEADER_PREFIX: &str = "rift-cluster-";

/// The revision a write committed at, as `<port>:<generation>.<revision>`.
pub const NOTE_REVISION: &str = "cluster.revision";

/// Non-fatal warnings about a write (e.g. a port that failed to bind on a peer).
pub const NOTE_WARNINGS: &str = "cluster.warnings";

/// The node that owned the key this request was serialized through.
pub const NOTE_OWNER: &str = "cluster.owner";

/// Ports whose committed config this node's engine could not realize — today, a port bind that
/// lost to an unrelated process (issue #143).
pub const NOTE_BIND_FAILURES: &str = "cluster.bind_failures";

/// Header names the admin write path sets directly on responses it builds
/// itself (issue #9): the front terminates mutating routes outside the OSS
/// handler pipeline, so no annotation scope exists there — but the names stay
/// exactly what the decorator would have produced for the matching notes.
pub const HEADER_REVISION: &str = "rift-cluster-revision";
/// The op id a write carried (minted or client-supplied), for correlation.
pub const HEADER_OP_ID: &str = "rift-cluster-op-id";
/// Non-fatal write warnings, e.g. `unapplied=<node,…>` on a barrier timeout.
pub const HEADER_WARNINGS: &str = "rift-cluster-warnings";
/// This node is serving the addressed imposter in-process only, because it could not bind that
/// imposter's port (issue #143).
///
/// Set directly by the front for the same reason as the three above, and with more force: the
/// affected reads are *proxied*, and the core admin phase decorates with `req_port: None`, so no
/// annotation scope downstream can know which imposter the response is about.
pub const HEADER_BIND_FAILURES: &str = "rift-cluster-bind-failures";
/// A fleet merge-on-read (the journal entries read, its `numberOfRequests` decoration, or the
/// transitional clear fan-out — issue #223) could not confirm every roster peer within its budget.
/// Set directly by the front for the same reason as the three above — there is no per-request
/// annotation scope for a read this journal-net-specific — and additively: a Ch.12 strict-mode gate
/// asserts the header's *absence* on a fully healthy answer, so this must never be stamped `false`.
pub const HEADER_PARTIAL: &str = "rift-cluster-partial";

/// Translates `cluster.*` annotations into `Rift-Cluster-*` response headers.
#[derive(Debug, Clone, Default)]
pub struct ClusterDecorator;

/// Derive the header name for a `cluster.*` annotation key, or `None` if the
/// suffix cannot be spelled as one.
///
/// Rejecting rather than sanitising is deliberate: a mangled header name is
/// indistinguishable from a real one to a client, so a key this function cannot
/// represent faithfully must not be represented at all.
fn header_name_for(note_key: &str) -> Option<String> {
    let suffix = note_key.strip_prefix(CLUSTER_NOTE_PREFIX)?;
    if suffix.is_empty() {
        return None;
    }
    let mut name = String::with_capacity(CLUSTER_HEADER_PREFIX.len() + suffix.len());
    name.push_str(CLUSTER_HEADER_PREFIX);
    for ch in suffix.chars() {
        match ch {
            'a'..='z' | '0'..='9' => name.push(ch),
            'A'..='Z' => name.push(ch.to_ascii_lowercase()),
            '.' | '_' | '-' => name.push('-'),
            _ => return None,
        }
    }
    Some(name)
}

impl ResponseDecorator for ClusterDecorator {
    fn decorate(
        &self,
        _phase: ResponsePhase,
        _req_port: Option<u16>,
        annotations: &[(&'static str, String)],
        headers: &mut hyper::HeaderMap,
    ) {
        for (key, value) in annotations {
            if !key.starts_with(CLUSTER_NOTE_PREFIX) {
                continue;
            }
            // A note that cannot be spelled as a header is dropped, but never
            // silently: it is operator-visible metadata, so losing it has to
            // leave a trace on the node that lost it.
            let Some(name) = header_name_for(key) else {
                tracing::warn!(note = key, "cluster note key is not a valid header name");
                continue;
            };
            let name = match hyper::header::HeaderName::try_from(name.as_str()) {
                Ok(name) => name,
                Err(e) => {
                    tracing::warn!(note = key, error = %e, "cluster note header name rejected");
                    continue;
                }
            };
            match hyper::header::HeaderValue::from_str(value) {
                // Append rather than insert: repeated notes (warnings above all)
                // are a list, and collapsing them would report one and hide the
                // rest.
                Ok(value) => {
                    headers.append(name, value);
                }
                Err(e) => {
                    tracing::warn!(note = key, error = %e, "cluster note value is not header-safe");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::HeaderMap;

    fn decorate(annotations: &[(&'static str, String)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        ClusterDecorator.decorate(ResponsePhase::Admin, Some(4545), annotations, &mut headers);
        headers
    }

    #[test]
    fn cluster_notes_become_prefixed_headers() {
        let headers = decorate(&[
            (NOTE_REVISION, "4545:1.7".to_owned()),
            (NOTE_OWNER, "node-3".to_owned()),
        ]);
        assert_eq!(headers["rift-cluster-revision"], "4545:1.7");
        assert_eq!(headers["rift-cluster-owner"], "node-3");
    }

    #[test]
    fn repeated_notes_are_appended_not_collapsed() {
        let headers = decorate(&[
            (NOTE_WARNINGS, "port 4545 not bound on node-2".to_owned()),
            (NOTE_WARNINGS, "port 4545 not bound on node-3".to_owned()),
        ]);
        let reported: Vec<_> = headers
            .get_all("rift-cluster-warnings")
            .iter()
            .map(|v| v.to_str().expect("ascii header"))
            .collect();
        assert_eq!(
            reported,
            [
                "port 4545 not bound on node-2",
                "port 4545 not bound on node-3"
            ]
        );
    }

    #[test]
    fn non_cluster_notes_are_left_alone() {
        let headers = decorate(&[
            ("flow.cas_retries", "2".to_owned()),
            ("journal.truncated", "true".to_owned()),
        ]);
        assert!(headers.is_empty(), "{headers:?}");
    }

    #[test]
    fn dotted_and_underscored_suffixes_become_dashes() {
        assert_eq!(
            header_name_for("cluster.config.revision").as_deref(),
            Some("rift-cluster-config-revision")
        );
        assert_eq!(
            header_name_for("cluster.bind_failures").as_deref(),
            Some("rift-cluster-bind-failures")
        );
    }

    // The front sets this header itself (the read it marks is proxied, and the admin phase
    // decorates with no port), so the constant and the annotation must not be able to drift
    // apart — a client cannot tell a renamed header from an absent one.
    #[test]
    fn the_directly_set_header_names_match_their_annotations() {
        assert_eq!(
            header_name_for(NOTE_BIND_FAILURES).as_deref(),
            Some(HEADER_BIND_FAILURES)
        );
        assert_eq!(
            header_name_for(NOTE_REVISION).as_deref(),
            Some(HEADER_REVISION)
        );
        assert_eq!(
            header_name_for(NOTE_WARNINGS).as_deref(),
            Some(HEADER_WARNINGS)
        );
    }

    #[test]
    fn unrepresentable_keys_are_refused_rather_than_mangled() {
        for key in ["cluster.", "cluster.a b", "cluster.ünicode", "clusterless"] {
            assert_eq!(header_name_for(key), None, "key {key}");
        }
    }

    #[test]
    fn a_value_that_is_not_header_safe_is_dropped_without_poisoning_the_rest() {
        let headers = decorate(&[
            (NOTE_REVISION, "line\none".to_owned()),
            (NOTE_OWNER, "node-1".to_owned()),
        ]);
        assert!(headers.get("rift-cluster-revision").is_none());
        assert_eq!(headers["rift-cluster-owner"], "node-1");
    }
}
