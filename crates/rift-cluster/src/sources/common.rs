//! Small helpers that were byte-identical across two or more of the
//! cluster source providers (issue #136 review): the multi-document merge
//! rule, hex encoding, and a capped body read. Extracted once rather than left
//! to drift apart as three near-duplicate copies.

use std::collections::HashMap;

use rift_cluster_base::seams::LoadedConfig;

/// Merge documents (or per-service responses) into one [`LoadedConfig`], in
/// the order given, refusing a port / `intercept` / `routes` block declared
/// by more than one.
///
/// Mirrors upstream `SourceSet::fetch_all`'s merge rule, applied here across
/// whatever a provider assembles from one source: a git directory's files
/// (`sources::git`), or a registry's per-service responses
/// (`sources::registry`). `noun` names what a duplicate is blamed on in the
/// error text — `"document"` for a git directory, `"registry service"` for a
/// registry — the only thing that differed between the two call sites before
/// this was extracted.
pub(crate) fn merge_documents(
    entries: Vec<(String, LoadedConfig)>,
    noun: &str,
) -> anyhow::Result<LoadedConfig> {
    let mut merged = LoadedConfig::default();
    let mut claimed_ports: HashMap<u16, String> = HashMap::new();
    let mut intercept_owner: Option<String> = None;
    let mut routes_owner: Option<String> = None;

    for (label, loaded) in entries {
        if let Some(block) = loaded.intercept {
            if let Some(other) = &intercept_owner {
                anyhow::bail!(
                    "{noun}s `{other}` and `{label}` both declare an `intercept` block; there is \
                     one intercept listener, so exactly one {noun} may declare it"
                );
            }
            intercept_owner = Some(label.clone());
            merged.intercept = Some(block);
        }
        if let Some(table) = loaded.routes {
            if let Some(other) = &routes_owner {
                anyhow::bail!(
                    "{noun}s `{other}` and `{label}` both declare a `routes` block; there is one \
                     front-door route table, so exactly one {noun} may declare it"
                );
            }
            routes_owner = Some(label.clone());
            merged.routes = Some(table);
        }
        for config in loaded.imposters {
            if let Some(port) = config.port {
                if let Some(other) = claimed_ports.get(&port) {
                    anyhow::bail!(
                        "{noun}s `{other}` and `{label}` both declare port {port}; each port may \
                         be declared by exactly one {noun}"
                    );
                }
                claimed_ports.insert(port, label.clone());
            }
            merged.imposters.push(config);
        }
    }

    Ok(merged)
}

/// Lowercase hex encoding, shared by every provider that renders a digest or
/// a SigV4 signature.
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Read `response`'s body, refusing anything past
/// [`rift_cluster_base::seams::MAX_BODY_BYTES`] — enforced while streaming rather than
/// trusting `Content-Length`, exactly like upstream's
/// `HttpSource::read_capped`, since a chunked response has no length to trust
/// and an attacker-controlled one cannot be relied on anyway.
///
/// `what` is the already-formatted subject of the error text (e.g. `"imposter
/// source {uri}"`, `"registry response from {url}"`) so each caller keeps its
/// own wording without this helper needing to know which provider called it.
pub(crate) async fn read_capped(response: reqwest::Response, what: &str) -> anyhow::Result<String> {
    use futures_util::StreamExt as _;
    use rift_cluster_base::seams::MAX_BODY_BYTES;

    let mut body: Vec<u8> = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            anyhow::bail!("{what} exceeds the {MAX_BODY_BYTES}-byte limit; refusing to buffer it");
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|e| anyhow::anyhow!("{what} is not valid UTF-8: {e}"))
}
