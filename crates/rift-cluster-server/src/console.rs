//! The embedded web console (RFC-006 §7, issue #186).
//!
//! **This whole module exists only under `--features console`**, which is off by default. That is
//! the parity guarantee, and it is structural rather than a promise: with the feature off there is
//! no module, no `rust-embed` dependency, and no arm in [`crate::admin_front`]'s `handle`, so
//! `/console` proxies upstream and 404s exactly as it did before C3. `tests/console_off.rs` asserts
//! the observable half of that on every ordinary CI run.
//!
//! The assets come from `web/dist/`, built by `pnpm build` in the release lane. `rust-embed` fails
//! the **compile** when that folder is missing, which is the point: a release cannot silently ship
//! consoleless (RFC-006 §7). An *empty* `web/dist` would satisfy the macro, so
//! [`tests::the_embed_contains_the_shell`] closes that gap separately — it is the failure mode the
//! issue calls out as most likely to be quietly satisfied.
//!
//! Serving is deliberately dumb: read-only, unauthenticated, no state. The shell is the login UI
//! (RFC-006 §5.3), so requiring a credential to fetch the page that collects one would be a closed
//! loop; the bundle carries no secrets, and every call it subsequently makes goes through
//! `authorize_action` like any other client.

use std::borrow::Cow;

use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Bytes;
use hyper::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Response, StatusCode};
use rust_embed::RustEmbed;

/// The same boxed body [`crate::admin_front`] serves; `handle` returns this module's responses
/// verbatim, so the types have to agree.
type ConsoleBody = BoxBody<Bytes, hyper::Error>;

/// The built console bundle.
///
/// The path is relative to this crate's `Cargo.toml`, which is how `rust-embed` resolves it — and it
/// bakes that resolved absolute path into a debug build, so load-from-disk works from any working
/// directory in the workspace. (`$CARGO_MANIFEST_DIR` is *not* interpolated here without the
/// `interpolate-folder-path` feature; writing it would produce a literal `$CARGO_MANIFEST_DIR`
/// path segment and a missing-folder error that looks like the real one.)
#[derive(RustEmbed)]
#[folder = "../../web/dist"]
struct Assets;

/// The path the console is mounted at. Vite is configured with the matching `base`, so the emitted
/// asset URLs are absolute from here.
pub(crate) const PREFIX: &str = "/console";

/// The policy from RFC-006 §9.1, sent on **every** response this module produces — including the
/// 404 and the 405, because a policy that is only on the happy path is not a policy.
///
/// `style-src` carries `'unsafe-inline'` as of C5 (#188), and only `style-src`: monaco's standalone
/// theme service styles the editor through two runtime `createElement('style')` calls, which the
/// previous `default-src` fallback blocked — an editor that loads unthemed is shipped broken. The
/// relaxation is scoped to styles; `script-src` stays `'self'` with no inline, which is the half of
/// the policy that stops injected *code*. Inline styles' own risk (selector-based exfiltration)
/// presupposes an attacker who can already inject markup — the layer React's escaping and the
/// no-`dangerouslySetInnerHTML` tests close first — and the shell artifact itself is still asserted
/// style-free by `tests/console.rs`, so the shipped bytes do not lean on this allowance.
///
/// `script-src` additionally carries `'wasm-unsafe-eval'`: browsers gate `WebAssembly` compilation
/// behind it whenever a `script-src` directive is present, and without it the bundled `rift-lint`
/// wasm fails to instantiate in exactly (and only) the shipped build — the one place the artifact
/// exists — while the pane's graceful "lint unavailable" state would have hidden the loss forever.
/// It permits wasm compilation alone, from bytes `default-src 'self'` already restricts to this
/// origin; it is not `'unsafe-eval'` and enables no JS string evaluation.
const CSP: &str = "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self' \
                   'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'";

/// The SPA shell. Served for `/console`, `/console/`, and any client-side route below it.
const SHELL: &str = "index.html";

/// Vite emits content-hashed filenames under this subdirectory and nowhere else, which is what makes
/// a one-year immutable cache safe for it and unsafe for everything outside it.
const HASHED_DIR: &str = "assets/";

/// Does this request belong to the console?
///
/// Exact-match on the prefix as well as the trailing-slash form, so `/consoles` — a different route
/// someone could add later — is not captured by a bare `starts_with`.
pub(crate) fn matches(path: &str) -> bool {
    path == PREFIX || path.starts_with("/console/")
}

/// Serve a console request. Reads only; there is nothing here to mutate.
pub(crate) fn serve(method: &Method, path: &str) -> Response<ConsoleBody> {
    if method != Method::GET && method != Method::HEAD {
        let mut response = refusal(
            StatusCode::METHOD_NOT_ALLOWED,
            "the console serves static assets; only GET and HEAD are supported",
        );
        // RFC 9110 §15.5.6 requires `Allow` on a 405, and it is what tells a client which methods to
        // retry with instead of guessing.
        response
            .headers_mut()
            .insert(hyper::header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }

    let relative = asset_key(path);

    // Reject traversal before the embed ever sees the key, and reject it as a 404 rather than
    // explaining what was wrong.
    //
    // In a *release* build this cannot matter — the embed is a compiled-in map, so an unknown key is
    // simply absent. It matters in a **debug** build, where `rust-embed` reads from the filesystem
    // (the live-reload behaviour that is the whole reason it was chosen over `include_dir`), and
    // where `handle` hands us `req.uri().path()` exactly as the client sent it: hyper does not
    // normalise, so `/console/../../../etc/passwd` arrives intact.
    //
    // rust-embed 8.12 does canonicalise and bounds-check that read, so this is not a live hole. It
    // is here because a security property should not rest on a transitive dependency's internal
    // guard, which a version bump can change without any signal here.
    if !is_safe_key(relative) {
        return refusal(StatusCode::NOT_FOUND, "no such console asset");
    }

    match Assets::get(relative) {
        Some(file) => asset_response(relative, file.data),
        // A path with a file extension that is not in the bundle is a stale or mistyped asset URL,
        // and answering it with the shell is the classic SPA-fallback bug: the browser refuses to
        // execute HTML as a script and reports a MIME error that points nowhere near the cause.
        // Only extensionless paths — which is what a client-side route looks like — fall back.
        None if has_extension(relative) => refusal(StatusCode::NOT_FOUND, "no such console asset"),
        None => match Assets::get(SHELL) {
            Some(shell) => asset_response(SHELL, shell.data),
            // Unreachable in a correct build — the embed is verified to contain the shell by
            // `tests::the_embed_contains_the_shell`. Surfaced as a 500 with a specific message
            // rather than a 404, because "this binary was built with an empty web/dist" and "you
            // asked for a page that does not exist" need different people to look at them.
            None => refusal(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the console bundle embedded in this binary contains no index.html",
            ),
        },
    }
}

/// The path within the bundle that `path` addresses, with the shell as the default.
///
/// Borrows rather than allocating: both arms are already `&str` — `SHELL` is `'static`, which
/// outlives `path` — so there is nothing here worth a `String` per request.
///
/// Percent-escapes are **not** decoded, deliberately. Vite's filenames are always encoding-safe, so
/// nothing in the bundle needs it; and not decoding is what keeps an encoded `%2e%2e` from becoming
/// a `..` *after* [`is_safe_key`] has already inspected the path. A decoding step added later must
/// therefore run before that check, not after.
fn asset_key(path: &str) -> &str {
    match path.strip_prefix("/console/") {
        None | Some("") => SHELL,
        Some(rest) => rest,
    }
}

/// Is this a plain relative key inside the bundle?
///
/// Rejects anything that could resolve outside `web/dist`: a `..` **segment** (not a substring — a
/// file legitimately named `index..hash.js` is fine), a leading `/` (which a doubled slash in the
/// request produces), and a backslash, which is a path separator on Windows and would otherwise
/// slip a `..\` past the segment check.
fn is_safe_key(relative: &str) -> bool {
    !relative.starts_with('/')
        && !relative.contains('\\')
        && !relative.split('/').any(|segment| segment == "..")
}

/// Does the last path segment carry a file extension?
///
/// Segment-scoped on purpose: a client-side route like `/console/imposters/v1.2` has a dot in it but
/// no extension in any meaningful sense, and must still reach the shell.
fn has_extension(relative: &str) -> bool {
    relative
        .rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'))
}

fn asset_response(relative: &str, data: Cow<'static, [u8]>) -> Response<ConsoleBody> {
    let cache = if relative.starts_with(HASHED_DIR) {
        // Content-hashed: the URL changes whenever the bytes do, so it can never go stale.
        "max-age=31536000, immutable"
    } else {
        // The shell's URL is fixed, so a cached copy would keep pointing at the previous build's
        // asset hashes across an upgrade. `no-cache` means revalidate, not "do not store".
        "no-cache"
    };
    respond(
        StatusCode::OK,
        body_bytes(data),
        content_type(relative),
        cache,
    )
}

/// An embedded asset's bytes as a body, **without copying them in a release build**.
///
/// In release, `rust-embed` hands back `Cow::Borrowed(&'static [u8])` — the bytes are already in the
/// binary's rodata — so `Bytes::from_static` is a pointer. Calling `into_owned()` here instead would
/// heap-copy the whole asset on every single request; the JS bundle alone is ~226 KB. The owned arm
/// only occurs in a debug build, where rust-embed read the file from disk and the allocation already
/// happened.
fn body_bytes(data: Cow<'static, [u8]>) -> Bytes {
    match data {
        Cow::Borrowed(bytes) => Bytes::from_static(bytes),
        Cow::Owned(bytes) => Bytes::from(bytes),
    }
}

fn refusal(status: StatusCode, message: &'static str) -> Response<ConsoleBody> {
    respond(
        status,
        Bytes::from_static(message.as_bytes()),
        "text/plain; charset=utf-8",
        "no-store",
    )
}

/// The single construction point for every response this module returns — which is what makes
/// "the CSP is on every console response" true by construction rather than by review.
fn respond(
    status: StatusCode,
    body: Bytes,
    content_type: &'static str,
    cache_control: &'static str,
) -> Response<ConsoleBody> {
    let mut response = Response::new(Full::new(body).map_err(|never| match never {}).boxed());
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(cache_control));
    headers.insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    // `content_type` below is a closed table, so a browser sniffing its way to a different type is
    // the one way an embedded file could still be interpreted as something it is not.
    headers.insert(
        hyper::header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// Content type by extension.
///
/// A conservative table, not a mime-guessing dependency: everything Vite emits for this bundle is
/// listed, and anything else is served as an opaque download rather than guessed at. Guessing wrong
/// on a text type is an XSS vector — `text/html` inferred for an attacker-supplied name would let a
/// bundled file execute in the console's own origin.
fn content_type(relative: &str) -> &'static str {
    let extension = relative.rsplit('.').next().unwrap_or_default();
    match extension {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gap `rust-embed`'s missing-folder compile error does *not* close: an **empty**
    /// `web/dist/` compiles happily and produces a binary that 500s on every console request.
    ///
    /// This is the check the issue flags as the one most likely to be quietly satisfied by an empty
    /// directory, so it is asserted directly rather than inferred from the build succeeding.
    #[test]
    fn the_embed_contains_the_shell() {
        assert!(
            Assets::get(SHELL).is_some(),
            "web/dist/ embedded but has no index.html — the console bundle was not built. \
             Run `pnpm build` in web/ before `cargo build --features console`."
        );
        let files: Vec<String> = Assets::iter().map(|f| f.to_string()).collect();
        assert!(
            files.iter().any(|f| f.starts_with(HASHED_DIR)),
            "the embed carries no hashed assets, only {files:?} — this looks like a stale or \
             partial `web/dist`"
        );
    }

    #[test]
    fn matches_the_console_prefix_and_nothing_adjacent() {
        assert!(matches("/console"));
        assert!(matches("/console/"));
        assert!(matches("/console/assets/index-abc.js"));
        assert!(matches("/console/imposters/4545"));

        assert!(!matches("/consoles"));
        assert!(!matches("/console-admin"));
        assert!(!matches("/imposters"));
        assert!(!matches("/"));
    }

    #[test]
    fn pathless_routes_resolve_to_the_shell_and_asset_paths_do_not() {
        assert_eq!(asset_key("/console"), SHELL);
        assert_eq!(asset_key("/console/"), SHELL);
        assert_eq!(
            asset_key("/console/assets/index-abc.js"),
            "assets/index-abc.js"
        );

        assert!(!has_extension("imposters"));
        assert!(!has_extension("fleet/nodes/rift-1"));
        // A dot inside a route segment is not an extension the bundle would ever contain, and the
        // route still has to reach the shell.
        assert!(has_extension("imposters/v1.2"));
        assert!(has_extension("assets/index-abc.js"));
    }

    #[test]
    fn traversal_keys_are_refused_before_they_reach_the_embed() {
        for path in [
            "/console/../Cargo.toml",
            "/console/../../../../etc/passwd",
            "/console/assets/../../secret",
            "/console//etc/passwd",
            "/console/..\\windows",
        ] {
            let response = serve(&Method::GET, path);
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{path} must not resolve"
            );
        }

        // A `..` inside a *segment* is not traversal, and refusing it would break a legitimate
        // filename. The rule is segment equality, not substring.
        assert!(is_safe_key("assets/index..hash.js"));
        assert!(is_safe_key("assets/index-abc.js"));
        assert!(!is_safe_key("../x"));
        assert!(!is_safe_key("a/../../x"));
        assert!(!is_safe_key("/etc/passwd"));
    }

    #[test]
    fn content_types_cover_what_the_bundle_actually_contains() {
        // Self-defending: without this the loop below iterates nothing against an empty `web/dist`
        // and the test passes having checked no content type at all. A sibling test would fail in
        // the same run, but a test that only holds because of its neighbour is not a check.
        assert!(
            Assets::iter().next().is_some(),
            "the embed is empty — nothing to check content types against"
        );
        for file in Assets::iter() {
            let relative = file.to_string();
            assert_ne!(
                content_type(&relative),
                "application/octet-stream",
                "{relative} is in the bundle but has no content type — a browser will not use it"
            );
        }
    }

    #[test]
    fn every_response_carries_the_policy() {
        let responses = [
            serve(&Method::GET, "/console/"),
            serve(&Method::GET, "/console/a/client/route"),
            serve(&Method::GET, "/console/assets/absent-0000.js"),
            serve(&Method::POST, "/console/"),
        ];
        for response in responses {
            assert_eq!(
                response
                    .headers()
                    .get(CONTENT_SECURITY_POLICY)
                    .and_then(|v| v.to_str().ok()),
                Some(CSP)
            );
        }
    }

    #[test]
    fn a_missing_asset_is_not_answered_with_the_shell() {
        let missing = serve(&Method::GET, "/console/assets/absent-0000.js");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        // The distinguishing detail, and the whole point of the branch: a missing asset must not be
        // answered as HTML, or the browser reports a MIME error instead of a 404.
        assert_eq!(
            missing
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );

        let fallback = serve(&Method::GET, "/console/a/client/route");
        assert_eq!(fallback.status(), StatusCode::OK);
        assert_eq!(
            fallback
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
    }

    #[test]
    fn writes_are_refused() {
        for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
            let response = serve(&method, "/console/");
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} must not be served"
            );
            assert_eq!(
                response
                    .headers()
                    .get(hyper::header::ALLOW)
                    .and_then(|v| v.to_str().ok()),
                Some("GET, HEAD"),
                "a 405 must say what is allowed instead"
            );
        }
    }
}
