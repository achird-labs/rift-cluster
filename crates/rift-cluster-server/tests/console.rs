//! The embedded console over HTTP (RFC-006 §7, issue #186) — the `--features console` half.
//!
//! Compiled only with the feature on, which is also the only configuration in which `web/dist`
//! exists. The mirror-image file `console_off.rs` asserts the parity invariant with the feature
//! **off**, and is the one that runs in every ordinary CI lane.
//!
//! What is asserted here is the behaviour a browser depends on and that a unit test cannot see: the
//! assets are really served, under the right content types and cache headers, with the CSP on every
//! response, and with no subresource pointing off-origin. "Air-gapped" is checked as *no external
//! subresource*, not as "no `http` substring anywhere" — React's minified bundle legitimately
//! contains `https://react.dev/errors/` and the SVG namespace URI as string literals, so a substring
//! rule would be red on a correct build and would be "fixed" by weakening it.
#![cfg(feature = "console")]

use std::collections::BTreeSet;
use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "console-embed-secret";

/// The exact policy RFC-006 §9.1 specifies. Byte-for-byte: this is a security control, and a test
/// that only checked for the *presence* of a CSP header would pass against a permissive one.
const EXPECTED_CSP: &str =
    "default-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'";

fn cluster_cli(state: &TempDir) -> EeCli {
    EeCli::try_parse_from([
        "rift-cluster-server",
        "--port",
        "0",
        "--metrics-port",
        "0",
        "--cluster",
        "--cluster-allow-solo",
        "--cluster-bind",
        "127.0.0.1:0",
        "--cluster-probe-bind",
        "127.0.0.1:0",
        "--cluster-secret",
        SECRET,
        "--cluster-state-dir",
        &state.path().to_string_lossy(),
    ])
    .expect("parses")
}

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn start(state: &TempDir) -> ComposedServer {
    let server = compose::start(cluster_cli(state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    server
}

async fn get(admin: &str, path: &str) -> Seen {
    let response = reqwest::get(format!("http://{admin}{path}"))
        .await
        .unwrap_or_else(|e| panic!("GET {path}: {e}"));
    Seen::of(response).await
}

/// Drop HTML comments before scanning for subresources.
///
/// Vite does not strip them, and `index.html`'s own comment explains the CSP by *quoting* the markup
/// it forbids — so a scanner that reads comments finds an "inline `<script>`" that is a sentence.
/// The same trap works in the other direction: a commented-out CDN `<link>` is not a subresource
/// either, and flagging it would train the next person to loosen this check.
fn strip_html_comments(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut rest = html;
    while let Some(open) = rest.find("<!--") {
        out.push_str(&rest[..open]);
        match rest[open..].find("-->") {
            Some(close) => rest = &rest[open + close + "-->".len()..],
            // Unterminated comment: everything after it is commented out.
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// Every `src`/`href` the served HTML declares, in document order.
fn subresource_urls(html: &str) -> Vec<String> {
    let stripped = strip_html_comments(html);
    let html: &str = &stripped;
    let mut found = Vec::new();
    for attr in ["src", "href"] {
        let needle = format!("{attr}=\"");
        let mut rest = html;
        while let Some(at) = rest.find(&needle) {
            rest = &rest[at + needle.len()..];
            match rest.find('"') {
                Some(end) => {
                    found.push(rest[..end].to_owned());
                    rest = &rest[end..];
                }
                None => break,
            }
        }
    }
    found
}

/// AC: `--features console` serves the console at `/console`, and it is the real bundle.
#[tokio::test]
async fn serves_the_console_index() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    for path in ["/console", "/console/"] {
        let seen = get(&admin, path).await;
        assert_eq!(seen.status, 200, "GET {path}: {seen}");
        assert_eq!(
            seen.header("content-type"),
            Some("text/html; charset=utf-8"),
            "GET {path}: {seen}"
        );
        // Not merely non-empty: an empty `web/dist` that rust-embed accepted would otherwise look
        // like success everywhere except in the browser.
        assert!(
            seen.body.contains("<div id=\"root\">"),
            "GET {path} did not serve the console shell: {seen}"
        );
    }

    server.shutdown().await;
}

/// AC: SPA-fallback for pathless routes — the console's own client-side routes must survive a
/// reload, which is what makes a deep link work at all.
#[tokio::test]
async fn pathless_routes_fall_back_to_the_shell() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    for path in ["/console/imposters", "/console/fleet/nodes/rift-1"] {
        let seen = get(&admin, path).await;
        assert_eq!(seen.status, 200, "GET {path}: {seen}");
        assert!(
            seen.body.contains("<div id=\"root\">"),
            "GET {path} must fall back to the shell: {seen}"
        );
    }

    server.shutdown().await;
}

/// A missing *asset* must 404, not fall back to HTML.
///
/// The classic SPA-fallback bug: a mistyped or stale bundle URL answers `200 text/html`, the browser
/// refuses to execute HTML as a script, and the page fails with a MIME-type error that points
/// nowhere near the cause.
#[tokio::test]
async fn a_missing_asset_is_404_not_the_shell() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let seen = get(&admin, "/console/assets/does-not-exist-abcdef.js").await;
    assert_eq!(seen.status, 404, "{seen}");
    assert!(
        !seen.body.contains("<div id=\"root\">"),
        "a missing asset must not be answered with the shell: {seen}"
    );

    server.shutdown().await;
}

/// AC: the CSP is on **every** console response — including the fallback and the 404, which are
/// exactly the paths a "add the header in the happy branch" implementation forgets.
#[tokio::test]
async fn csp_is_on_every_console_response() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let index = get(&admin, "/console/").await;
    let asset_url = subresource_urls(&index.body)
        .into_iter()
        .find(|url| url.ends_with(".js"))
        .expect("the shell references a script");

    for path in [
        "/console",
        "/console/",
        asset_url.as_str(),
        "/console/deep/client/route",
        "/console/assets/missing-XXXX.js",
    ] {
        let seen = get(&admin, path).await;
        assert_eq!(
            seen.header("content-security-policy"),
            Some(EXPECTED_CSP),
            "GET {path} carried the wrong CSP: {seen}"
        );
    }

    server.shutdown().await;
}

/// AC: hashed assets are immutable-cacheable; the shell never is.
///
/// Getting this backwards is a deploy-shaped bug: a cached `index.html` keeps pointing at the
/// previous build's asset hashes long after an upgrade.
#[tokio::test]
async fn cache_control_matches_the_asset_lifetime() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let index = get(&admin, "/console/").await;
    assert_eq!(
        index.header("cache-control"),
        Some("no-cache"),
        "the shell must be revalidated: {index}"
    );

    let urls = subresource_urls(&index.body);
    let hashed: Vec<&String> = urls
        .iter()
        .filter(|url| url.starts_with("/console/assets/"))
        .collect();
    assert!(
        !hashed.is_empty(),
        "the shell referenced no hashed assets: {urls:?}"
    );
    for url in hashed {
        let seen = get(&admin, url).await;
        assert_eq!(seen.status, 200, "GET {url}: {seen}");
        assert_eq!(
            seen.header("cache-control"),
            Some("max-age=31536000, immutable"),
            "GET {url}: {seen}"
        );
    }

    server.shutdown().await;
}

/// AC: content type by extension — a bundle served as `application/octet-stream` does not execute.
#[tokio::test]
async fn assets_are_served_with_their_own_content_type() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let index = get(&admin, "/console/").await;
    let urls = subresource_urls(&index.body);

    let js = urls.iter().find(|u| u.ends_with(".js")).expect("a script");
    let css = urls
        .iter()
        .find(|u| u.ends_with(".css"))
        .expect("a stylesheet");

    assert_eq!(
        get(&admin, js).await.header("content-type"),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(
        get(&admin, css).await.header("content-type"),
        Some("text/css; charset=utf-8")
    );

    server.shutdown().await;
}

/// AC: air-gapped — the served page pulls nothing from a foreign origin, and runs no inline script.
///
/// Checked against the *served* bytes rather than the sources, because the failure mode this guards
/// is a build step (a plugin inlining a runtime, a CDN font surviving into `dist/`) rather than an
/// edit to `index.html`.
#[tokio::test]
async fn the_served_page_has_no_external_subresource_and_no_inline_script() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let index = get(&admin, "/console/").await;

    for url in subresource_urls(&index.body) {
        assert!(
            !url.contains("://") && !url.starts_with("//"),
            "the shell declares an off-origin subresource: {url}"
        );
    }

    // Every `<script` in the document must carry a `src`. An inline script would be blocked by
    // `script-src 'self'` at runtime, so this catches it at build time instead of as a blank page.
    let markup = strip_html_comments(&index.body);
    // Guard the guard: an over-eager stripper would empty the document and make every assertion
    // below pass by having nothing left to look at.
    assert!(
        markup.contains("<div id=\"root\">"),
        "comment stripping ate the document: {markup}"
    );
    for fragment in markup.split("<script").skip(1) {
        let tag = fragment.split('>').next().unwrap_or_default();
        assert!(
            tag.contains("src="),
            "inline <script> in the served shell — blocked by the CSP: <script{tag}>"
        );
    }

    // The same rule for styles, and it is the one that decides the UI stack (RFC-006 §7's
    // design-stack note). The CSP declares no `style-src`, so styles fall back to
    // `default-src 'self'` with no `'unsafe-inline'` — which blocks both a `<style>` element and a
    // `style=` attribute in the served markup. Runtime CSS-in-JS produces exactly that and would
    // fail only in the embedded build; a build-time stylesheet, which is what this scaffold uses,
    // produces neither.
    //
    // This is also what makes the animation-library question answerable rather than argued: any
    // library that injects markup-level styles fails here the moment it is added.
    if let Some(fragment) = markup.split("<style").nth(1) {
        let tag = fragment.split('>').next().unwrap_or_default();
        panic!(
            "inline <style{tag}> in the served shell — blocked by the CSP's default-src fallback"
        );
    }
    assert!(
        !markup.contains(" style=\""),
        "inline style= attribute in the served shell — blocked by the CSP's default-src fallback"
    );

    // CSS is the other subresource-bearing text asset: a webfont or background image would appear
    // as `url(https://…)` or an `@import` of a foreign origin.
    let css_urls: BTreeSet<String> = subresource_urls(&index.body)
        .into_iter()
        .filter(|u| u.ends_with(".css"))
        .collect();
    assert!(!css_urls.is_empty(), "no stylesheet was served");
    for url in css_urls {
        let css = get(&admin, &url).await;
        assert_eq!(css.status, 200, "GET {url}: {css}");
        for marker in ["url(http", "url(\"http", "url('http", "@import url(http"] {
            assert!(
                !css.body.contains(marker),
                "stylesheet {url} pulls from a foreign origin ({marker})"
            );
        }
    }

    server.shutdown().await;
}

/// The console shell is served **unauthenticated**, deliberately.
///
/// It is the login UI (RFC-006 §5.3): requiring a credential to fetch the page that collects the
/// credential is a closed loop. The bundle carries no secrets, and every API call it then makes goes
/// through the same `authorize_action` chokepoint as any other client. Pinned as a test so the
/// posture is a decision on record rather than an oversight someone later "fixes" in either
/// direction without noticing which one this is.
#[tokio::test]
async fn the_shell_is_served_without_a_credential() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let seen = get(&admin, "/console/").await;
    assert_eq!(
        seen.status, 200,
        "the login UI must be reachable without a credential: {seen}"
    );

    server.shutdown().await;
}

/// `HEAD` is accepted alongside `GET`, as it is for any static asset — and it must answer the
/// headers without a body.
///
/// Asserted rather than assumed: hyper suppresses the body for a `HEAD` response at the encoder,
/// which is a property of the server this module does not control and would otherwise be a protocol
/// bug nothing here would notice.
#[tokio::test]
async fn head_returns_the_headers_without_a_body() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let response = reqwest::Client::new()
        .head(format!("http://{admin}/console/"))
        .send()
        .await
        .expect("HEAD /console/");
    let seen = Seen::of(response).await;

    assert_eq!(seen.status, 200, "{seen}");
    assert_eq!(
        seen.header("content-type"),
        Some("text/html; charset=utf-8"),
        "{seen}"
    );
    assert_eq!(seen.header("content-security-policy"), Some(EXPECTED_CSP));
    assert!(
        seen.body.is_empty(),
        "HEAD must not carry a body, got {} bytes",
        seen.body.len()
    );

    server.shutdown().await;
}

/// Only reads. The console prefix is static assets; a `POST /console` is a client bug, and answering
/// it with the shell and a `200` would hide it.
#[tokio::test]
async fn non_get_methods_are_rejected() {
    let state = TempDir::new().expect("tempdir");
    let server = start(&state).await;
    let admin = server.admin_addr().to_string();

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{admin}/console/"))
        .send()
        .await
        .expect("POST /console/");
    let seen = Seen::of(response).await;
    assert_eq!(seen.status, 405, "{seen}");
    assert_eq!(
        seen.header("content-security-policy"),
        Some(EXPECTED_CSP),
        "even the refusal carries the policy: {seen}"
    );

    server.shutdown().await;
}
