//! The parity invariant for the console embed (RFC-006 §7, issue #186): with the `console` feature
//! **off** — which is every ordinary dev and CI lane, including the `--cluster-off` parity lanes
//! (#139) — `/console` is not a route this binary knows about. It proxies upstream and 404s exactly
//! as it did before C3.
//!
//! The mirror image of `console.rs`, and the one that actually runs on every PR. `cfg` makes the
//! serving code unreachable by construction; this asserts the *observable* half of that claim, which
//! is the half a reviewer of a future change can be shown.
#![cfg(not(feature = "console"))]

use std::time::Duration;

use clap::Parser;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "console-parity-secret";

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

/// AC: feature off ⇒ zero new code reachable on the request path.
///
/// Pins D-33 (the feature-off half): the default build carries no console code at all, so even a
/// *clustered* node answers `/console` with upstream's own 404 and none of the console module's
/// headers — the drop-in-for-OSS invariant does not depend on `--cluster` being off. The
/// unclustered half is `tests/passthrough.rs::a_non_cluster_server_does_not_serve_the_console`.
#[tokio::test]
async fn console_is_not_served_when_the_feature_is_off() {
    let state = TempDir::new().expect("tempdir");
    let server = compose::start(cluster_cli(&state))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();

    for path in ["/console", "/console/", "/console/assets/index.js"] {
        let response = reqwest::get(format!("http://{admin}{path}"))
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        let seen = Seen::of(response).await;

        assert_eq!(
            seen.status, 404,
            "GET {path} must proxy upstream and 404 with the feature off: {seen}"
        );
        // The header is the console module's signature. Its absence is what proves the request was
        // never handled by console code — a stronger statement than the status alone, which upstream
        // could produce for unrelated reasons.
        assert_eq!(
            seen.header("content-security-policy"),
            None,
            "console code ran with the feature off: {seen}"
        );
        assert!(
            !seen.body.contains("<div id=\"root\">"),
            "the console shell was served with the feature off: {seen}"
        );
    }

    server.shutdown().await;
}
