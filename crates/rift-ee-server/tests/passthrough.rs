//! Behaviour parity with the open-source binary (issue #10 AC1): with the
//! `--cluster` master switch off, the enterprise binary composes the same server
//! the `rift` binary does and behaves identically.

use clap::Parser;
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose;

mod common;

fn cli(args: &[&str]) -> EeCli {
    let mut full = vec!["rift-ee-server", "--port", "0", "--metrics-port", "0"];
    full.extend_from_slice(args);
    EeCli::try_parse_from(full).expect("parses")
}

#[tokio::test]
async fn a_non_cluster_server_serves_the_admin_api_and_its_imposters() {
    let server = compose::start(cli(&[]))
        .await
        .expect("non-cluster server starts");
    let admin = format!("http://{}", server.admin_addr());
    let http = reqwest::Client::new();

    // Create an imposter through the Mountebank-compatible admin API.
    let created = http
        .post(format!("{admin}/imposters"))
        .json(&serde_json::json!({
            "port": 0,
            "protocol": "http",
            "stubs": [{
                "responses": [{ "is": { "statusCode": 201, "body": "composed" } }]
            }]
        }))
        .send()
        .await
        .expect("create imposter");
    assert_eq!(created.status().as_u16(), 201);
    let imposter: serde_json::Value = created.json().await.expect("imposter json");
    let port = imposter["port"].as_u64().expect("assigned port");

    // And serve traffic on it — the data plane, not just the control plane.
    let served = http
        .get(format!("http://127.0.0.1:{port}/anything"))
        .send()
        .await
        .expect("imposter serves");
    assert_eq!(served.status().as_u16(), 201);
    assert_eq!(served.text().await.expect("body"), "composed");

    let deleted = http
        .delete(format!("{admin}/imposters/{port}"))
        .send()
        .await
        .expect("delete imposter");
    assert_eq!(deleted.status().as_u16(), 200);

    server.shutdown().await;
}

#[tokio::test]
async fn a_non_cluster_server_adds_no_cluster_surface() {
    let server = compose::start(cli(&[]))
        .await
        .expect("non-cluster server starts");

    // Clustering off must be indistinguishable from the open-source binary: no
    // probe listener, no cluster port, nothing extra bound.
    assert!(
        server.probe_addr().is_none(),
        "probes must not appear without --cluster"
    );
    assert!(
        server.cluster_addr().is_none(),
        "the cluster port must not appear without --cluster"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn startup_guards_run_before_anything_binds() {
    // A rejected configuration must fail as a startup error, not as a listener
    // that came up and then fell over.
    let err = match compose::start(cli(&["--cluster", "--cluster-secret", "s3cret"])).await {
        Ok(_) => panic!("a cluster without --cluster-bind must not start"),
        Err(e) => e,
    };
    let rendered = format!("{err:#}");
    assert!(rendered.contains("--cluster-bind"), "{rendered}");
}

/// Issue #67: `replay` actually replays.
///
/// It parsed and was then refused with an explanatory error, so the one thing
/// an operator wants from the subcommand — the saved imposters coming back —
/// did not happen. Driven through `dispatch` and `compose::start` together
/// because that pairing *is* the feature: dispatch rewrites the config file and
/// the normal serve path does the rest, exactly as upstream does it.
#[tokio::test]
async fn replay_loads_the_saved_imposters() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let imposter_port = common::ports::reserve_port();

    let saved = dir.path().join("saved.json");
    std::fs::write(
        &saved,
        serde_json::json!({
            "imposters": [{
                "port": imposter_port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "replayed" } }] }]
            }]
        })
        .to_string(),
    )
    .expect("write the saved snapshot");

    let mut parsed = cli(&["replay", "--configfile", &saved.to_string_lossy()]);
    assert_eq!(
        rift_ee_server::bootstrap::dispatch(&mut parsed).expect("replay dispatches"),
        rift_ee_server::bootstrap::AfterBootstrap::Serve
    );

    let server = compose::start(parsed)
        .await
        .expect("the replayed server starts");
    let admin = server.admin_addr();

    let listed: serde_json::Value = reqwest::get(format!("http://{admin}/imposters"))
        .await
        .expect("list imposters")
        .json()
        .await
        .expect("imposters json");
    assert!(
        listed["imposters"]
            .as_array()
            .expect("imposters array")
            .iter()
            .any(|i| i["port"].as_u64() == Some(u64::from(imposter_port))),
        "the replayed imposter must be present: {listed}"
    );

    let body = reqwest::get(format!("http://127.0.0.1:{imposter_port}/"))
        .await
        .expect("the replayed imposter answers")
        .text()
        .await
        .expect("body");
    assert_eq!(
        body, "replayed",
        "the replayed imposter must serve its stub"
    );

    server.shutdown().await;
}
