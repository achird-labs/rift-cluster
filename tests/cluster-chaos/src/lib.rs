//! Container-tier chaos harness (issue #11).
//!
//! The in-process harness in `rift-cluster/tests/cluster.rs` runs real
//! `RaftNode`s over localhost TCP, which is fast and deterministic and covers
//! everything that is only about nodes and a network. It cannot cover what this
//! one does: **process death**. A `kill -9`, a SIGTERM that has to be answered
//! by a real signal handler, a cold start that has to re-open redb from disk,
//! and the admin API as an operator actually reaches it — those need processes,
//! so they need containers.
//!
//! The topology is `deploy/compose/docker-compose.yml` itself rather than a copy
//! of it, so the artifact that gets shipped and the artifact that gets tested
//! cannot drift apart.
//!
//! Scenarios are `#[ignore]`d: they need a container runtime, and a workspace
//! `cargo test` on a machine without one must not fail. Run them with
//! `cargo test -p cluster-chaos -- --ignored --test-threads=1`.

use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

/// One node as reached from the host: the ports `docker-compose.yml` publishes.
pub struct Node {
    pub name: &'static str,
    pub admin: u16,
    pub probe: u16,
    pub metrics: u16,
}

/// The fleet, in the order the compose file founds it: node 1 bootstraps, the
/// others seed-join through it.
pub const NODES: [Node; 3] = [
    Node {
        name: "rift-1",
        admin: 12525,
        probe: 12526,
        metrics: 19090,
    },
    Node {
        name: "rift-2",
        admin: 22525,
        probe: 22526,
        metrics: 29090,
    },
    Node {
        name: "rift-3",
        admin: 32525,
        probe: 32526,
        metrics: 39090,
    },
];

/// How long a whole fleet gets to come up from cold. Generous: the first run on
/// a machine builds the image.
const UP_TIMEOUT: Duration = Duration::from_secs(240);
/// How long a single convergence assertion waits before failing.
pub const CONVERGE_TIMEOUT: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(250);

/// The compose file publishes fixed host ports and a fixed subnet, so two
/// stacks cannot coexist. Scenarios therefore run one at a time; this is the
/// lock that enforces it even under `cargo test`'s default parallelism, so a
/// forgotten `--test-threads=1` degrades speed rather than correctness.
fn stack_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// A running 3-node cluster. Dropping it tears the stack down, so a scenario
/// that panics mid-assertion still cleans up after itself.
pub struct Cluster {
    _guard: MutexGuard<'static, ()>,
}

impl Cluster {
    /// Bring the fleet up and wait until all three report ready.
    ///
    /// Readiness, not liveness: a node answers `/healthz` long before it has
    /// joined, so waiting on that would prove nothing about a cluster forming.
    pub async fn up() -> anyhow::Result<Self> {
        // Poisoning only means a previous scenario panicked; the stack is torn
        // down by `Drop` either way, so the lock still hands over a clean slate.
        let guard = stack_lock().lock().unwrap_or_else(|e| e.into_inner());
        let cluster = Self { _guard: guard };

        // Down first: a stack left behind by an interrupted run would otherwise
        // be silently reused, and its state dirs would make `test_cold_start`
        // pass for the wrong reason.
        compose(&["down", "-v", "--remove-orphans"]).ok();
        compose(&["up", "-d", "--build"]).context("compose up")?;

        cluster.wait_all_ready(UP_TIMEOUT).await?;
        Ok(cluster)
    }

    /// Wait until every node reports ready.
    pub async fn wait_all_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut ready = 0;
            for node in &NODES {
                if probe(node.probe, "/readyz").await.is_ok_and(|s| s == 200) {
                    ready += 1;
                }
            }
            if ready == NODES.len() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let _ = compose(&["ps"]);
                bail!("only {ready}/{} nodes became ready", NODES.len());
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// SIGTERM a node and wait for it to exit — the graceful-leave path, with a
    /// real signal handler answering a real signal.
    pub fn stop(&self, name: &str) -> anyhow::Result<()> {
        compose(&["stop", name]).with_context(|| format!("stop {name}"))?;
        Ok(())
    }

    /// SIGKILL a node: no drain, no leave, no chance to tidy up.
    pub fn kill(&self, name: &str) -> anyhow::Result<()> {
        run("docker", &["kill", "--signal", "KILL", name])
            .with_context(|| format!("kill {name}"))?;
        Ok(())
    }

    /// Start a stopped/killed node again, keeping its state directory.
    pub fn start(&self, name: &str) -> anyhow::Result<()> {
        compose(&["start", name]).with_context(|| format!("start {name}"))?;
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Best effort by construction: this runs during unwind on a failed
        // assertion, where a second failure would replace the real one.
        let _ = compose(&["down", "-v", "--remove-orphans"]);
    }
}

/// Run a `docker compose` subcommand against the shipped deployment file.
fn compose(args: &[&str]) -> anyhow::Result<Output> {
    let file = compose_file();
    let mut full = vec!["compose", "-f", &file];
    full.extend_from_slice(args);
    run("docker", &full)
}

fn compose_file() -> String {
    // CARGO_MANIFEST_DIR is `<repo>/tests/cluster-chaos`.
    format!(
        "{}/../../deploy/compose/docker-compose.yml",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn run(program: &str, args: &[&str]) -> anyhow::Result<Output> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("spawn {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} {args:?} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

/// GET a probe endpoint, returning its status.
pub async fn probe(port: u16, path: &str) -> anyhow::Result<u16> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(3))
        .send()
        .await?;
    Ok(response.status().as_u16())
}

/// Create an imposter through a node's admin API, returning the response status.
pub async fn put_imposter(admin: u16, port: u16, body_text: &str) -> anyhow::Result<u16> {
    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "responses": [{ "is": { "statusCode": 200, "body": body_text } }]
        }]
    });
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{admin}/imposters"))
        .timeout(Duration::from_secs(15))
        .json(&body)
        .send()
        .await?;
    Ok(response.status().as_u16())
}

/// The ports a node currently has configured, read from its admin API.
pub async fn imposter_ports(admin: u16) -> anyhow::Result<Vec<u64>> {
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{admin}/imposters"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;
    Ok(body["imposters"]
        .as_array()
        .map(|imposters| {
            imposters
                .iter()
                .filter_map(|i| i["port"].as_u64())
                .collect()
        })
        .unwrap_or_default())
}

/// Poll every node until each has `port` configured — the convergence
/// assertion, read from the admin API rather than from logs.
pub async fn wait_converged(port: u64, timeout: Duration) -> anyhow::Result<()> {
    wait_converged_on(&NODES.iter().collect::<Vec<_>>(), port, timeout).await
}

/// [`wait_converged`], restricted to a named subset — for scenarios where some
/// node is deliberately down.
pub async fn wait_converged_on(
    nodes: &[&Node],
    port: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut seen = 0;
        for node in nodes {
            if imposter_ports(node.admin)
                .await
                .is_ok_and(|ports| ports.contains(&port))
            {
                seen += 1;
            }
        }
        if seen == nodes.len() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("imposter {port} reached only {seen}/{} nodes", nodes.len());
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Scrape one gauge/counter family from a node's metrics port.
///
/// Assertions read metrics and the admin API, never log output: a log line is
/// not an interface and a scenario that greps for one fails the day someone
/// rewords it.
pub async fn metric(port: u16, family: &str) -> anyhow::Result<f64> {
    let text = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .text()
        .await?;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(family)
            && let Some(value) = rest.split_whitespace().next_back()
        {
            return value.parse().context("parse metric value");
        }
    }
    bail!("metric family {family} not present on :{port}")
}

/// `rift_cluster_members{state="leader"}` — 1 on the node that holds
/// leadership, 0 elsewhere, so summing it across the fleet answers "is there
/// exactly one leader?".
const LEADER_GAUGE: &str = r#"rift_cluster_members{state="leader"}"#;
/// `rift_cluster_members{state="voter"}` — the size of the effective voter set
/// as this node sees it.
const VOTER_GAUGE: &str = r#"rift_cluster_members{state="voter"}"#;

/// Wait until **exactly one** node reports itself leader, and return its index.
///
/// Exactly one, not at least one: a split brain must fail here rather than pass
/// as "a leader exists". The fleet gauges are sampled on a 5s timer, so this
/// polls rather than reading once — asserting immediately races the sampler and
/// fails on a healthy cluster.
pub async fn wait_single_leader(timeout: Duration) -> anyhow::Result<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut leaders = Vec::new();
        for (i, node) in NODES.iter().enumerate() {
            if metric(node.metrics, LEADER_GAUGE)
                .await
                .is_ok_and(|v| v == 1.0)
            {
                leaders.push(i);
            }
        }
        if leaders.len() == 1 {
            return Ok(leaders[0]);
        }
        if Instant::now() >= deadline {
            bail!("expected exactly one leader, found {leaders:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until `node` reports the effective voter set has reached `expected`.
pub async fn wait_voters(node: &Node, expected: f64, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let seen = metric(node.metrics, VOTER_GAUGE).await;
        if seen.as_ref().is_ok_and(|v| *v == expected) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "{} reports voters={:?}, expected {expected}",
                node.name,
                seen.ok()
            );
        }
        tokio::time::sleep(POLL).await;
    }
}
