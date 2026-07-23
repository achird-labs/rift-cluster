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
    /// Its fixed address on the `rift` network. Needed to re-pin the address on
    /// [`Cluster::heal`]: toxiproxy's upstreams name these, so a node that came
    /// back on a DHCP-assigned address would be unreachable to its peers.
    pub ip: &'static str,
    /// The toxiproxy listener that fronts this node's cluster port under the
    /// chaos overlay — the handle C6 attaches toxics to.
    pub proxy: &'static str,
    /// This node's admin API reached over `mgmt`, via toxiproxy.
    ///
    /// Use this, not [`Node::admin`], to assert on a node that is currently
    /// partitioned: a published port's DNAT is programmed against one network,
    /// and `docker network disconnect` takes it down with that network. This
    /// path is published from a container that is never disconnected and hops
    /// to the node over `mgmt`, so neither leg depends on `rift`. Chaos overlay
    /// only.
    pub admin_via_mgmt: u16,
    /// This node's metrics endpoint over `mgmt` — same reasoning.
    pub metrics_via_mgmt: u16,
}

/// The fleet, in the order the compose file founds it: node 1 bootstraps, the
/// others seed-join through it.
pub const NODES: [Node; 3] = [
    Node {
        name: "rift-1",
        admin: 12525,
        probe: 12526,
        metrics: 19090,
        ip: "172.28.7.11",
        proxy: "cluster-rift-1",
        admin_via_mgmt: 45251,
        metrics_via_mgmt: 45261,
    },
    Node {
        name: "rift-2",
        admin: 22525,
        probe: 22526,
        metrics: 29090,
        ip: "172.28.7.12",
        proxy: "cluster-rift-2",
        admin_via_mgmt: 45252,
        metrics_via_mgmt: 45262,
    },
    Node {
        name: "rift-3",
        admin: 32525,
        probe: 32526,
        metrics: 39090,
        ip: "172.28.7.13",
        proxy: "cluster-rift-3",
        admin_via_mgmt: 45253,
        metrics_via_mgmt: 45263,
    },
];

/// The compose project's `rift` network, as Docker names it (`<project>_<net>`).
/// Partitioning detaches a node from this one and leaves `mgmt` attached.
const RIFT_NETWORK: &str = "rift-ee-cluster_rift";

/// Toxiproxy's API port, published by the chaos overlay.
const TOXIPROXY_PORT: u16 = 48474;

/// Envoy's front door and admin interface, published by the chaos overlay.
pub const FRONT_PORT: u16 = 42525;
pub const ENVOY_ADMIN_PORT: u16 = 49901;

/// How long a whole fleet gets to come up from cold. Generous: the first run on
/// a machine builds the image.
const UP_TIMEOUT: Duration = Duration::from_secs(240);
/// How long a single convergence assertion waits before failing.
pub const CONVERGE_TIMEOUT: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(250);
/// How long a published host port may stay bound after its stack is gone.
///
/// Generous against dockerd's own proxy teardown, short enough that a genuine
/// squatter is reported rather than waited out: the alternative to failing here
/// is `compose up` failing anyway, 30s later, without naming the port.
const PORTS_FREE_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// The `-f` list this stack came up with. Held so teardown and every
    /// per-node command address the same topology: tearing a chaos stack down
    /// with only the base file leaves toxiproxy and Envoy running, and the next
    /// scenario inherits them.
    files: Vec<String>,
    _guard: MutexGuard<'static, ()>,
}

impl Cluster {
    /// Bring the fleet up and wait until all three report ready.
    ///
    /// Readiness, not liveness: a node answers `/healthz` long before it has
    /// joined, so waiting on that would prove nothing about a cluster forming.
    pub async fn up() -> anyhow::Result<Self> {
        Self::start_stack(vec![base_file()]).await
    }

    /// [`Cluster::up`] with the chaos overlay layered on: every cluster link
    /// runs through toxiproxy, an Envoy front is published, and every node also
    /// sits on the `mgmt` network so it stays reachable from the host while
    /// partitioned.
    pub async fn up_with_chaos() -> anyhow::Result<Self> {
        Self::start_stack(vec![base_file(), overlay_file()]).await
    }

    /// [`Cluster::up`] with `--cluster-write-barrier=none` on every node.
    ///
    /// The whole fleet, not one node: the barrier is a property of whichever
    /// node answers the write, so a mixed fleet would make a convergence
    /// measurement depend on which node the scenario happened to write through.
    pub async fn up_with_barrier_none() -> anyhow::Result<Self> {
        Self::start_stack(vec![base_file(), barrier_none_file()]).await
    }

    /// [`Cluster::up`] with an explicit list of overlays layered over the
    /// shipped base file, in order.
    ///
    /// The named helpers above cover the one- and two-file cases; C16 needs
    /// three at once (chaos for the toxiproxy links, barrier-none so a write
    /// returns before the fleet has applied it, and pull-on-miss to publish the
    /// data port its assertion reads). Composing them by name beats adding a
    /// third named constructor per combination.
    pub async fn up_with_overlays(overlays: &[&str]) -> anyhow::Result<Self> {
        let mut files = vec![base_file()];
        files.extend(overlays.iter().map(|name| compose_file(name)));
        Self::start_stack(files).await
    }

    /// Bring up exactly one node, on the shipped topology, and do **not** wait
    /// for readiness.
    ///
    /// `--no-deps` so `depends_on` does not quietly drag the seed up and make
    /// the node's seeds reachable after all — which would turn a scenario about
    /// never becoming ready into one that passes by becoming ready.
    pub async fn up_isolated(name: &str) -> anyhow::Result<Self> {
        let guard = stack_lock().lock().unwrap_or_else(|e| e.into_inner());
        let cluster = Self {
            files: vec![base_file()],
            _guard: guard,
        };
        compose_with(
            &[base_file(), overlay_file()],
            &["down", "-v", "--remove-orphans"],
        )
        .ok();
        wait_stack_gone();
        wait_ports_free(PORTS_FREE_TIMEOUT)?;
        cluster
            .compose(&["up", "-d", "--build", "--no-deps", name])
            .context("compose up single node")?;
        Ok(cluster)
    }

    async fn start_stack(files: Vec<String>) -> anyhow::Result<Self> {
        // Poisoning only means a previous scenario panicked; the stack is torn
        // down by `Drop` either way, so the lock still hands over a clean slate.
        let guard = stack_lock().lock().unwrap_or_else(|e| e.into_inner());
        let cluster = Self {
            files,
            _guard: guard,
        };

        // Down first: a stack left behind by an interrupted run would otherwise
        // be silently reused, and its state dirs would make `test_cold_start`
        // pass for the wrong reason.
        //
        // Torn down with BOTH files regardless of which this stack wants, so a
        // chaos stack left behind by an interrupted run cannot survive into a
        // plain one.
        compose_with(
            &[base_file(), overlay_file()],
            &["down", "-v", "--remove-orphans"],
        )
        .ok();
        wait_stack_gone();
        wait_ports_free(PORTS_FREE_TIMEOUT)?;
        cluster
            .compose(&["up", "-d", "--build"])
            .context("compose up")?;

        cluster.wait_all_ready(UP_TIMEOUT).await?;
        cluster.wait_cluster_formed(UP_TIMEOUT).await?;
        Ok(cluster)
    }

    /// Wait until the fleet has actually formed a cluster, not merely started.
    ///
    /// `/readyz` going 200 on all three is necessary but not sufficient: the
    /// Raft-derived gauges are resampled on a 5s timer, so a node that founded
    /// solo and has since been joined still *publishes* one voter for up to a
    /// sampling interval. A scenario that begins asserting on that window reads
    /// a stale gauge as a membership change -- which is how C6 failed before
    /// this existed, and it would have been reported as the product flapping.
    pub async fn wait_cluster_formed(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut voters_ok = 0;
            let mut leaders = 0;
            for node in &NODES {
                if metric(node.metrics, VOTER_GAUGE)
                    .await
                    .is_ok_and(|v| v == NODES.len() as f64)
                {
                    voters_ok += 1;
                }
                if metric(node.metrics, LEADER_GAUGE)
                    .await
                    .is_ok_and(|v| v == 1.0)
                {
                    leaders += 1;
                }
            }
            if voters_ok == NODES.len() && leaders == 1 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "fleet started but never formed a cluster: {voters_ok}/{} nodes see a \
                     full voter set, {leaders} leaders",
                    NODES.len()
                );
            }
            tokio::time::sleep(POLL).await;
        }
    }

    fn compose(&self, args: &[&str]) -> anyhow::Result<Output> {
        compose_with(&self.files, args)
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
                let _ = self.compose(&["ps"]);
                bail!("only {ready}/{} nodes became ready", NODES.len());
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// SIGTERM a node and wait for it to exit — the graceful-leave path, with a
    /// real signal handler answering a real signal.
    pub fn stop(&self, name: &str) -> anyhow::Result<()> {
        self.compose(&["stop", name])
            .with_context(|| format!("stop {name}"))?;
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
        self.compose(&["start", name])
            .with_context(|| format!("start {name}"))?;
        Ok(())
    }

    /// Cut a node off from every peer by detaching it from the `rift` network.
    ///
    /// This is a whole-node isolation and it is symmetric — inbound and
    /// outbound die together, because they were the same interface. That
    /// matters: a cut that only blocks inbound leaves the isolated node
    /// campaigning at a majority that can still hear it, which destabilises the
    /// majority and is the opposite of what a partition scenario asserts.
    ///
    /// Toxiproxy cannot express this. Every peer dials a node at its single
    /// advertised address, so one listener carries all of them; disabling it
    /// cuts inbound only. Per-link proxies would need per-source addressing,
    /// i.e. hostname advertise, which is #68.
    ///
    /// The node stays reachable from the host over `mgmt` — see the network
    /// comment in `chaos.overlay.yml`. Chaos overlay only.
    pub fn partition(&self, name: &str) -> anyhow::Result<()> {
        run("docker", &["network", "disconnect", RIFT_NETWORK, name])
            .with_context(|| format!("partition {name}"))?;
        Ok(())
    }

    /// Undo [`Cluster::partition`], restoring the node's fixed address.
    ///
    /// The address is re-pinned rather than left to Docker: toxiproxy's
    /// upstreams name these IPs, so a node that healed onto a different one
    /// would be reachable by nobody and the scenario would misread that as a
    /// failure to converge.
    pub fn heal(&self, node: &Node) -> anyhow::Result<()> {
        run(
            "docker",
            &[
                "network",
                "connect",
                "--ip",
                node.ip,
                RIFT_NETWORK,
                node.name,
            ],
        )
        .with_context(|| format!("heal {}", node.name))?;
        Ok(())
    }

    /// Replace a node with a brand-new container, discarding its state.
    ///
    /// `rm -sf` then `up`, not `restart`: the compose file declares no volumes,
    /// so state lives in the container's own filesystem and only destroying it
    /// produces the empty `/var/lib/rift` that a first-time joiner has.
    pub fn recreate(&self, name: &str) -> anyhow::Result<()> {
        self.compose(&["rm", "-sf", name])
            .with_context(|| format!("rm {name}"))?;
        self.compose(&["up", "-d", "--no-deps", name])
            .with_context(|| format!("recreate {name}"))?;
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // A nightly soak that fails at 3am and tears the evidence down with it
        // is a failure nobody can act on, so capture first -- but only when
        // something actually went wrong, and only where the runner asked for it.
        if std::thread::panicking()
            && let Ok(dir) = std::env::var("CHAOS_LOG_DIR")
        {
            let _ = self.dump_logs(&dir);
        }
        // Best effort by construction: this runs during unwind on a failed
        // assertion, where a second failure would replace the real one.
        let _ = self.compose(&["down", "-v", "--remove-orphans"]);
    }
}

impl Cluster {
    /// Write `compose ps` and `compose logs` to `$CHAOS_LOG_DIR` before
    /// teardown. Named for the failing test, so a matrix job's artifact says
    /// which scenario produced it.
    fn dump_logs(&self, dir: &str) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir).context("create log dir")?;
        let scenario = std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_owned();
        let path = std::path::Path::new(dir).join(format!("{scenario}.log"));

        let mut out = String::new();
        for args in [
            &["ps", "-a"][..],
            &["logs", "--no-color", "--timestamps"][..],
        ] {
            out.push_str(&format!("===== docker compose {} =====\n", args.join(" ")));
            match self.compose(args) {
                Ok(o) => {
                    out.push_str(&String::from_utf8_lossy(&o.stdout));
                    out.push_str(&String::from_utf8_lossy(&o.stderr));
                }
                Err(e) => out.push_str(&format!("<capture failed: {e}>\n")),
            }
            out.push('\n');
        }
        std::fs::write(&path, out).context("write log dump")
    }
}

/// Every host port the compose files in this repo publish to the host.
///
/// Derived from the constants the scenarios already use rather than written out
/// again: a port list that has to be maintained in two places is a port list
/// that will disagree with itself, and the half that disagrees silently is the
/// barrier.
///
/// These constants are the scenarios' view; the compose files are the truth.
/// `the_barrier_covers_exactly_what_compose_publishes` asserts the two are the
/// same set, because deriving from constants alone would only prove they agree
/// with themselves.
#[must_use]
pub fn published_host_ports() -> Vec<u16> {
    let mut ports = Vec::with_capacity(NODES.len() * 5 + 6);
    for node in &NODES {
        ports.extend([
            node.admin,
            node.probe,
            node.metrics,
            node.admin_via_mgmt,
            node.metrics_via_mgmt,
        ]);
    }
    ports.extend([FRONT_PORT, ENVOY_ADMIN_PORT, TOXIPROXY_PORT]);
    ports.extend(PULL_ON_MISS_HOST_PORTS);
    ports
}

/// Block until every published host port can be bound again.
///
/// [`wait_stack_gone`] waits for *containers*; this waits for the *sockets*, and
/// they are not the same moment. Whatever still holds one — dockerd's per-port
/// proxy finishing its own teardown, or an unrelated outbound connection that
/// was handed the port out of the ephemeral pool — `compose up` fails on it with
/// `failed to bind host port ... address already in use` and no indication of
/// which port or who held it. That opaque failure is issue #117; this turns it
/// into a named one, before the fleet is even asked to start.
///
/// Probing means binding: there is no way to ask "is this bindable" that is not
/// itself a bind, and a bind that succeeds is dropped immediately. That leaves a
/// window in which something else could take the port between the probe and
/// docker's own bind — this narrows the race rather than closing it, which is
/// why the CI-side reservation of the ephemeral range (issue #117's other half)
/// is not redundant with it.
///
/// What it does **not** see is a port held only in `TIME_WAIT`, because both
/// this probe and docker-proxy bind with `SO_REUSEADDR` and that option exists
/// precisely to permit such a bind (measured on darwin: rebinding a
/// `TIME_WAIT`-held port succeeds, while a live listener is refused). That is
/// the right behaviour rather than a gap — a port docker *can* take is a port
/// this must report free, or the barrier would stall 60s after every scenario.
pub fn wait_ports_free(timeout: Duration) -> anyhow::Result<()> {
    wait_ports_free_in(&published_host_ports(), timeout)
}

/// [`wait_ports_free`] over an explicit port list.
///
/// Exists so the barrier can be tested against a port the test itself owns: a
/// test that waited on the whole published set would fail on any machine with
/// the `deploy/compose` demo stack up, which is a false alarm about the
/// developer's machine rather than a fact about the barrier.
pub fn wait_ports_free_in(ports: &[u16], timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        // `0.0.0.0`, matching what compose publishes on: BSD accepts a wildcard
        // bind alongside a loopback one, so probing `127.0.0.1` would report a
        // port free that docker cannot have.
        let held: Vec<u16> = ports
            .iter()
            .copied()
            .filter(|&port| std::net::TcpListener::bind(("0.0.0.0", port)).is_err())
            .collect();

        if held.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "published host ports still bound {timeout:?} after teardown: {held:?}. \
                 `compose up` would fail on one of these with an unattributable \
                 'address already in use'. Find the holder with: \
                 lsof -nP -iTCP:{} -sTCP:LISTEN",
                held[0]
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Block until the previous stack's containers are actually gone.
///
/// `compose down` returns once it has *asked* for removal, and a container
/// still shutting down keeps its published ports bound and keeps answering
/// probes. The next scenario then reads the dying stack as its own -- which is
/// how a chaos scenario came to see a fleet it never started, and read a stale
/// one-voter membership as a real one.
fn wait_stack_gone() {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let remaining = Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                "label=com.docker.compose.project=rift-ee-cluster",
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().is_empty())
            .unwrap_or(true);
        if remaining {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Run a `docker compose` subcommand against a given `-f` list.
fn compose_with(files: &[String], args: &[&str]) -> anyhow::Result<Output> {
    let mut full = vec!["compose"];
    for file in files {
        full.push("-f");
        full.push(file);
    }
    full.extend_from_slice(args);
    run("docker", &full)
}

/// The shipped topology — tested as deployed, never as a copy.
fn base_file() -> String {
    // CARGO_MANIFEST_DIR is `<repo>/tests/cluster-chaos`.
    format!(
        "{}/../../deploy/compose/docker-compose.yml",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// The chaos-only additions, layered over it.
fn overlay_file() -> String {
    format!("{}/compose/chaos.overlay.yml", env!("CARGO_MANIFEST_DIR"))
}

fn barrier_none_file() -> String {
    format!(
        "{}/compose/barrier-none.overlay.yml",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// An overlay in `compose/`, by file name.
fn compose_file(name: &str) -> String {
    format!("{}/compose/{name}", env!("CARGO_MANIFEST_DIR"))
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

/// GET a JSON document from any published HTTP port.
pub async fn get_json(port: u16, path: &str) -> anyhow::Result<(u16, serde_json::Value)> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    Ok((status, body))
}

/// Poll until a node's admin API answers, and fail with the last error if it
/// never does.
///
/// Retried rather than asked once, because `docker network disconnect`
/// reprograms the published-port DNAT rules and an in-flight connection during
/// that window hangs rather than being refused. A single request issued right
/// after a partition therefore times out on a node that is perfectly reachable
/// a second later -- which reads as "the mgmt network did not hold" and is
/// wrong. A genuinely unreachable node still fails, just after the timeout.
pub async fn wait_admin_reachable(admin: u16, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let attempt = match get_json(admin, "/imposters").await {
            Ok((200, _)) => return Ok(()),
            Ok((status, _)) => format!("status {status}"),
            Err(e) => e.to_string(),
        };
        if Instant::now() >= deadline {
            bail!("admin :{admin} never answered ({attempt})");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Replace an imposter's stub list, for the reorder scenario.
pub async fn put_stubs(admin: u16, port: u16, stubs: serde_json::Value) -> anyhow::Result<u16> {
    let response = reqwest::Client::new()
        .put(format!("http://127.0.0.1:{admin}/imposters/{port}/stubs"))
        .timeout(Duration::from_secs(30))
        .json(&serde_json::json!({ "stubs": stubs }))
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

/// [`put_imposter`] carrying an `Idempotency-Key`, returning the status and the
/// response headers.
///
/// The headers are the point: a write that cannot reach a leader is answered
/// `503`/`504` with a `rift-cluster-op-id`, and that id is the receipt proving
/// the intent was parked durably rather than dropped.
pub async fn put_imposter_with_key(
    admin: u16,
    port: u16,
    body_text: &str,
    idempotency_key: &str,
) -> anyhow::Result<(u16, reqwest::header::HeaderMap, serde_json::Value)> {
    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "stubs": [{
            "responses": [{ "is": { "statusCode": 200, "body": body_text } }]
        }]
    });
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{admin}/imposters"))
        // Must outlast the server's own 10s forward deadline, or the client
        // gives up first and the scenario cannot tell a parked 504 from a hang.
        .timeout(Duration::from_secs(30))
        .header("Idempotency-Key", idempotency_key)
        .json(&body)
        .send()
        .await?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    // A body that is not JSON is itself a finding, so it surfaces as null
    // rather than as an error that would mask the status the caller came for.
    let envelope = response.json().await.unwrap_or(serde_json::Value::Null);
    Ok((status, headers, envelope))
}

/// The one imposter data port the `pull-on-miss` overlay publishes, and the
/// host ports it appears on — indexed like [`NODES`]. C16 only.
///
/// Published on every node because C16 picks its lagging node at run time: the
/// node that lags must be a follower, and leadership is not a scenario's to
/// assume.
pub const PULL_ON_MISS_IMPOSTER_PORT: u16 = 6300;
pub const PULL_ON_MISS_HOST_PORTS: [u16; 3] = [16300, 26300, 36300];

/// Append a stub to an existing imposter — a `PatchStubs` `ControlOp`, i.e. a
/// config write like any other, not a whole-imposter replacement.
///
/// Returns the status. Distinct from [`put_imposter`] because the point of C16
/// is that the imposter (and therefore the bound port) already exists fleet-wide
/// while a *stub* is still in flight: a node that has not applied a missing
/// imposter has no port bound at all, so a request there is refused at the
/// socket and never reaches the no-match hook the safety net hangs on.
pub async fn append_stub(
    admin: u16,
    port: u16,
    path: &str,
    body_text: &str,
) -> anyhow::Result<u16> {
    let body = serde_json::json!({
        "stub": {
            "predicates": [{ "equals": { "path": path } }],
            "responses": [{ "is": { "statusCode": 200, "body": body_text } }]
        }
    });
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{admin}/imposters/{port}/stubs"))
        .timeout(Duration::from_secs(15))
        .json(&body)
        .send()
        .await?;
    Ok(response.status().as_u16())
}

/// GET a published imposter data port from the host, keeping the response
/// headers.
///
/// [`exec_probe`] cannot serve here: it shells out to the binary's `healthcheck`
/// subcommand, which reports only success or failure and drops headers — and
/// the `rift-cluster-pull-on-miss` header is the entire assertion in C16.
pub async fn get_data_plane(
    host_port: u16,
    path: &str,
) -> anyhow::Result<(u16, reqwest::header::HeaderMap, String)> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{host_port}{path}"))
        // Comfortably past the hook's own 500 ms budget, so a scenario failure
        // reads as "not rescued" rather than as the client giving up first.
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = response.text().await.unwrap_or_default();
    Ok((status, headers, body))
}

/// Probe a URL from *inside* a container, answering with success/failure.
///
/// Imposter ports are not published to the host, so the data plane is only
/// reachable this way. The runtime image ships no curl on purpose; the binary's
/// own `healthcheck` subcommand is the sanctioned in-container probe.
pub fn exec_probe(container: &str, url: &str) -> bool {
    Command::new("docker")
        .args([
            "exec",
            container,
            "rift-ee-server",
            "healthcheck",
            "--url",
            url,
        ])
        .output()
        .is_ok_and(|o| o.status.success())
}

/// Add a toxic to one of the cluster listeners.
pub async fn add_toxic(proxy: &str, toxic: serde_json::Value) -> anyhow::Result<()> {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{TOXIPROXY_PORT}/proxies/{proxy}/toxics"
        ))
        .timeout(Duration::from_secs(10))
        .json(&toxic)
        .send()
        .await?;
    if !response.status().is_success() {
        bail!(
            "add toxic to {proxy}: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }
    Ok(())
}

/// How many toxics are currently attached to a listener.
///
/// A scenario that degrades a link should assert this before concluding
/// anything from the calm that follows: if the toxics never landed, "nothing
/// flapped" is a statement about an untouched cluster and the scenario passes
/// while testing nothing.
pub async fn toxic_count(proxy: &str) -> anyhow::Result<usize> {
    let toxics: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{TOXIPROXY_PORT}/proxies/{proxy}/toxics"
        ))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;
    Ok(toxics.as_array().map(Vec::len).unwrap_or(0))
}

/// Remove every toxic from a listener, restoring a clean link.
pub async fn clear_toxics(proxy: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let toxics: serde_json::Value = client
        .get(format!(
            "http://127.0.0.1:{TOXIPROXY_PORT}/proxies/{proxy}/toxics"
        ))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;
    for toxic in toxics.as_array().into_iter().flatten() {
        let Some(name) = toxic["name"].as_str() else {
            continue;
        };
        client
            .delete(format!(
                "http://127.0.0.1:{TOXIPROXY_PORT}/proxies/{proxy}/toxics/{name}"
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;
    }
    Ok(())
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

/// `rift_cluster_config_revision{port}` on one node — the log index that last
/// wrote that imposter's config.
pub async fn config_revision(metrics: u16, port: u64) -> anyhow::Result<f64> {
    metric(
        metrics,
        &format!(r#"rift_cluster_config_revision{{port="{port}"}}"#),
    )
    .await
}

/// [`wait_revisions_agree`] restricted to the nodes given.
///
/// The unrestricted form requires a reading from **every** node in [`NODES`],
/// so it only works on a whole fleet. A scenario that stopped a node must use
/// this instead, or it waits out the entire timeout polling a node that is gone
/// and then reports its absence as a disagreement.
pub async fn wait_revisions_agree_on(
    nodes: &[&Node],
    port: u64,
    timeout: Duration,
) -> anyhow::Result<f64> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut revisions = Vec::new();
        for node in nodes {
            match config_revision(node.metrics, port).await {
                Ok(v) => revisions.push(v),
                Err(_) => break,
            }
        }
        if revisions.len() == nodes.len() && revisions.iter().all(|v| *v == revisions[0]) {
            return Ok(revisions[0]);
        }
        if Instant::now() >= deadline {
            bail!("nodes disagree on the revision of port {port}: {revisions:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Poll until every node reports the *same* applied revision for `port`.
///
/// Stronger than [`wait_converged`], which only asks whether a port is present:
/// two nodes can both serve a port while one is still on an older config for
/// it. Equal revisions is the real "these nodes agree" surface.
pub async fn wait_revisions_agree(port: u64, timeout: Duration) -> anyhow::Result<f64> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut revisions = Vec::new();
        for node in &NODES {
            match config_revision(node.metrics, port).await {
                Ok(v) => revisions.push(v),
                Err(_) => break,
            }
        }
        if revisions.len() == NODES.len() && revisions.iter().all(|v| *v == revisions[0]) {
            return Ok(revisions[0]);
        }
        if Instant::now() >= deadline {
            bail!("nodes disagree on the revision of port {port}: {revisions:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Poll Envoy's admin API until `ip` is failing its active health check — i.e.
/// the front has actually taken the backend out of rotation.
///
/// Waiting for this is not politeness: round-robin keeps offering the dead
/// backend its share of requests until the check trips, so asserting "the front
/// routes around it" any earlier measures Envoy's health-check interval rather
/// than its routing.
pub async fn wait_backend_ejected(ip: &str, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if backend_failing_health_check(ip).await.unwrap_or(false) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("envoy never marked {ip} as failing its active health check");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Whether Envoy currently reports `ip` as failing its active health check.
pub async fn backend_failing_health_check(ip: &str) -> anyhow::Result<bool> {
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{ENVOY_ADMIN_PORT}/clusters?format=json"
        ))
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .json()
        .await?;

    for cluster in body["cluster_statuses"].as_array().into_iter().flatten() {
        for host in cluster["host_statuses"].as_array().into_iter().flatten() {
            if host["address"]["socket_address"]["address"].as_str() == Some(ip) {
                // Read the active-check flag, not `eds_health_status`: the
                // latter reports what service discovery said and stays HEALTHY
                // for a backend Envoy has already stopped using.
                return Ok(host["health_status"]["failed_active_health_check"]
                    .as_bool()
                    .unwrap_or(false));
            }
        }
    }
    Ok(false)
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
