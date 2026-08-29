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
const RIFT_NETWORK: &str = "rift-cluster_rift";

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

/// Every image this tier builds for itself, as the compose files tag them.
///
/// There are two, and the difference between them is not cosmetic: the faketime
/// flavor carries an `LD_PRELOAD` that lies about the clock. Before they were
/// tagged apart they shared whatever name compose derived from the project and
/// service (`rift-cluster-rift-1`), and the ONLY thing keeping C12 from running
/// on a truthful clock — or a later scenario from inheriting a lying one — was
/// that every scenario passed `--build` and so re-tagged on the way up. That is
/// a real invariant resting on an argument nobody would think to keep.
///
/// `compose_images_are_tagged_by_flavor` pins this list against the compose
/// files, so an overlay that introduces a third flavor fails a test rather than
/// silently sharing a tag with one of these.
pub const BUILT_IMAGES: [&str; 2] = ["rift-cluster-server:local", "rift-cluster-server:faketime"];

/// Whether `cluster-smoke` has already built and loaded [`BUILT_IMAGES`].
///
/// A shard runner gets the images from the prepare job's artifact, not from a
/// build of its own (D-58), so `up` must not pass `--build`: there is no layer
/// cache behind it and it would rebuild the whole thing from cold, which is the
/// cost the prepare job exists to pay once.
fn prebuilt() -> bool {
    std::env::var_os("RIFT_CHAOS_PREBUILT_IMAGE").is_some()
}

/// Fail before the first stack if a prebuilt image was promised and is missing.
///
/// Fail-closed, and once per process. Without it a missing tag sends compose to
/// a registry for an image that only ever existed on a runner's disk, and the
/// error names a failed pull of `rift-cluster-server:local` — which reads as a
/// network problem rather than as "the prepare job did not hand this shard what
/// it said it did".
fn ensure_prebuilt_images() -> anyhow::Result<()> {
    static CHECKED: OnceLock<Result<(), String>> = OnceLock::new();
    CHECKED
        .get_or_init(|| {
            if !prebuilt() {
                return Ok(());
            }
            for image in BUILT_IMAGES {
                let present = Command::new("docker")
                    .args(["image", "inspect", image])
                    .output()
                    .is_ok_and(|out| out.status.success());
                if !present {
                    return Err(format!(
                        "RIFT_CHAOS_PREBUILT_IMAGE is set but `{image}` is not loaded. The \
                         prepare job builds and uploads it; unset the variable to build here \
                         instead."
                    ));
                }
            }
            Ok(())
        })
        .clone()
        .map_err(anyhow::Error::msg)
}

/// The `up` arguments for this run: `--build` locally, nothing in CI.
///
/// Locally `--build` is what makes an edit show up in the next `cargo test`, so
/// it stays. It is also what has always re-tagged the flavor in use — see
/// [`BUILT_IMAGES`] — which is why dropping it required tagging the flavors
/// apart first rather than as a follow-up.
fn up_args() -> &'static [&'static str] {
    if prebuilt() {
        &["up", "-d"]
    } else {
        &["up", "-d", "--build"]
    }
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
    /// When this stack was asked for, so `Drop` can report what the scenario
    /// cost end to end. See [`record`].
    created: Instant,
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
            created: Instant::now(),
        };
        ensure_prebuilt_images()?;
        compose_with(
            &[base_file(), overlay_file()],
            &["down", "-v", "--remove-orphans"],
        )
        .ok();
        wait_stack_gone();
        wait_ports_free(PORTS_FREE_TIMEOUT)?;
        let mut args = up_args().to_vec();
        args.extend(["--no-deps", name]);
        cluster.compose(&args).context("compose up single node")?;
        Ok(cluster)
    }

    async fn start_stack(files: Vec<String>) -> anyhow::Result<Self> {
        // Poisoning only means a previous scenario panicked; the stack is torn
        // down by `Drop` either way, so the lock still hands over a clean slate.
        let guard = stack_lock().lock().unwrap_or_else(|e| e.into_inner());
        let cluster = Self {
            files,
            _guard: guard,
            created: Instant::now(),
        };
        ensure_prebuilt_images()?;

        // Down first: a stack left behind by an interrupted run would otherwise
        // be silently reused, and its state dirs would make `test_cold_start`
        // pass for the wrong reason.
        //
        // Torn down with BOTH files regardless of which this stack wants, so a
        // chaos stack left behind by an interrupted run cannot survive into a
        // plain one.
        let t = Instant::now();
        compose_with(
            &[base_file(), overlay_file()],
            &["down", "-v", "--remove-orphans"],
        )
        .ok();
        wait_stack_gone();
        let t = record("down", t);
        wait_ports_free(PORTS_FREE_TIMEOUT)?;
        let t = record("ports_free", t);
        cluster.compose(up_args()).context("compose up")?;
        let t = record("up", t);

        cluster.wait_all_ready(UP_TIMEOUT).await?;
        let t = record("ready", t);
        cluster.wait_cluster_formed(UP_TIMEOUT).await?;
        record("formed", t);
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
        let t = Instant::now();
        let _ = self.compose(&["down", "-v", "--remove-orphans"]);
        record("teardown", t);
        record("total", self.created);
    }
}

/// Append `<scenario>\t<phase>\t<ms>` to `$CHAOS_TIMING_LOG`, and return the
/// instant the next phase starts from.
///
/// Unset — every local run, and every lane that has not asked for it — this is a
/// clock read and nothing else.
///
/// It exists because the per-scenario cost of this tier was, until it was
/// measured, only inferable by regressing whole-job wall clock against scenario
/// count across a month of runs. libtest buffers a piped run's output, so even
/// the per-test lines arrive in one burst at the end with no timestamps to read.
/// `cluster-smoke` renders this file as a step summary, so the next person asking
/// "which phase is the 19 s floor?" reads an answer instead of deriving one.
///
/// Best effort throughout: a timing file that cannot be written must never fail
/// a scenario, and this is called from `Drop` during unwind.
fn record(phase: &str, since: Instant) -> Instant {
    let now = Instant::now();
    if let Some(path) = std::env::var_os("CHAOS_TIMING_LOG") {
        let scenario = std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_owned();
        let line = format!(
            "{scenario}\t{phase}\t{}\n",
            now.duration_since(since).as_millis()
        );
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            use std::io::Write as _;
            let _ = f.write_all(line.as_bytes());
        }
    }
    now
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
    let mut ports = Vec::with_capacity(NODES.len() * 5 + 12);
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
    ports.extend(FLOW_STATE_HOST_PORTS);
    ports.extend(FRONT_DOOR_HOST_PORTS);
    ports.extend(SOURCES_HOST_PORTS);
    ports.extend(TENANCY_A_HOST_PORTS);
    ports.extend(TENANCY_B_HOST_PORTS);
    ports.extend(SEQUENCING_HOST_PORTS);
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
                "label=com.docker.compose.project=rift-cluster",
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
    get_json_with_key(port, path, None).await
}

/// [`get_json`] carrying an `authorization` header, for the scenarios that run
/// against a closed admin plane (C24-C27 under `tenancy.overlay.yml`).
pub async fn get_json_with_key(
    port: u16,
    path: &str,
    key: Option<&str>,
) -> anyhow::Result<(u16, serde_json::Value)> {
    let mut request = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(10));
    if let Some(key) = key {
        request = request.header("authorization", key);
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    // The status is the subject here, so a body that is not JSON (or is empty)
    // must not mask it — callers assert on the status and use the body only as
    // forensics. `imposter_ports_with_key` is where a non-2xx becomes an error.
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
    wait_admin_reachable_with_key(admin, timeout, None).await
}

/// [`wait_admin_reachable`] carrying a credential — C25's closed admin plane.
///
/// Reachability and authorization are different questions, and on a closed plane
/// the unauthenticated probe cannot tell them apart: it answers `401` from a node
/// that is perfectly reachable, and the poll then burns its whole timeout before
/// reporting what reads as a partition that did not hold.
pub async fn wait_admin_reachable_with_key(
    admin: u16,
    timeout: Duration,
    key: Option<&str>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let attempt = match get_json_with_key(admin, "/imposters", key).await {
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
    put_imposter_config(admin, &body)
        .await
        .map(|(status, _)| status)
}

/// `POST /imposters` with a config the caller built itself.
///
/// [`put_imposter`] covers the "one static stub" shape every config-plane
/// scenario needs; this one exists for the scenarios whose *config* is the
/// subject — a scripted stub, a `_rift.flowState` block — where the point is
/// exactly the fields the convenience helper does not expose.
/// Returns the status **and the body**: a config-shaped 400 carries the reason
/// in its typed error envelope, and a status alone would make a refusal
/// indistinguishable from any other refusal.
pub async fn put_imposter_config(
    admin: u16,
    config: &serde_json::Value,
) -> anyhow::Result<(u16, String)> {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{admin}/imposters"))
        .timeout(Duration::from_secs(15))
        .json(config)
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Ok((status, body))
}

/// `PUT /front-door/routes`: a whole-table replace of the front door's route
/// table, returning the status **and the body** — the same forensic shape as
/// [`put_imposter_config`], for the same reason: a config-shaped 400 carries
/// the reason in its typed error envelope, and a bare status would make a
/// refusal indistinguishable from any other refusal.
///
/// Takes a `serde_json::Value` rather than a `RouteTable`, matching every
/// other write helper in this module: `Cargo.toml` pulls in no `rift_cluster_base` /
/// `rift_http_proxy` types on purpose — this crate drives real processes over
/// plain HTTP, so a route table is built as JSON at the call site, the same
/// as an imposter config via [`put_imposter_config`].
pub async fn put_routes(admin: u16, table: &serde_json::Value) -> anyhow::Result<(u16, String)> {
    let response = reqwest::Client::new()
        .put(format!("http://127.0.0.1:{admin}/front-door/routes"))
        .timeout(Duration::from_secs(15))
        .json(table)
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Ok((status, body))
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

/// An admin-plane request carrying an API key, returning `(status, body)`.
///
/// Forensic body, not a bare status, for the same reason every other helper
/// here returns one: C24 compares whole responses across three nodes, and a
/// `403` that agrees on the status while disagreeing on the reason is exactly
/// the divergence the scenario exists to catch.
///
/// `key: None` sends no credential at all — which is a distinct case from a
/// wrong key, and the two must be distinguishable: an open plane answers the
/// first and a closed one refuses it.
pub async fn admin_with_key(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
    key: Option<&str>,
) -> anyhow::Result<(u16, serde_json::Value)> {
    admin_as(port, method, path, body, key, None).await
}

/// [`admin_with_key`] naming the tenant the caller is acting under.
///
/// `X-Rift-Tenant` **selects among the principal's existing bindings; it never
/// grants one** (RFC-002 §8.1). Sent explicitly rather than left to default,
/// because for a create it is the header that decides which tenant *acquires*
/// the resource — so a scenario that omitted it would be asserting against
/// whichever tenant the server picked, not the one it meant.
pub async fn admin_as(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
    key: Option<&str>,
    tenant: Option<&str>,
) -> anyhow::Result<(u16, serde_json::Value)> {
    let url = format!("http://127.0.0.1:{port}{path}");
    let client = reqwest::Client::new();
    let mut request = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "DELETE" => client.delete(url),
        other => anyhow::bail!("unsupported method {other:?}"),
    }
    .timeout(Duration::from_secs(30));
    if let Some(key) = key {
        request = request.header("authorization", key);
    }
    if let Some(tenant) = tenant {
        request = request.header("X-Rift-Tenant", tenant);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    // A non-JSON body surfaces as null rather than as an error: the status is
    // what the caller came for, and an unparseable body is itself a finding.
    let parsed = response.json().await.unwrap_or(serde_json::Value::Null);
    Ok((status, parsed))
}

/// Create a tenant as the fleet admin. Returns `(status, body)`.
pub async fn create_tenant(
    admin: u16,
    tenant: &str,
    key: &str,
) -> anyhow::Result<(u16, serde_json::Value)> {
    admin_with_key(
        admin,
        "POST",
        "/admin/tenants",
        Some(&serde_json::json!({ "id": tenant, "displayName": tenant })),
        Some(key),
    )
    .await
}

/// Mint a principal bound to `tenant` with `role`, returning `(id, raw key)`.
///
/// `role` is the **kebab-case** wire form (`"viewer"`, `"operator"`, `"editor"`,
/// `"tenant-admin"`) — `Role`'s serde representation, not its Rust spelling.
///
/// The id is returned alongside the key because revoking a binding addresses
/// the principal by id (`DELETE /admin/tenants/:t/bindings/:pid`), and the id is
/// derived from the key rather than chosen, so a caller cannot reconstruct it.
///
/// The key is returned in this one response and never again (RFC-002 §5 shows
/// it once), so a scenario that drops it cannot recover it — hence returning it
/// rather than the whole body.
pub async fn mint_principal(
    admin: u16,
    tenant: &str,
    role: &str,
    key: &str,
) -> anyhow::Result<(String, String)> {
    let (status, body) = admin_with_key(
        admin,
        "POST",
        &format!("/admin/tenants/{tenant}/principals"),
        Some(&serde_json::json!({ "displayName": format!("{tenant}-{role}"), "role": role })),
        Some(key),
    )
    .await?;
    anyhow::ensure!(
        status == 201,
        "minting a {role} in {tenant} failed: {status} {body}"
    );
    // `apiKey`, not `key` — the field name is the contract `tenancy_api.rs`
    // already asserts in process, and reading the wrong one here would fail as
    // "no raw key" rather than as the typo it is.
    let id = body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("the mint response carried no id: {body}"))?;
    let raw = body
        .get("apiKey")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("the mint response carried no raw key: {body}"))?;
    Ok((id, raw))
}

/// The one imposter data port the `pull-on-miss` overlay publishes, and the
/// host ports it appears on — indexed like [`NODES`]. C16 only.
///
/// Published on every node because C16 picks its lagging node at run time: the
/// node that lags must be a follower, and leadership is not a scenario's to
/// assume.
pub const PULL_ON_MISS_IMPOSTER_PORT: u16 = 6300;
pub const PULL_ON_MISS_HOST_PORTS: [u16; 3] = [16300, 26300, 36300];

/// The flow-state scenario's imposter port, and the host ports
/// `flow-state.overlay.yml` publishes it on — one per node, in `NODES` order,
/// so a scenario can round-robin its data-plane requests across the fleet the
/// way a load balancer would.
pub const FLOW_STATE_IMPOSTER_PORT: u16 = 6400;
pub const FLOW_STATE_HOST_PORTS: [u16; 3] = [16400, 26400, 36400];

/// C33's imposter data port, and the host ports `sequencing.overlay.yml`
/// publishes it on — one per node, in [`NODES`] order, so the scenario can
/// round-robin its data-plane requests across the fleet the way a load balancer
/// would.
///
/// Sequencing needs the *body* and the `rift-cluster-sequence` header, neither
/// of which [`exec_probe`] can return, and it needs all three nodes because
/// "one cursor, not three" is only falsifiable when the requests are spread.
///
/// 36700 is inside Linux's ephemeral range (32768-60999) and is reserved in
/// both workflows alongside 36300/36400/36500-36501; the other two sit below it
/// — see the overlay's header and #117.
pub const SEQUENCING_IMPOSTER_PORT: u16 = 6700;
pub const SEQUENCING_HOST_PORTS: [u16; 3] = [16700, 26700, 36700];

/// C27's two imposter data ports — one per tenant — and the host ports
/// `tenancy.overlay.yml` publishes them on, indexed like [`NODES`].
///
/// Two tenants, two ports, published on every node, because C27's claim is that
/// tenancy isolates *ownership* and not the data plane: both imposters must
/// answer unauthenticated traffic through any node. One port would prove only
/// that one tenant's imposter serves; one node would prove nothing about the
/// fleet.
///
/// Both sit below Linux's ephemeral range (32768-60999), so unlike
/// [`FLOW_STATE_HOST_PORTS`]'s 36400 they need no `ip_local_reserved_ports`
/// entry — see #117 and the overlay's header.
pub const TENANCY_A_IMPOSTER_PORT: u16 = 6500;
pub const TENANCY_A_HOST_PORTS: [u16; 3] = [16500, 26500, 36500];
pub const TENANCY_B_IMPOSTER_PORT: u16 = 6501;
pub const TENANCY_B_HOST_PORTS: [u16; 3] = [16501, 26501, 36501];

/// The fleet-admin credential `tenancy.overlay.yml` boots the fleet with.
///
/// A constant rather than a generated value: it is set in the overlay, so the
/// scenario and the compose file have to agree on it, and a literal that
/// appears in both is easier to keep true than a value threaded through the
/// environment.
pub const TENANCY_FLEET_KEY: &str = "chaos-fleet-admin-key";

/// The front door's host ports under `front-door.overlay.yml` — one per node,
/// in `NODES` order. C17 and C18 only: no other scenario binds `--front-door`.
pub const FRONT_DOOR_HOST_PORTS: [u16; 3] = [12527, 22527, 32527];

/// Each node's **cluster port** as `sources.overlay.yml` publishes it, in
/// [`NODES`] order. C20-C23 only.
///
/// Published because `/admin/sources*` rides the cluster port, not the admin
/// API: sources are a control-plane object authenticated with the cluster
/// credential (see `crates/rift-cluster/src/sources/mod.rs`'s module doc). The
/// shipped topology deliberately does not publish 4790 — peers reach it over
/// the container network and an operator reaches it from inside the perimeter —
/// so this is scoped to the one overlay whose scenarios need it and is not a
/// change to the reference deployment.
///
/// All three nodes, not one, and that is the point rather than a convenience:
/// C20's barrier and C22's post-restart checks both assert on **every** node's
/// own applied state, which is the only way "the fleet converged" differs from
/// "the node we wrote through converged".
pub const SOURCES_CLUSTER_HOST_PORTS: [u16; 3] = [14790, 24790, 34790];

/// The counting origin's admin API, as `sources.overlay.yml` publishes it.
///
/// The origin is a fourth `rift-cluster-server`, run **un-clustered**, whose imposter
/// serves the very config documents the fleet fetches. Its Mountebank-compatible
/// admin API therefore hands the harness an exact fetch counter for free:
/// `GET /imposters/:port` reports `numberOfRequests`. That is what turns
/// "fetched once fleet-wide" into an equality against a first-class API value
/// instead of a log scrape — and it is why the origin is another rift container
/// rather than a new image: the chaos tier pins images by digest, and inventing
/// one for a static-file server would be a new supply-chain surface to serve a
/// counter this build already publishes.
pub const SOURCES_ORIGIN_ADMIN_PORT: u16 = 46525;

/// Every host port `sources.overlay.yml` publishes, as one list — what
/// [`published_host_ports`] extends itself with.
///
/// Derived from the two constants above rather than written out again, for the
/// reason `published_host_ports` gives: a port list maintained in two places is
/// a port list that will disagree with itself.
///
/// **34790 and 46525 are inside** Linux's ephemeral source-port range
/// (32768-60999), so both are reserved by the `ip_local_reserved_ports` step
/// `ci.yml` and `nightly-chaos.yml` run;
/// `ci_reserves_every_published_port_that_linux_could_hand_out` fails the build
/// if that is ever forgotten. 14790 and 24790 sit below the range and need
/// nothing.
pub const SOURCES_HOST_PORTS: [u16; 4] = [
    SOURCES_CLUSTER_HOST_PORTS[0],
    SOURCES_CLUSTER_HOST_PORTS[1],
    SOURCES_CLUSTER_HOST_PORTS[2],
    SOURCES_ORIGIN_ADMIN_PORT,
];

/// The imposter port the counting origin serves its config documents on.
///
/// Clear of every other scenario's range (C17/C18 own 6500-6512), because a
/// shared data port would couple two scenarios through the one thing this
/// scenario counts.
pub const SOURCES_ORIGIN_IMPOSTER_PORT: u16 = 6600;

/// The origin as the *fleet* reaches it — a container-network name, resolved by
/// Docker's embedded DNS, never a host address. A source URI is replicated
/// state: every node has to be able to fetch it, so it cannot name anything
/// that is only meaningful from the harness's side of the network.
pub const SOURCES_ORIGIN_BASE_URL: &str = "http://source-origin:6600";

/// The cluster secret `deploy/compose/docker-compose.yml` sets on every node.
///
/// Inline here for the same reason it is inline there: this is the local
/// development topology, and the harness has to present the same credential the
/// nodes verify against. A real deployment injects it from a secret store.
const CLUSTER_SECRET: &str = "local-development-cluster-secret";

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
    get_data_plane_with(host_port, path, &[]).await
}

/// [`get_data_plane`] carrying request headers.
///
/// Flow state needs it: an imposter with `flowIdSource: "header:<Name>"` keys
/// its state off that header, so driving several *distinct* flows through one
/// imposter — the only way to prove per-flow isolation survives a restart —
/// means setting it per request.
pub async fn get_data_plane_with(
    host_port: u16,
    path: &str,
    headers: &[(&str, &str)],
) -> anyhow::Result<(u16, reqwest::header::HeaderMap, String)> {
    let mut request = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{host_port}{path}"))
        // Comfortably past the hook's own 500 ms budget, so a scenario failure
        // reads as "not rescued" rather than as the client giving up first.
        .timeout(Duration::from_secs(10));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    let response_headers = response.headers().clone();
    let body = response.text().await.unwrap_or_default();
    Ok((status, response_headers, body))
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
            "rift-cluster-server",
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
    imposter_ports_with_key(admin, None).await
}

/// [`imposter_ports`] carrying a credential, for the scenarios that run against
/// a *closed* admin plane (C24-C27 under `tenancy.overlay.yml`).
///
/// **Why the status is checked rather than the body simply parsed.** Without the
/// check a `401` body has no `imposters` array, so it read as "this node has no
/// imposters" — and `wait_converged` then reported `reached only 0/3 nodes`, a
/// convergence failure, for what was actually a missing credential. That cost a
/// full container run to diagnose. An unauthorized read is not an empty read,
/// and the two must not be spelled the same way.
pub async fn imposter_ports_with_key(admin: u16, key: Option<&str>) -> anyhow::Result<Vec<u64>> {
    let mut request = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{admin}/imposters"))
        .timeout(Duration::from_secs(10));
    if let Some(key) = key {
        request = request.header("authorization", key);
    }
    let response = request.send().await?;
    let status = response.status();
    let body: serde_json::Value = response.json().await?;
    if !status.is_success() {
        bail!("GET /imposters on {admin} answered {status}: {body}");
    }
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

/// [`wait_converged`] carrying a credential — C24-C27's closed admin plane.
///
/// **Only observes the credential's own tenant.** These wrappers poll `GET /imposters`, and since
/// issue #182 that listing is filtered to the caller's tenant — so waiting here for a port owned by
/// a *different* tenant does not fail, it hangs until the timeout, which reads as "the fleet never
/// converged" rather than "you asked the wrong tenant". A tenant-owned port wants a per-port poll
/// with an explicit `X-Rift-Tenant` instead; see `wait_imposter_visible_as` in `scenarios.rs`.
pub async fn wait_converged_with_key(
    port: u64,
    timeout: Duration,
    key: &str,
) -> anyhow::Result<()> {
    wait_converged_on_with_key(&NODES.iter().collect::<Vec<_>>(), port, timeout, Some(key)).await
}

/// [`wait_converged`], restricted to a named subset — for scenarios where some
/// node is deliberately down.
pub async fn wait_converged_on(
    nodes: &[&Node],
    port: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    wait_converged_on_with_key(nodes, port, timeout, None).await
}

/// The one implementation behind the three wrappers above.
///
/// The last read error is carried into the timeout message. Polling must treat
/// an error as "not yet" — a node that is still starting legitimately refuses —
/// but discarding it entirely is what made the missing-credential failure above
/// present as a bare `0/3 nodes` with nothing to act on.
pub async fn wait_converged_on_with_key(
    nodes: &[&Node],
    port: u64,
    timeout: Duration,
    key: Option<&str>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    loop {
        let mut seen = 0;
        for node in nodes {
            match imposter_ports_with_key(node.admin, key).await {
                Ok(ports) if ports.contains(&port) => seen += 1,
                Ok(_) => {}
                Err(e) => last_error = Some(format!("{}: {e}", node.name)),
            }
        }
        if seen == nodes.len() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let detail = last_error.map_or_else(String::new, |e| format!(" (last error: {e})"));
            bail!(
                "imposter {port} reached only {seen}/{} nodes{detail}",
                nodes.len()
            );
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

// ---------------------------------------------------------------------------
// The cluster port, as an operator reaches it (C20-C23)
// ---------------------------------------------------------------------------

/// A client for the **cluster port**, signed with the cluster credential.
///
/// Every other helper in this module talks plain HTTP, because every other
/// surface it drives is plain HTTP. The cluster port is not: it is
/// HMAC-authenticated per request (`x-rift-cluster-auth`, RFC-001 §11.2) with a
/// length-prefixed canonical form over method, path and body, and it negotiates
/// a protocol version. So this one helper reaches for `rift_cluster`'s own
/// `RpcClient` rather than hand-rolling that framing here — a second
/// implementation of a security-critical wire format is not a thing to keep in
/// a test harness, and the shipped client is exactly what an operator's tooling
/// would use.
///
/// **`max_retries: 0` is load-bearing, not tidiness.** The default client
/// retries transient failures three times, and a retried `POST
/// /admin/sources/:id/pull` fetches the source again — which would quietly turn
/// C20's `== 1` counter equality into whatever the transport happened to do.
/// A pull that times out must surface as a failure the scenario reports, never
/// as a second fetch nobody asked for.
///
/// The timeout is likewise raised well above the 2s default: a pull does a real
/// network fetch and then a Raft round trip, and 2s would make a healthy fleet
/// look broken.
fn cluster_client() -> rift_cluster::rpc::RpcClient {
    rift_cluster::rpc::RpcClient::new(
        Some(rift_cluster::rpc::Signer::new(CLUSTER_SECRET)),
        std::sync::Arc::new(rift_cluster::rpc::AlwaysHealthy),
        rift_cluster::rpc::RpcClientConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 0,
        },
    )
}

/// Call a cluster-port endpoint on one node, returning `(status, body)`.
///
/// Forensic by construction, like [`put_imposter_config`]: a refusal on this
/// surface carries its reason in a typed error envelope, and a bare status
/// would make "this source declares an intercept block" indistinguishable from
/// "unknown source". A failure is rendered as its HTTP status plus the message,
/// so an assertion that trips prints what the node actually said.
///
/// # Errors
/// Only if the host port cannot be parsed as an address — every transport and
/// handler failure is reported through the returned status instead, so a
/// scenario asserts on it rather than unwrapping past it.
pub async fn cluster_api(
    host_port: u16,
    method: &str,
    path: &str,
    body: &serde_json::Value,
) -> anyhow::Result<(u16, serde_json::Value)> {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{host_port}")
        .parse()
        .context("cluster-port address")?;
    let payload = if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(body).context("encode cluster-port request body")?
    };
    match cluster_client().call(addr, method, path, payload).await {
        Ok(raw) => {
            // A 2xx whose body is not JSON is itself a finding, so it surfaces
            // as null rather than as an error that would mask the status.
            let parsed = serde_json::from_slice(&raw).unwrap_or(serde_json::Value::Null);
            Ok((200, parsed))
        }
        Err(e) => Ok((
            e.status(),
            serde_json::json!({ "error": e.reason(), "message": e.to_string() }),
        )),
    }
}

/// Poll until a node's cluster port answers `GET /admin/sources`.
///
/// Readiness on the probe port says the *node* is up; it says nothing about
/// whether the source puller has been bound to it yet, and an unbound puller
/// answers "cluster node is not available yet". Polling here means a scenario's
/// first source call is not racing composition order.
pub async fn wait_sources_reachable(host_port: u16, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let attempt =
            match cluster_api(host_port, "GET", "/admin/sources", &serde_json::Value::Null).await {
                Ok((200, _)) => return Ok(()),
                Ok((status, body)) => format!("status {status}: {body}"),
                Err(e) => e.to_string(),
            };
        if Instant::now() >= deadline {
            bail!("cluster port :{host_port} never served /admin/sources ({attempt})");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Declare a source on one node, returning `(status, body)`.
pub async fn declare_source(
    host_port: u16,
    source: &serde_json::Value,
) -> anyhow::Result<(u16, serde_json::Value)> {
    cluster_api(host_port, "POST", "/admin/sources", source).await
}

/// `POST /admin/sources/:id/pull` on one node, returning `(status, body)`.
pub async fn pull_source(host_port: u16, id: &str) -> anyhow::Result<(u16, serde_json::Value)> {
    cluster_api(
        host_port,
        "POST",
        &format!("/admin/sources/{id}/pull"),
        &serde_json::Value::Null,
    )
    .await
}

/// `GET /admin/sources/:id` on one node, returning `(status, body)`.
pub async fn read_source(host_port: u16, id: &str) -> anyhow::Result<(u16, serde_json::Value)> {
    cluster_api(
        host_port,
        "GET",
        &format!("/admin/sources/{id}"),
        &serde_json::Value::Null,
    )
    .await
}

/// `GET /_cluster/config` on one node — the ports it has a committed config
/// for, and the source provenance stamped on each. Returns `(status, body)`.
pub async fn cluster_config(host_port: u16) -> anyhow::Result<(u16, serde_json::Value)> {
    cluster_api(
        host_port,
        "GET",
        "/_cluster/config",
        &serde_json::Value::Null,
    )
    .await
}

/// `GET /_cluster/imposters` on one node — every port it has a committed config
/// for, with the config body. Returns `(status, body)`.
///
/// The only way to read what the fleet actually *holds* for a port when the
/// overlay publishes no imposter data port. C23 needs it: "the pull overwrote
/// the hand edit" is a claim about committed content, and the admin API's
/// `/imposters` listing answers with what this node's engine has bound, which
/// is a different question.
pub async fn cluster_imposters(host_port: u16) -> anyhow::Result<(u16, serde_json::Value)> {
    cluster_api(
        host_port,
        "GET",
        "/_cluster/imposters",
        &serde_json::Value::Null,
    )
    .await
}

/// The committed config body for `port`, from a `/_cluster/imposters` body.
#[must_use]
pub fn committed_config(imposters: &serde_json::Value, port: u16) -> Option<serde_json::Value> {
    imposters["imposters"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["port"].as_u64() == Some(u64::from(port)))
        .map(|entry| entry["config"].clone())
}

/// The source id and version stamped on `port`, from a `/_cluster/config` body.
///
/// `None` when that node holds no config for the port at all, which a caller
/// must be able to tell apart from "holds it, unstamped" — a hand-written
/// imposter has no provenance and that is not the same finding.
#[must_use]
pub fn provenance_of(config: &serde_json::Value, port: u16) -> Option<(String, Option<String>)> {
    config["provenance"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["port"].as_u64() == Some(u64::from(port)))
        .map(|entry| {
            (
                entry["sourceId"].as_str().unwrap_or_default().to_owned(),
                entry["version"].as_str().map(str::to_owned),
            )
        })
}

// ---------------------------------------------------------------------------
// The counting origin (C20-C23)
// ---------------------------------------------------------------------------

/// Publish a set of `(path, document)` pairs on the origin, creating its
/// imposter. Returns `(status, body)`.
///
/// One imposter with a stub per path rather than one imposter per document: the
/// request counter is per imposter, and C20's whole assertion is a count, so
/// keeping every document behind one counter is what makes "the fleet fetched
/// exactly once" a single number rather than a sum a scenario could get wrong.
///
/// The document is serialised into the response body as a **string**. A JSON
/// object would be re-encoded by the origin on its way out, and this body is
/// then hashed by the fetching node into the digest the no-change short circuit
/// compares — so it has to be exactly the bytes the scenario intended.
pub async fn origin_publish(
    documents: &[(&str, serde_json::Value)],
) -> anyhow::Result<(u16, String)> {
    let config = serde_json::json!({
        "port": SOURCES_ORIGIN_IMPOSTER_PORT,
        "protocol": "http",
        "name": "source-origin",
        "stubs": origin_stubs(documents)?,
    });
    put_imposter_config(SOURCES_ORIGIN_ADMIN_PORT, &config).await
}

/// Replace what the origin serves, **without** replacing the imposter.
///
/// `PUT /imposters/:port/stubs` rather than a delete-and-recreate, deliberately:
/// recreating the imposter would reset `numberOfRequests`, and a scenario that
/// changes the document mid-run (C21's content change, C23's second pull) still
/// needs the counter to be continuous across that change.
pub async fn origin_republish(
    documents: &[(&str, serde_json::Value)],
) -> anyhow::Result<(u16, String)> {
    let stubs = origin_stubs(documents)?;
    let response = reqwest::Client::new()
        .put(format!(
            "http://127.0.0.1:{SOURCES_ORIGIN_ADMIN_PORT}/imposters/{SOURCES_ORIGIN_IMPOSTER_PORT}/stubs"
        ))
        .timeout(Duration::from_secs(15))
        .json(&serde_json::json!({ "stubs": stubs }))
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Ok((status, body))
}

/// One path-predicated stub per document.
fn origin_stubs(documents: &[(&str, serde_json::Value)]) -> anyhow::Result<serde_json::Value> {
    let mut stubs = Vec::with_capacity(documents.len());
    for (path, document) in documents {
        let encoded = serde_json::to_string(document).context("encode a source document")?;
        stubs.push(serde_json::json!({
            "predicates": [{ "equals": { "path": path } }],
            "responses": [{ "is": {
                "statusCode": 200,
                // No `ETag`, and that is the point: upstream's `HttpSource`
                // sends `If-None-Match` whenever it has one cached, and a 304
                // would let a node answer from its own cache instead of
                // reaching the origin — which is precisely the request C20
                // counts.
                "headers": { "Content-Type": "application/json" },
                "body": encoded,
            } }]
        }));
    }
    Ok(serde_json::Value::Array(stubs))
}

/// How many requests the origin's document imposter has served, ever.
///
/// The count is maintained whether or not request *recording* is on (upstream's
/// `note_request_counts_even_when_recording_off`), so nothing here depends on a
/// journal being configured. Scenarios assert on the **delta** across an action
/// rather than on the absolute value: that is immune both to whatever a stack
/// did before the measurement and to whether
/// `DELETE /imposters/:port/savedRequests` (which does reset it) was reached.
pub async fn origin_request_count() -> anyhow::Result<u64> {
    let (status, body) = get_json(
        SOURCES_ORIGIN_ADMIN_PORT,
        &format!("/imposters/{SOURCES_ORIGIN_IMPOSTER_PORT}"),
    )
    .await?;
    if status != 200 {
        bail!("origin admin answered {status} for its document imposter: {body}");
    }
    body["numberOfRequests"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("origin reported no numberOfRequests: {body}"))
}

/// Poll until the origin's admin API answers, so a scenario's first publish is
/// not racing a container that is still starting.
pub async fn wait_origin_ready(timeout: Duration) -> anyhow::Result<()> {
    wait_admin_reachable(SOURCES_ORIGIN_ADMIN_PORT, timeout).await
}

/// A source document declaring one imposter that answers every request with
/// `body` — the smallest thing whose content is visible fleet-wide.
#[must_use]
pub fn source_document(ports_and_bodies: &[(u16, &str)]) -> serde_json::Value {
    let imposters: Vec<serde_json::Value> = ports_and_bodies
        .iter()
        .map(|(port, body)| {
            serde_json::json!({
                "port": port,
                "protocol": "http",
                "name": body,
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": body } }] }],
            })
        })
        .collect();
    serde_json::json!({ "imposters": imposters })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every compose file this tier composes, base and overlays alike.
    fn compose_sources() -> Vec<(String, String)> {
        let root = env!("CARGO_MANIFEST_DIR");
        let mut files = vec![(
            "deploy/compose/docker-compose.yml".to_owned(),
            std::fs::read_to_string(base_file()).expect("read the base compose file"),
        )];
        let overlays = std::fs::read_dir(format!("{root}/compose")).expect("read compose/");
        for entry in overlays {
            let path = entry.expect("read a compose/ entry").path();
            if path.extension().is_some_and(|e| e == "yml") {
                let name = path.file_name().expect("a file name").to_string_lossy();
                let body = std::fs::read_to_string(&path).expect("read an overlay");
                files.push((format!("tests/cluster-chaos/compose/{name}"), body));
            }
        }
        files
    }

    /// Collect the values of a `key:` across a compose file, ignoring comments.
    ///
    /// Line-oriented on purpose: pulling a YAML parser into this crate to read
    /// two keys would be a dependency taken on for a guard, and the guard does
    /// not need structure — it needs the two flat sets below.
    fn values_of(key: &str, body: &str) -> Vec<String> {
        body.lines()
            .map(str::trim)
            .filter(|l| !l.starts_with('#'))
            .filter_map(|l| l.strip_prefix(key))
            .map(|v| v.trim().trim_matches('"').to_owned())
            .collect()
    }

    /// The default a `${VAR:-default}` resolves to, which is what a run without
    /// an override uses and what the prebuild tags.
    fn resolved(image: &str) -> &str {
        image
            .rsplit_once(":-")
            .map_or(image, |(_, d)| d.trim_end_matches('}'))
    }

    /// A build target and the tag it lands on must be introduced together.
    ///
    /// This is the guard for what [`BUILT_IMAGES`] documents. It does not parse
    /// the compose graph — it compares two flat sets — but the two failures it
    /// catches are the two that matter, and both have precedent here:
    ///
    /// 1. A new `build.target` with no tag of its own. Until D-58 the faketime
    ///    flavor was exactly this: it shared the production tag, and what kept
    ///    the bytes matching the overlay was `--build` running before every
    ///    `up`. `cluster-smoke` no longer passes `--build`, so a repeat would
    ///    mean a scenario silently running the wrong flavor.
    /// 2. A tag the prebuild does not produce. `ensure_prebuilt_images` asserts
    ///    `BUILT_IMAGES` is loaded; a compose file naming some other
    ///    `rift-cluster-server:` tag would pass that check and then fail at
    ///    `up`, as a registry pull for an image that never left a runner.
    #[test]
    fn compose_images_are_tagged_by_flavor() {
        let mut targets = std::collections::BTreeSet::new();
        let mut tags = std::collections::BTreeSet::new();

        for (name, body) in compose_sources() {
            targets.extend(values_of("target:", &body));
            for image in values_of("image:", &body) {
                let tag = resolved(&image);
                if tag.starts_with("rift-cluster-server:") {
                    assert!(
                        BUILT_IMAGES.contains(&tag),
                        "{name} names `{tag}`, which is not in BUILT_IMAGES, so the prebuild \
                         never produces it and `up` would try to pull it"
                    );
                    tags.insert(tag.to_owned());
                }
            }
        }

        assert_eq!(
            targets.len(),
            BUILT_IMAGES.len(),
            "the compose files build {} distinct targets ({targets:?}) but BUILT_IMAGES has {}. \
             A new build target needs a tag of its own, or it shares one with another flavor and \
             whichever built last wins.",
            targets.len(),
            BUILT_IMAGES.len()
        );
        assert_eq!(
            tags.len(),
            BUILT_IMAGES.len(),
            "only {tags:?} are tagged across the compose files, but BUILT_IMAGES declares \
             {BUILT_IMAGES:?}. An image nothing names is one the prebuild wastes a build on."
        );
    }

    /// `--build` locally, never under a prebuilt image.
    ///
    /// The two halves of D-58's trade: a local `cargo test` still picks up a
    /// working-tree edit, and a shard never rebuilds from cold behind the
    /// prepare job's back.
    #[test]
    fn up_args_drop_the_build_flag_only_when_prebuilt() {
        // Asserts the mapping against whatever the environment says rather than
        // setting the variable, which is process-wide and would race any test
        // running beside it.
        let args = up_args();
        assert_eq!(args.contains(&"--build"), !prebuilt());
        assert!(args.starts_with(&["up", "-d"]));
    }
}
