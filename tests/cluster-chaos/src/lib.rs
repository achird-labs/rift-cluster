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
/// One, as of #552. There were two: the second was a `faketime` flavor carrying
/// an `LD_PRELOAD` that lies about the clock, built for the clock-skew scenario
/// that proved journal clears consulted no timestamp. That scenario left with
/// the clear generations it exercised (D-74), and nothing else ever wanted a
/// lying clock, so the flavor went with it.
///
/// The list stays a list, and `compose_images_are_tagged_by_flavor` stays,
/// because the invariant it encodes outlived the flavor: before D-58 the two
/// flavors shared whatever name compose derived from the project and service
/// (`rift-cluster-rift-1`), and the ONLY thing keeping a scenario from
/// inheriting the wrong one was that every `up` passed `--build` and so
/// re-tagged on the way. A second flavor arriving again must declare its tag
/// here, not rediscover that.
pub const BUILT_IMAGES: [&str; 1] = ["rift-cluster-server:local"];

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
        // The base topology is all this composes (`--no-deps`, one service), so
        // it is all this waits on — same rule as `start_stack`.
        wait_ports_free_for(&[base_file()], PORTS_FREE_TIMEOUT)?;
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
        // Exactly what *this* stack publishes, and nothing else (#580). A port an
        // overlay this scenario never loads cannot collide with it, so waiting on
        // one can only stall it — and be reported against it. The teardown's own
        // files are deliberately NOT added: `down` removes containers, which does
        // not make their ports relevant to the `up` about to happen, and
        // `wait_stack_gone` above already waits for every project container to go.
        wait_ports_free_for(&cluster.files, PORTS_FREE_TIMEOUT)?;
        let t = record("ports_free", t);
        cluster.compose(up_args()).context("compose up")?;
        let t = record("up", t);

        cluster.wait_all_ready(UP_TIMEOUT).await?;
        let t = record("ready", t);
        cluster.wait_cluster_formed(UP_TIMEOUT).await?;
        let t = record("formed", t);
        // The front (Envoy) is a separate container with its own startup; the
        // fleet being formed says nothing about whether it has bound its
        // listener yet. With live readiness (#548) a scenario can reach its
        // first write through the front ~300 ms after the nodes come up, which
        // is faster than Envoy starts — the write then dies with "connection
        // reset by peer" on :42525. Wait for Envoy's own readiness before
        // handing the stack over, so the scenario measures Rift, not Envoy.
        if cluster.files.iter().any(|f| f.contains("chaos.overlay")) {
            cluster.wait_front_ready(UP_TIMEOUT).await?;
            record("front", t);
        }
        Ok(cluster)
    }

    /// Wait until the front (Envoy, `chaos.overlay.yml`) has bound its listeners.
    ///
    /// Envoy's admin `/ready` answers 200 only once every listener is
    /// accepting, which is the fact a scenario's first write through
    /// `FRONT_PORT` depends on. Polled because container start is asynchronous
    /// with respect to the fleet's own readiness.
    pub async fn wait_front_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if probe(ENVOY_ADMIN_PORT, "/ready")
                .await
                .is_ok_and(|s| s == 200)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let _ = self.compose(&["ps"]);
                bail!(
                    "the front never reported ready on :{ENVOY_ADMIN_PORT}/ready within {timeout:?}"
                );
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Wait until the fleet has actually formed a cluster, not merely started.
    ///
    /// `/readyz` going 200 on all three is necessary but not sufficient: a node
    /// that founded solo is ready before the others have joined and been
    /// promoted, so a scenario that begins asserting on that window reads a
    /// legitimate promotion as a membership change -- which is how C6 failed
    /// before this existed, and it would have been reported as the product
    /// flapping. Formed means every node lists a full voter set and every node
    /// names the same, non-null `current_leader` on `/_fleet/members` — the
    /// shape `deploy/compose/smoke.sh` asserts (#555).
    pub async fn wait_cluster_formed(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut voters_ok = 0;
            let mut leaders: Vec<Option<String>> = Vec::new();
            for node in &NODES {
                if let Ok(body) = fleet_members(node.admin).await {
                    if voter_count(&body) == Some(NODES.len()) {
                        voters_ok += 1;
                    }
                    leaders.push(body["current_leader"].as_str().map(str::to_owned));
                }
            }
            let agreed = leaders.len() == NODES.len()
                && leaders[0].is_some()
                && leaders.iter().all(|leader| *leader == leaders[0]);
            if voters_ok == NODES.len() && agreed {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "fleet started but never formed a cluster: {voters_ok}/{} nodes see a \
                     full voter set, leaders named: {leaders:?}",
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
    if let Some(path) = std::env::var_os(TIMING_LOG_ENV) {
        let line = format!(
            "{}\t{phase}\t{}\n",
            scenario_name(),
            now.duration_since(since).as_millis()
        );
        let _ = append_line(std::path::Path::new(&path), &line);
    }
    now
}

/// The environment variable `cluster-smoke` sets to collect per-phase timings.
const TIMING_LOG_ENV: &str = "CHAOS_TIMING_LOG";

/// The environment variable `cluster-smoke` sets to collect scenario artifacts.
const ARTIFACT_LOG_ENV: &str = "CHAOS_ARTIFACT_LOG";

/// The name of the scenario currently running, as libtest names its thread.
///
/// `--test-threads=1` does not change this: libtest still runs each test on its
/// own named thread, so this is the test's name in both lanes.
fn scenario_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_owned()
}

/// Best-effort append of one already-formatted line to a TSV.
///
/// Shared by the two collectors above and below so "append a line" has one
/// definition, and so the row formats are testable against a real file without
/// touching the process-global environment they are reached through in a run.
fn append_line(path: &std::path::Path, line: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(line.as_bytes())
}

/// One artifact row as it lands in `$CHAOS_ARTIFACT_LOG`.
///
/// Tabs and newlines are collapsed to spaces because the file is a TSV read one
/// row per artifact. The artifact lines are assembled from multi-line format
/// strings held together by `\` continuations, so a real newline is one dropped
/// backslash away — and it would not fail anything, it would quietly split one
/// measurement across two rows and mislabel the second with the first's
/// scenario. Sanitizing here is cheaper than a parser that tolerates it.
fn artifact_row(scenario: &str, text: &str) -> String {
    let flat: String = text
        .chars()
        .map(|c| if c == '\t' || c == '\n' { ' ' } else { c })
        .collect();
    format!("{scenario}\t{flat}\n")
}

/// Record a scenario's measured figure — Ch. 12's "printed as the run's
/// artifact" contract, made true on a **passing** run.
///
/// Implements D-67: a scenario that measures a figure records it here, and a
/// bare `println!` for a measured figure is a defect.
///
/// Prints it, and — when `$CHAOS_ARTIFACT_LOG` is set — appends it to that file
/// as `<scenario>\t<text>`.
///
/// Both halves are needed and neither subsumes the other. `println!` alone is
/// what this was before: libtest captures a passing test's stdout, so the figure
/// reached a human only when the scenario **failed**, which is exactly when it is
/// least useful because the run aborted before settling the measurement.
/// `cluster-smoke` passes `--nocapture` so the line lands in the log beside its
/// scenario; the file is what makes the same number greppable *across* runs, so a
/// bound drifting inside its own ceiling — duplicate upstream calls creeping 2 → 4
/// while still under C10's assertion — is visible before it crosses. A
/// thirty-minute log is not a place a trend can be read.
///
/// Best effort on the file, like [`record`]: an artifact log that cannot be
/// written must never fail a scenario. Losing the row costs a datum; failing the
/// scenario costs the run.
pub fn record_artifact(text: &str) {
    println!("{text}");
    if let Some(path) = std::env::var_os(ARTIFACT_LOG_ENV) {
        let _ = append_line(
            std::path::Path::new(&path),
            &artifact_row(&scenario_name(), text),
        );
    }
}

/// Print and record a scenario's measured figure. See [`record_artifact`].
///
/// Takes `println!`'s arguments so a call site reads as the `println!` it
/// replaces, and so nothing is tempted back to a bare print that no file sees.
#[macro_export]
macro_rules! chaos_artifact {
    ($($arg:tt)*) => {
        $crate::record_artifact(&format!($($arg)*))
    };
}

impl Cluster {
    /// Write `compose ps` and `compose logs` to `$CHAOS_LOG_DIR` before
    /// teardown. Named for the failing test, so a matrix job's artifact says
    /// which scenario produced it.
    fn dump_logs(&self, dir: &str) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir).context("create log dir")?;
        let path = std::path::Path::new(dir).join(format!("{}.log", scenario_name()));

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
    ports.push(PROXY_ORIGIN_ADMIN_PORT);
    ports.extend(SEQUENCING_HOST_PORTS);
    ports
}

/// The barrier over an explicit port list — for a test that owns its own port.
///
/// Production goes through [`wait_ports_free_for`], which is where the barrier's
/// rationale lives. Ports supplied here carry no owner, so a bail names them
/// bare.
///
/// Exists so the barrier can be tested against a port the test itself owns: a
/// test that waited on the whole published set would fail on any machine with
/// the `deploy/compose` demo stack up, which is a false alarm about the
/// developer's machine rather than a fact about the barrier.
pub fn wait_ports_free_in(ports: &[u16], timeout: Duration) -> anyhow::Result<()> {
    let anonymous: Vec<(u16, String)> = ports.iter().map(|port| (*port, String::new())).collect();
    wait_ports_free_owned(&anonymous, timeout)
}

/// The host ports each of `files` publishes, paired with the file that publishes
/// it — the set a stack composed of exactly those files can actually collide on.
///
/// Read out of the compose files rather than declared in a table beside them.
/// A hand-written overlay→ports map is a second source of truth that agrees with
/// itself: an overlay gaining a `ports:` entry would be waited on by nobody, and
/// nothing would say so.
#[must_use]
pub fn published_ports_of(files: &[String]) -> Vec<(u16, String)> {
    let mut owners = Vec::new();
    for path in files {
        // Panics rather than skipping, matching this crate's existing compose
        // reads. Every path here is a repo file named by `base_file()` or
        // `compose_file()`, so an unreadable one is a broken checkout — and a
        // skipped file would silently shrink the wait set, which is the failure
        // this function exists to prevent.
        let body = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read the compose file {path}: {e}"));
        let name = std::path::Path::new(path)
            .file_name()
            .map_or_else(|| path.clone(), |n| n.to_string_lossy().into_owned());
        owners.extend(
            host_ports_in(&body)
                .into_iter()
                .map(|port| (port, name.clone())),
        );
    }
    owners.sort_unstable();
    owners.dedup();
    owners
}

/// Block until every host port `files` publish can be bound again — the barrier
/// every stack passes before it starts.
///
/// [`wait_stack_gone`] waits for *containers*; this waits for the *sockets*, and
/// they are not the same moment. Whatever still holds one — dockerd's per-port
/// proxy finishing its own teardown, or an unrelated outbound connection that
/// was handed the port out of the ephemeral pool — `compose up` fails on it with
/// `failed to bind host port ... address already in use` and no indication of
/// which port or who held it. That opaque failure is issue #117; this turns it
/// into a named one, before the fleet is even asked to start.
///
/// **Scoped to the stack's own files, and there is deliberately no whole-set
/// variant.** The barrier used to wait on every port any overlay publishes, so a
/// port an overlay the scenario never loads could stall it — and because the wait
/// runs before the first container starts, the bail landed on whichever scenario
/// asked for a stack first in its shard. `c10_proxy_once_survives_owner_and_leader_kills`
/// failed that way on `36700`, which `sequencing.overlay.yml` publishes and c10
/// does not load, and the bare port number sent a reader looking through c10 for
/// it. That was #580. A port this stack does not publish cannot collide with it,
/// so waiting on one could only ever stall it.
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
pub fn wait_ports_free_for(files: &[String], timeout: Duration) -> anyhow::Result<()> {
    wait_ports_free_owned(&published_ports_of(files), timeout)
}

fn wait_ports_free_owned(owners: &[(u16, String)], timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        // `0.0.0.0`, matching what compose publishes on: BSD accepts a wildcard
        // bind alongside a loopback one, so probing `127.0.0.1` would report a
        // port free that docker cannot have.
        let held: Vec<&(u16, String)> = owners
            .iter()
            .filter(|(port, _)| std::net::TcpListener::bind(("0.0.0.0", *port)).is_err())
            .collect();

        if held.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let named: Vec<String> = held
                .iter()
                .map(|(port, owner)| {
                    if owner.is_empty() {
                        port.to_string()
                    } else {
                        format!("{port} ({owner})")
                    }
                })
                .collect();
            bail!(
                "HARNESS: published host ports still bound {timeout:?} after teardown: {}. \
                 This is the host, not the scenario — the wait runs before the first \
                 container starts. `compose up` would fail on one of these with an \
                 unattributable 'address already in use'. Find the holder with: \
                 lsof -nP -iTCP:{} -sTCP:LISTEN",
                named.join(", "),
                held[0].0
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Every host port a compose file publishes, in `"HOST:CONTAINER"` order.
///
/// Line-oriented rather than a YAML parse: this reads one key, and pulling a
/// YAML dependency into the harness to do it would be a dependency taken on for
/// a guard.
#[must_use]
pub fn host_ports_in(compose: &str) -> Vec<u16> {
    let mut ports = Vec::new();
    let mut in_ports = false;
    for line in compose.lines() {
        let trimmed = line.trim();
        if trimmed == "ports:" {
            in_ports = true;
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !trimmed.starts_with('-') {
            in_ports = false;
            continue;
        }
        if !in_ports {
            continue;
        }
        let spec = trimmed.trim_start_matches('-').trim();
        let spec = spec.split('#').next().unwrap_or(spec).trim();
        let spec = spec.trim_matches('"').trim_matches('\'');
        if let Some((host, _container)) = spec.split_once(':')
            && let Ok(port) = host.trim().parse::<u16>()
        {
            ports.push(port);
        }
    }
    ports
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
///
/// No credential: every scenario in this tier runs against an open admin plane.
/// The fleet is started without `MB_APIKEY`, which is what leaves it open (D-46).
pub async fn get_json(port: u16, path: &str) -> anyhow::Result<(u16, serde_json::Value)> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    let status = response.status().as_u16();
    // The status is the subject here, so a body that is not JSON (or is empty)
    // must not mask it — callers assert on the status and use the body only as
    // forensics. `imposter_ports` is where a non-2xx becomes an error.
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

/// The front door's host ports under `front-door.overlay.yml` — one per node,
/// in `NODES` order. C17 and C18 only: no other scenario binds `--front-door`.
pub const FRONT_DOOR_HOST_PORTS: [u16; 3] = [12527, 22527, 32527];

/// The counting origin's admin API, as `proxy-origin.overlay.yml` publishes it.
///
/// The origin is a fourth `rift-cluster-server`, run **un-clustered**, that the
/// fleet's `proxyOnce`/`proxyAlways` imposters forward to. Its
/// Mountebank-compatible admin API therefore hands the harness an exact upstream
/// counter for free: `GET /imposters/:port` reports `numberOfRequests` and
/// `savedRequests`. That is what turns C10's duplicate-upstream bound and C11's
/// "proxyOnce freezes the origin" into equalities against first-class API values
/// instead of a log scrape — and it is why the origin is another rift container
/// rather than a new image: the chaos tier pins images by digest, and inventing
/// one for a static-file server would be a new supply-chain surface to serve a
/// counter this build already publishes.
///
/// **46525 is inside** Linux's ephemeral source-port range (32768-60999), so it
/// is reserved by the `ip_local_reserved_ports` step `ci.yml` and
/// `nightly-chaos.yml` run;
/// `ci_reserves_every_published_port_that_linux_could_hand_out` fails the build
/// if that is ever forgotten.
pub const PROXY_ORIGIN_ADMIN_PORT: u16 = 46525;

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
///
/// **Why the status is checked rather than the body simply parsed.** Without the
/// check a refusal body has no `imposters` array, so it read as "this node has no
/// imposters" — and `wait_converged` then reported `reached only 0/3 nodes`, a
/// convergence failure, for what was actually a refused read. That cost a full
/// container run to diagnose. A refused read is not an empty read, and the two
/// must not be spelled the same way.
pub async fn imposter_ports(admin: u16) -> anyhow::Result<Vec<u64>> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{admin}/imposters"))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
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

/// [`wait_converged`], restricted to a named subset — for scenarios where some
/// node is deliberately down.
///
/// The last read error is carried into the timeout message. Polling must treat
/// an error as "not yet" — a node that is still starting legitimately refuses —
/// but discarding it entirely is what made a read failure present as a bare
/// `0/3 nodes` with nothing to act on.
pub async fn wait_converged_on(
    nodes: &[&Node],
    port: u64,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    loop {
        let mut seen = 0;
        for node in nodes {
            match imposter_ports(node.admin).await {
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

/// Scrape one counter or gauge family from a node's metrics port.
///
/// Only the families `crates/rift-cluster/src/metrics.rs` keeps as correctness
/// instrumentation (D-71, #548) — counts of things that happened, which no
/// state endpoint can answer. Membership, leadership and bind state are read
/// from [`fleet_members`] instead.
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

/// `GET /_fleet/members` on a node's admin port — the fleet's live membership
/// view (RFC-006 §5.2): `voters`, `current_leader`, `is_leader`, `last_applied`
/// and this node's own `bind_failures` (#369).
///
/// The admin port, not `/_cluster/members`: that one rides the **cluster port**
/// behind the HMAC credential the harness does not hold (see
/// `FAILOVER_WRITE_BOUND` in the scenarios). Read without a credential, like
/// every other admin read in this tier — the fleet boots with no `MB_APIKEY`,
/// so the admin plane is open (D-46).
///
/// Live, not sampled: the body is read off the node's Raft state at request
/// time, so unlike the retired `rift_cluster_members` gauges (D-71, #548) there
/// is no sampler to race. The waits below still poll, because forming, electing
/// and promoting are asynchronous.
pub async fn fleet_members(admin: u16) -> anyhow::Result<serde_json::Value> {
    let (status, body) = get_json(admin, "/_fleet/members").await?;
    if status != 200 {
        bail!("/_fleet/members on :{admin} answered {status}");
    }
    Ok(body)
}

/// Whether a `/_fleet/members` answer says the answering node holds leadership.
#[must_use]
pub fn claims_leadership(body: &serde_json::Value) -> bool {
    body["is_leader"].as_bool() == Some(true)
}

/// The effective voter set's size, as a `/_fleet/members` answer reports it.
#[must_use]
pub fn voter_count(body: &serde_json::Value) -> Option<usize> {
    body["voters"].as_array().map(Vec::len)
}

/// Wait until **exactly one** node reports itself leader, and return its index.
///
/// Exactly one, not at least one: a split brain must fail here rather than pass
/// as "a leader exists". Polled rather than read once because an election is
/// asynchronous — asserting immediately after readiness fails a healthy cluster
/// that is still electing.
pub async fn wait_single_leader(timeout: Duration) -> anyhow::Result<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut leaders = Vec::new();
        for (i, node) in NODES.iter().enumerate() {
            if fleet_members(node.admin)
                .await
                .is_ok_and(|body| claims_leadership(&body))
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

/// Wait until `node` reports the effective voter set has reached `expected`
/// (`voters` on `/_fleet/members`, read on the open admin plane).
pub async fn wait_voters(node: &Node, expected: usize, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let seen = fleet_members(node.admin)
            .await
            .map(|body| voter_count(&body));
        if seen.as_ref().is_ok_and(|v| *v == Some(expected)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "{} reports voters={:?}, expected {expected}",
                node.name,
                seen.ok().flatten()
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
    /// 1. A new `build.target` with no tag of its own. Until D-58 the (since
    ///    removed, #552) faketime flavor was exactly this: it shared the
    ///    production tag, and what kept the bytes matching the overlay was
    ///    `--build` running before every `up`. `cluster-smoke` no longer passes
    ///    `--build`, so a repeat would mean a scenario silently running the
    ///    wrong flavor.
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

    /// The workflow step that runs this tier, as text.
    fn chaos_step() -> String {
        let ci = std::fs::read_to_string(format!(
            "{}/../../.github/workflows/ci.yml",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("read .github/workflows/ci.yml");
        let (_, after) = ci
            .split_once("- name: Container chaos scenarios")
            .expect("ci.yml must still have the `Container chaos scenarios` step");
        // Up to the next step at the same indentation.
        after
            .split_once("\n      - ")
            .map_or(after, |(step, _)| step)
            .to_owned()
    }

    /// The `cargo test` command that actually **runs** this tier, with its `\`
    /// line continuations joined into one line.
    ///
    /// Two things here are not pedantry; mutation testing caught both.
    ///
    /// The step invokes `cargo test -p cluster-chaos` **twice** — once with
    /// `--list` to derive the shard's scenario names, once to run them — so
    /// taking the first match asserts against the listing pass, which carries
    /// none of the flags that matter and would have reported the run's flags
    /// missing whatever they were.
    ///
    /// And the assertions run against the command, not the step text: the step
    /// *explains* `--nocapture` in a comment, so a `step.contains("--nocapture")`
    /// stays satisfied by the prose describing the flag long after the flag
    /// itself is gone.
    fn chaos_test_invocation() -> String {
        let step = chaos_step();
        let mut runs: Vec<String> = Vec::new();
        for tail in step.split("cargo test -p cluster-chaos").skip(1) {
            let mut cmd = String::from("cargo test -p cluster-chaos");
            for line in tail.lines() {
                let trimmed = line.trim_end();
                cmd.push(' ');
                cmd.push_str(trimmed.trim_end_matches('\\').trim());
                if !trimmed.ends_with('\\') {
                    break;
                }
            }
            // The listing pass derives the shard; it runs no scenario.
            if !cmd.contains("--list") {
                runs.push(cmd);
            }
        }
        assert_eq!(
            runs.len(),
            1,
            "expected exactly one non-`--list` chaos invocation to assert against, \
             found {}: {runs:#?}",
            runs.len()
        );
        let cmd = runs.remove(0);
        // The run is the invocation piped through the empty-run guard. Anchoring
        // on that rather than on position says which command this is supposed to
        // be, so a future step that adds a third `cargo test` fails here instead
        // of silently moving the assertions onto it.
        assert!(
            cmd.contains("assert-scenarios-ran.sh"),
            "the selected invocation is not the guarded scenario run: {cmd}"
        );
        cmd
    }

    /// Pins D-67's other half: the collector is only durable if the workflow
    /// actually switches it on.
    ///
    /// A collector the workflow does not switch on is a collector that does not
    /// exist, and it fails **silently** — the harness reads an unset variable,
    /// writes nothing, and the job stays green with an empty artifact.
    ///
    /// That is the shape of #534, in the other direction: the artifact prints
    /// were there and correct, and nothing in CI made them reachable, so the
    /// gap survived until someone went looking for a number they assumed had
    /// been recorded all along. Nothing else pins these two names together —
    /// one is a Rust string constant, the other a YAML key — so a rename on
    /// either side would restore exactly that silence.
    #[test]
    fn cluster_smoke_sets_the_log_variables_this_harness_reads() {
        let step = chaos_step();
        for var in [TIMING_LOG_ENV, ARTIFACT_LOG_ENV] {
            assert!(
                step.contains(&format!("{var}:")),
                "`cluster-smoke` does not set ${var}, which this harness reads to \
                 decide whether to collect. Unset, the collector is a no-op and the \
                 job is green with nothing recorded."
            );
        }
    }

    /// Pins D-67: a measured chaos figure is recorded as a run artifact, not
    /// merely printed — and the printing half only happens with `--nocapture`.
    ///
    /// libtest captures a passing test's stdout. Without the flag the artifact
    /// lines reach a human only on a **failing** scenario — which is when they
    /// are least useful, the run having aborted before settling the measurement.
    /// That was #534.
    ///
    /// Pinned here rather than trusted to review because dropping the flag
    /// breaks nothing loudly: the scenarios still pass, the file is still
    /// written, and only the in-log copy Ch.12 promises quietly disappears.
    #[test]
    fn cluster_smoke_runs_the_chaos_tier_with_nocapture() {
        let cmd = chaos_test_invocation();
        assert!(
            cmd.contains("--nocapture"),
            "the chaos invocation dropped `--nocapture`, so libtest captures the \
             scenario artifacts again and a passing run prints none of them: {cmd}"
        );
        // `--nocapture` is a libtest argument, not a cargo one: before the `--`
        // it is an unrecognised cargo flag and the step dies. Both halves have
        // to be true for the flag to have any effect at all.
        let (cargo_args, libtest_args) = cmd
            .split_once(" -- ")
            .expect("the invocation must pass libtest arguments after a `--`");
        assert!(
            libtest_args.contains("--nocapture") && !cargo_args.contains("--nocapture"),
            "`--nocapture` must sit after the `--`, where libtest reads it: {cmd}"
        );
    }

    /// The file is a TSV read one row per artifact, and the artifact lines are
    /// assembled from multi-line format strings held together by `\` line
    /// continuations. A dropped backslash puts a real newline in the text, which
    /// would not fail anything — it would split one measurement across two rows
    /// and label the second with the first's scenario.
    #[test]
    fn an_artifact_row_is_one_line_whatever_the_text_contains() {
        let row = artifact_row("c10_scenario", "max = 3\tof 4\nrefused = 1");
        assert_eq!(row, "c10_scenario\tmax = 3 of 4 refused = 1\n");
        assert_eq!(row.matches('\n').count(), 1, "exactly one row");
        assert_eq!(
            row.matches('\t').count(),
            1,
            "exactly one separator, so column 2 is the whole measurement"
        );
    }

    /// Every artifact in a shard lands in one file, so the collector has to
    /// append. Truncating instead would leave only the last scenario's figure
    /// and still produce a plausible-looking summary of one row.
    #[test]
    fn artifacts_accumulate_rather_than_replace() {
        let path = std::env::temp_dir().join(format!(
            "chaos-artifact-test-{}-{:?}.tsv",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);

        append_line(&path, &artifact_row("c10", "max = 3")).expect("create and write");
        append_line(&path, &artifact_row("c29", "answered in 1.2s")).expect("append");

        let body = std::fs::read_to_string(&path).expect("read back");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            body, "c10\tmax = 3\nc29\tanswered in 1.2s\n",
            "both rows, in order, one per line"
        );
    }

    /// Best effort, like the timing collector: this is reached from scenarios
    /// mid-assertion, and a log that cannot be written must cost a datum, never
    /// the run. `record_artifact` discards the error — this pins that there IS
    /// an error to discard rather than a panic.
    #[test]
    fn an_unwritable_artifact_log_is_an_error_not_a_panic() {
        let dir = std::env::temp_dir().join(format!("chaos-artifact-dir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create a directory");
        // A directory is not an appendable file.
        let err = append_line(&dir, "c10\tmax = 3\n");
        let _ = std::fs::remove_dir(&dir);
        assert!(err.is_err(), "opening a directory for append must fail");
    }
}

#[cfg(test)]
mod overlay_scoped_ports {
    use super::*;

    /// #580: a stack must wait only on the ports its own compose files publish.
    ///
    /// `wait_ports_free()` waits on the whole published set before every stack,
    /// so a port belonging to an overlay this scenario never loads can stall it
    /// — and the bail then lands on whichever scenario asks for a stack first in
    /// its shard, which is an accident of shard order, not a fact about that
    /// scenario. That is how `c10_proxy_once_survives_owner_and_leader_kills`
    /// came to fail on `36700`, a port `sequencing.overlay.yml` publishes and
    /// c10 never touches.
    #[test]
    fn a_stack_waits_only_on_the_ports_its_own_files_publish() {
        let files = vec![base_file(), compose_file("front-door.overlay.yml")];
        let ports: Vec<u16> = published_ports_of(&files)
            .into_iter()
            .map(|(port, _)| port)
            .collect();

        for port in FRONT_DOOR_HOST_PORTS {
            assert!(
                ports.contains(&port),
                "the front-door overlay publishes {port}, so its own stack must wait on it: \
                 {ports:?}"
            );
        }
        for port in SEQUENCING_HOST_PORTS {
            assert!(
                !ports.contains(&port),
                "{port} belongs to sequencing.overlay.yml, which this stack never loads — \
                 waiting on it is how an unrelated scenario's held port stalls this one (#580)"
            );
        }
        assert!(
            ports.contains(&NODES[0].admin),
            "the base file's own published ports must still be waited on: {ports:?}"
        );
    }

    /// The bail has to name the overlay, not only the port. `36700` alone sent a
    /// reader looking through c10 for a port c10 does not publish.
    #[test]
    fn a_held_port_is_named_with_the_overlay_that_publishes_it() {
        let owners = published_ports_of(&[compose_file("sequencing.overlay.yml")]);
        let (port, owner) = owners
            .iter()
            .find(|(port, _)| *port == SEQUENCING_HOST_PORTS[2])
            .expect("the sequencing overlay publishes 36700");
        assert_eq!(*port, 36700, "the port this actually bit on");
        assert_eq!(
            owner, "sequencing.overlay.yml",
            "the bail must be able to name the file that publishes the port"
        );
    }

    /// The two tests above pin `published_ports_of`; this pins that `start_stack`
    /// actually *uses* it, on the files it is about to compose.
    ///
    /// Without this the fix is unguarded where it matters. Rewiring the wait back
    /// to a fixed list — which is the shape #580 was — leaves both tests above
    /// green, because they call the helper directly and never execute the call
    /// site. Confirmed by mutation: replacing `cluster.files` with a hardcoded
    /// overlay list passed the whole suite until this test existed.
    ///
    /// Structural because the alternative is not available: `start_stack` runs
    /// `docker compose`, so no unit test can execute it.
    #[test]
    fn start_stack_waits_on_the_files_it_is_about_to_compose() {
        const SOURCE: &str = include_str!("lib.rs");
        let start = SOURCE
            .find("async fn start_stack(")
            .expect("start_stack must exist");
        // Bounded at the next item, taking the *nearer* of the two boundaries.
        // An unbounded scan runs off the end of the function and matches these
        // very assertions further down the file, so deleting the wait outright
        // would still pass — which it did, until mutation caught it. Preferring
        // one boundary over the nearer one has the same failure mode on a delay:
        // there is no `\n    async fn ` in this file today (every later method is
        // `pub async fn`), so an `or_else` chain silently ran to `fn compose`.
        let rest = &SOURCE[start..];
        let tail = &rest[1..];
        let end = [
            tail.find("\n    async fn "),
            tail.find("\n    pub async fn "),
            tail.find("\n    fn "),
            tail.find("\n    pub fn "),
        ]
        .into_iter()
        .flatten()
        .min()
        .map_or(rest.len(), |at| at + 1);
        let body = &rest[..end];

        // Asserted against the call's own argument, not against text appearing
        // anywhere before it. A preamble check passes on `let _ = &cluster.files;`
        // beside a fixed list, and on a widening that routes through a helper —
        // both of which are #580 again.
        assert!(
            body.contains("wait_ports_free_for(&cluster.files,"),
            "start_stack must wait on exactly the files it is about to compose, as \
             `wait_ports_free_for(&cluster.files, ..)`. Any other argument is #580: \
             a port belonging to an overlay this scenario never loads cannot collide \
             with it, so waiting on one can only stall it — and the bail is then \
             reported against whichever scenario starts first in the shard. Body:\n{body}"
        );
    }

    /// The bail itself has to name the overlay — the half of #580 an operator
    /// actually reads.
    ///
    /// The test above asserts on `published_ports_of`'s *data*; this asserts on
    /// the message, which is a different piece of code. Nothing else reaches the
    /// named branch of the formatter: `the_port_barrier_names_the_port_that_is_
    /// still_held` goes through `wait_ports_free_in`, which supplies empty owners
    /// and so only ever exercises the bare-port branch. Replacing the whole
    /// `named` block with `port.to_string()` left every other test green.
    ///
    /// A port this test owns, not one from a compose file: waiting on a published
    /// port would fail on any machine with the demo stack up, which says nothing
    /// about the barrier.
    #[test]
    fn the_bail_names_the_overlay_that_publishes_a_held_port() {
        let squatter = std::net::TcpListener::bind(("0.0.0.0", 0)).expect("take a free port");
        let port = squatter.local_addr().expect("addr").port();

        let err = wait_ports_free_owned(
            &[(port, "sequencing.overlay.yml".to_owned())],
            Duration::from_millis(300),
        )
        .expect_err("a held port must fail the barrier");
        let message = err.to_string();

        assert!(
            message.contains(&format!("{port} (sequencing.overlay.yml)")),
            "the bail must name the overlay beside the port, not the port alone — \
             `36700` on its own is what sent a reader looking through c10 for a port \
             c10 never publishes: {message}"
        );
        assert!(
            message.starts_with("HARNESS:"),
            "the bail is a fact about the host, not about the scenario that happened \
             to ask for a stack first: {message}"
        );

        drop(squatter);
        wait_ports_free_owned(
            &[(port, "sequencing.overlay.yml".to_owned())],
            Duration::from_secs(5),
        )
        .expect("the barrier clears once the port is released");
    }

    /// The scraper feeding both of the above must not silently match nothing —
    /// every assertion here would pass against an empty set.
    #[test]
    fn the_overlay_scraper_is_not_silently_empty() {
        let all = published_ports_of(&[base_file(), overlay_file()]);
        assert!(
            all.len() >= NODES.len() * 3,
            "scraped only {} ports from the base and chaos files; the scraper is \
             broken, not the topology",
            all.len()
        );
    }
}
