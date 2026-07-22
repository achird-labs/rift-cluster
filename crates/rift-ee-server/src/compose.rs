//! Composition: the open-source server plus, when `--cluster` is on, the
//! control-plane node, the operator surface and the probes.
//!
//! Composition rather than a fork is the whole point of this crate. With the
//! master switch off, [`start`] hands the open-source [`ServerBuilder`] the very
//! same CLI the `rift` binary would and adds nothing — no extra listener, no
//! decorator, no manager override. With it on, the only difference is a manager
//! pre-built with the cluster backends, which the builder accepts through its
//! documented embedding seam.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use rift_cluster::rpc::Router;
use rift_cluster::{ClusterDecorator, NodeConfig, NodeError, NodeIdentity, RaftNode, metrics};
use rift_ee::seams::{ImposterManager, RunningServer, ServerBuilder, TlsDefaults};

use crate::admin_front::{self, AdminFront, FrontConfig};
use crate::cli::EeCli;
use crate::cluster_api::{self, NodeSlot};
use crate::probes::{self, ProbeListener};
use crate::readiness::{GATE_JOINED, GATE_RECONCILED, Readiness};

/// How long a starting node keeps trying its seeds before giving up.
///
/// A node routinely starts before its seeds are accepting during a rolling
/// deploy, so this is a startup grace period, not a request timeout — long
/// enough to outlast a peer's own boot, short enough that a genuinely
/// misconfigured seed list fails the deployment instead of hanging it.
const SEED_JOIN_DEADLINE: Duration = Duration::from_secs(30);

/// How often the fleet gauges are re-sampled from Raft metrics. Fast enough that
/// a leadership change is visible within a scrape interval, slow enough to stay
/// invisible next to the data plane.
const METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// A running enterprise server: the open-source planes, plus the cluster ones
/// when clustering is on.
pub struct ComposedServer {
    server: RunningServer,
    probes: Option<ProbeListener>,
    node: Option<Arc<RaftNode>>,
    /// The clustered admin front, when clustering is on: it owns the public
    /// admin address, and the OSS admin inside `server` is on loopback.
    front: Option<AdminFront>,
    /// Samples the fleet gauges. Aborted on shutdown so it cannot outlive the
    /// node it reads.
    metrics_sampler: Option<tokio::task::JoinHandle<()>>,
    /// Reconciles the engine and satisfies [`GATE_RECONCILED`]; aborted on
    /// shutdown for the same reason as the sampler.
    reconciler: Option<tokio::task::JoinHandle<()>>,
    /// Replays parked intents on leader changes (issue #9 R4); same lifecycle
    /// rules as the reconciler.
    intent_replayer: Option<tokio::task::JoinHandle<()>>,
    cluster_addr: Option<SocketAddr>,
    /// The clustered engine, held so shutdown can stop its imposters: their
    /// listeners are independent of the admin plane, and a composed shutdown
    /// that leaves them serving would strand every bound port (visible
    /// in-process; in a container the process exit hides it).
    manager: Option<Arc<ImposterManager>>,
    readiness: Arc<Readiness>,
    leave_timeout: Duration,
}

impl ComposedServer {
    /// The bound admin API address — the front's when clustering is on, the
    /// OSS admin's otherwise. Either way: where clients point.
    #[must_use]
    pub fn admin_addr(&self) -> SocketAddr {
        self.front
            .as_ref()
            .map_or_else(|| self.server.admin_addr(), AdminFront::local_addr)
    }

    /// The bound probe address — `None` when clustering is off, because an
    /// un-clustered node must be indistinguishable from the open-source binary.
    #[must_use]
    pub fn probe_addr(&self) -> Option<SocketAddr> {
        self.probes.as_ref().map(ProbeListener::local_addr)
    }

    /// The bound cluster port, when clustering is on.
    #[must_use]
    pub fn cluster_addr(&self) -> Option<SocketAddr> {
        self.cluster_addr
    }

    /// The readiness latch behind `/readyz`.
    #[must_use]
    pub fn readiness(&self) -> &Arc<Readiness> {
        &self.readiness
    }

    /// The control-plane node, when clustering is on. The composition root
    /// hands this out for embedders and tests that need the node's own view
    /// (parked intents, status) rather than the HTTP surfaces.
    #[must_use]
    pub fn node(&self) -> Option<&Arc<RaftNode>> {
        self.node.as_ref()
    }

    /// Serve until the admin API stops.
    pub async fn join(self) -> anyhow::Result<()> {
        self.server.join().await
    }

    /// Stop every listener immediately, without a drain.
    pub async fn shutdown(self) {
        if let Some(sampler) = self.metrics_sampler {
            sampler.abort();
        }
        if let Some(reconciler) = self.reconciler {
            reconciler.abort();
        }
        if let Some(replayer) = self.intent_replayer {
            replayer.abort();
        }
        if let Some(probes) = self.probes {
            probes.shutdown().await;
        }
        // The front goes before the OSS admin behind it, so a straggling
        // request meets a closed port rather than a half-alive pipeline.
        if let Some(front) = self.front {
            front.shutdown().await;
        }
        self.server.shutdown().await;
        if let Some(manager) = self.manager {
            manager.shutdown().await;
        }
        if let Some(node) = self.node
            && let Err(e) = node.shutdown().await
        {
            // Error, not warn: a Raft store that did not close keeps the redb
            // lock, so the next start of this node fails with an
            // unrelated-looking "state dir locked".
            tracing::error!(error = %e, "cluster node shutdown reported an error");
        }
    }

    /// A graceful leave (RFC-001 §7.1.2): fail readiness first so the balancer
    /// stops sending new work, let in-flight work finish for the leave timeout,
    /// and only then close the listeners.
    ///
    /// The order matters — closing sockets first turns every in-flight request
    /// into a client-visible error, which is exactly what the leave exists to
    /// avoid. The orchestrator's grace period must exceed this or it will kill
    /// the process mid-drain.
    pub async fn graceful_leave(self) {
        self.readiness.start_draining();
        tracing::info!(
            timeout_secs = self.leave_timeout.as_secs(),
            "graceful leave: reporting not-ready and draining in-flight work"
        );
        tokio::time::sleep(self.leave_timeout).await;
        self.shutdown().await;
    }

    /// Serve until either the admin plane stops or `signal` resolves, then leave
    /// gracefully; returns the admin plane's outcome.
    ///
    /// Racing the two is the point (issue #42). Awaiting only the signal left a
    /// node whose admin accept loop had died running as a cluster member until
    /// someone sent SIGTERM — the failure was not merely unpropagated, it was
    /// unobserved.
    ///
    /// The leave runs on **both** arms, in RFC-001 §7.1.2 order. A dead admin
    /// plane leaves the imposter listeners serving, so the balancer must still be
    /// told to shed this node before any socket closes.
    pub async fn serve_until(self, signal: impl Future<Output = ()>) -> anyhow::Result<()> {
        let outcome = tokio::select! {
            // Biased so a death that is already observable wins over a signal
            // arriving in the same poll. Under `select!`'s default random order
            // the signal could take that tie, dropping the wait future before it
            // reads the outcome — and upstream delivers that error to the first
            // caller only, so the node would exit 0 on a dead admin plane.
            biased;
            result = self.server.wait() => result,
            () = signal => Ok(()),
        };
        self.graceful_leave().await;
        outcome
    }
}

/// Compose and start the server described by `cli`.
pub async fn start(cli: EeCli) -> anyhow::Result<ComposedServer> {
    start_with_runtimes(cli, Vec::new()).await
}

/// [`start`], with the per-core worker runtimes imposter accept loops fan out
/// across. Only the un-clustered path can supply any: the cluster guards refuse
/// `--runtime per-core` outright (D-14).
pub async fn start_with_runtimes(
    cli: EeCli,
    accept_runtimes: Vec<tokio::runtime::Handle>,
) -> anyhow::Result<ComposedServer> {
    let cluster = cli.resolve_cluster()?;

    if !cluster.enabled {
        let server = ServerBuilder::from_cli(cli.oss)
            .accept_runtimes(accept_runtimes)
            .start()
            .await?;
        return Ok(ComposedServer {
            server,
            probes: None,
            node: None,
            front: None,
            metrics_sampler: None,
            reconciler: None,
            intent_replayer: None,
            cluster_addr: None,
            manager: None,
            readiness: Arc::new(Readiness::awaiting([])),
            leave_timeout: Duration::ZERO,
        });
    }

    let bind = cluster
        .bind
        .context("--cluster-bind is required with --cluster")?;
    let state_dir = cli.cluster_state_dir();
    std::fs::create_dir_all(&state_dir).with_context(|| {
        format!(
            "creating the cluster state directory {}",
            state_dir.display()
        )
    })?;
    let identity = NodeIdentity::load_or_mint(&state_dir, cli.proposed_node_id())
        .with_context(|| format!("reading node identity from {}", state_dir.display()))?;

    // Auditable before anything binds: an unauthenticated cluster port must be
    // visible on /metrics even if the node then fails to start.
    metrics::set_insecure(cluster.is_insecure());

    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED, GATE_RECONCILED]));

    // Probes come up first, before the node exists. `/healthz` has to answer
    // *during* the join — a liveness probe that gets connection-refused while
    // the node is converging restarts the pod mid-convergence, which is the one
    // thing the liveness/readiness split exists to prevent. Binding here also
    // means nothing is running yet if the probe port is taken, so that failure
    // cannot orphan a listener.
    let probe_bind = cli.cluster.cluster_probe_bind;
    let probes = probes::bind(probe_bind, Arc::clone(&readiness))
        .await
        .with_context(|| format!("binding the probe listener on {probe_bind}"))?;

    // The manager exists before the node so committed ops can drive it from
    // the very first apply; the data-plane server later composes around this
    // same instance rather than building its own.
    let manager = match cluster_manager(&cli, accept_runtimes) {
        Ok(manager) => Arc::new(manager),
        Err(e) => {
            probes.shutdown().await;
            return Err(e.context("building the clustered imposter manager"));
        }
    };

    let slot = NodeSlot::default();
    let node = match RaftNode::start(NodeConfig {
        node_id: identity.node_id(),
        bind,
        advertise: cli.cluster.cluster_advertise,
        data_dir: state_dir,
        secret: cluster.secret,
        routes: cluster_api::routes(Router::new(), slot.clone(), Arc::clone(&readiness)),
        engine: Some(Arc::clone(&manager)),
    })
    .await
    {
        Ok(node) => Arc::new(node),
        Err(e) => {
            probes.shutdown().await;
            return Err(anyhow::Error::new(e).context("starting the cluster control-plane node"));
        }
    };
    if let Err(e) = slot.set(&node) {
        probes.shutdown().await;
        if let Err(e) = node.shutdown().await {
            tracing::error!(error = %e, "cluster node shutdown reported an error");
        }
        return Err(anyhow::Error::new(e).context("binding the operator surface to the node"));
    }

    let cluster_addr = node.advertise_addr();
    let leave_timeout = Duration::from_secs(cli.cluster.cluster_leave_timeout);

    // Everything from here can fail with a live node and, later, a live server.
    // `start` is an embedding seam that callers retry, so a failure must not
    // leave the cluster port bound or the redb state dir locked — it would fail
    // the retry too, with an error that hides the real cause.
    match attach_data_plane(cli, &node, &readiness, Arc::clone(&manager)).await {
        Ok((server, front, reconciler)) => Ok(ComposedServer {
            server,
            probes: Some(probes),
            front: Some(front),
            metrics_sampler: Some(spawn_metrics_sampler(Arc::clone(&node))),
            reconciler: Some(reconciler),
            intent_replayer: Some(spawn_intent_replayer(Arc::clone(&node))),
            node: Some(node),
            cluster_addr: Some(cluster_addr),
            manager: Some(manager),
            readiness,
            leave_timeout,
        }),
        Err(e) => {
            probes.shutdown().await;
            if let Err(e) = node.shutdown().await {
                tracing::error!(error = %e, "cluster node shutdown reported an error");
            }
            Err(e)
        }
    }
}

/// Join the cluster, then compose and start the open-source data plane on top of
/// the running node. Split out so a failure anywhere in it lands on one cleanup
/// path in [`start_with_runtimes`].
async fn attach_data_plane(
    mut cli: EeCli,
    node: &Arc<RaftNode>,
    readiness: &Arc<Readiness>,
    manager: Arc<ImposterManager>,
) -> anyhow::Result<(RunningServer, AdminFront, tokio::task::JoinHandle<()>)> {
    join_or_bootstrap(node, &cli).await?;
    readiness.satisfy(GATE_JOINED);

    // The public admin address belongs to the front (issue #9): the OSS admin
    // retreats to an ephemeral loopback port the front proxies to. `--cluster`
    // off never reaches this function, so that path keeps upstream's binding
    // untouched.
    let public_admin = format!("{}:{}", cli.oss.host, cli.oss.port);
    let api_key = cli.oss.api_key.clone();
    let allow_injection = cli.oss.allow_injection;
    let scripts_dir = cli.oss.scripts_dir.clone();
    cli.oss.host = "127.0.0.1".to_owned();
    cli.oss.port = 0;

    let barrier = cli.cluster.cluster_write_barrier;
    let barrier_timeout = Duration::from_secs(cli.cluster.cluster_write_barrier_timeout);
    let admin_async = cli.cluster.cluster_admin_async;

    let server = ServerBuilder::from_cli(cli.oss)
        .manager(manager)
        .start()
        .await?;

    // The config file's `intercept` block is the other spelling of the flag the
    // startup guards already refuse, but it is only known once the builder has
    // loaded the config — so it is caught here instead, before the node serves
    // anything, rather than duplicating the loader to check it earlier.
    if server.intercept_addr().is_some() {
        server.shutdown().await;
        return Err(anyhow::Error::new(
            rift_cluster::ConfigError::InterceptUnsupported,
        ));
    }

    let front = match admin_front::bind(
        FrontConfig {
            public_addr: public_admin.clone(),
            upstream_admin: server.admin_addr(),
            api_key,
            allow_injection,
            scripts_dir,
            barrier,
            barrier_timeout,
            admin_async,
        },
        node,
    )
    .await
    {
        Ok(front) => front,
        Err(e) => {
            server.shutdown().await;
            return Err(anyhow::Error::new(e).context(format!(
                "binding the clustered admin front on {public_admin}"
            )));
        }
    };

    Ok((server, front, spawn_reconciler(node, readiness)))
}

/// Catch up to the leader's applied index as observed at join, project the
/// applied configs onto the engine, then open [`GATE_RECONCILED`] (ADR-001
/// §5.2). Every step retries: readiness simply stays pending — visibly, with
/// the gate named by `/readyz` — until the node is genuinely reconciled.
fn spawn_reconciler(
    node: &Arc<RaftNode>,
    readiness: &Arc<Readiness>,
) -> tokio::task::JoinHandle<()> {
    let node = Arc::downgrade(node);
    let readiness = Arc::clone(readiness);
    tokio::spawn(async move {
        let mut quiet_ticks: u32 = 0;
        loop {
            let Some(node) = node.upgrade() else { return };
            // A wedged reconcile must be diagnosable from the logs, not only
            // from the gate `/readyz` keeps naming: one warning every ~5s.
            quiet_ticks += 1;
            if quiet_ticks.is_multiple_of(50) {
                tracing::warn!(
                    last_applied = node.status().last_applied,
                    "still reconciling: no leader reachable or applied state behind"
                );
            }
            if let Some(target) = node.leader_applied().await
                && node.status().last_applied.unwrap_or(0) >= target
            {
                match node.reconcile_engine().await {
                    Ok(()) => {
                        readiness.satisfy(GATE_RECONCILED);
                        return;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "engine reconcile failed; retrying");
                    }
                }
            }
            drop(node);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

/// Re-sample the fleet gauges on a timer.
///
/// A `Weak` handle for the same reason the operator surface holds one: this task
/// must never be what keeps the node alive, or shutdown would deadlock on a task
/// that is itself waiting to read the node.
fn spawn_metrics_sampler(node: Arc<RaftNode>) -> tokio::task::JoinHandle<()> {
    let node = Arc::downgrade(&node);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(METRICS_SAMPLE_INTERVAL);
        loop {
            ticker.tick().await;
            let Some(node) = node.upgrade() else { return };
            metrics::observe_node(&node.status(), &node.ring());
        }
    })
}

/// The open-source manager the builder would have constructed, plus the cluster
/// backends. Mirrors `ServerBuilder`'s internal construction because injecting a
/// manager replaces it wholesale — the seam is all-or-nothing.
fn cluster_manager(
    cli: &EeCli,
    accept_runtimes: Vec<tokio::runtime::Handle>,
) -> anyhow::Result<ImposterManager> {
    let default_cert = cli
        .oss
        .default_tls_cert
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()
        .context("reading --default-tls-cert")?;
    let default_key = cli
        .oss
        .default_tls_key
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()
        .context("reading --default-tls-key")?;

    Ok(ImposterManager::with_datadir(cli.oss.datadir.clone())
        .with_tls_defaults(TlsDefaults {
            default_cert,
            default_key,
            allow_self_signed: !cli.oss.no_self_signed_tls,
        })
        .with_accept_runtimes(accept_runtimes)
        .with_response_decorator(Arc::new(ClusterDecorator)))
}

/// Replay parked intents (issue #9 R4): drain whenever a leader (re)appears —
/// which includes startup, once the join completes — plus a slow periodic
/// sweep as a safety net. The state machine's dedup makes a replay of an
/// already-applied op collapse to its original response, so replaying is
/// always safe, never a double-apply.
fn spawn_intent_replayer(node: Arc<RaftNode>) -> tokio::task::JoinHandle<()> {
    let node = Arc::downgrade(&node);
    tokio::spawn(async move {
        let mut last_leader = None;
        let mut ticks: u32 = 0;
        loop {
            let Some(node) = node.upgrade() else { return };
            let leader = node.status().current_leader;
            ticks = ticks.wrapping_add(1);
            let leader_appeared = leader.is_some() && leader != last_leader;
            last_leader = leader;
            if leader_appeared || (leader.is_some() && ticks.is_multiple_of(120)) {
                drain_parked_intents(&node).await;
            }
            drop(node);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
}

async fn drain_parked_intents(node: &RaftNode) {
    let intents = match node.parked_intents() {
        Ok(intents) => intents,
        Err(e) => {
            tracing::error!(error = %e, "cannot read parked intents");
            return;
        }
    };
    metrics::intents_pending_sampled(intents.len());
    for request in intents {
        let op_id = request.op_id;
        match node.submit(request).await {
            // Terminal either way — an op the state machine refused is refused
            // identically on every replay, so it retires like a success and
            // stays queryable through GET /_cluster/ops/:id. An unpark that
            // keeps failing is retried every sweep; dedup keeps each retry a
            // no-op inside its 24 h window (a metric for over-aged intents is
            // the metrics slice's job).
            Ok(_) => match node.unpark_intent(&op_id) {
                Ok(()) => {
                    metrics::intent_replayed();
                    tracing::info!(%op_id, "replayed parked intent");
                }
                Err(e) => tracing::error!(%op_id, error = %e, "replayed but could not unpark"),
            },
            // No quorum fails every intent identically — stop the sweep. Any
            // other error is this op's own (encode, fatal runtime): log it and
            // keep going, or one poisoned intent starves the rest.
            Err(e @ NodeError::Unavailable(_)) => {
                tracing::warn!(%op_id, error = %e, "no quorum; intents stay parked for the next sweep");
                return;
            }
            Err(e) => {
                tracing::error!(%op_id, error = %e, "replay failed for this intent; continuing");
            }
        }
    }
}

/// Attach to an existing cluster through the seeds, or found one when the
/// operator has said that is what they want.
async fn join_or_bootstrap(node: &RaftNode, cli: &EeCli) -> anyhow::Result<()> {
    // A restart resumes: the durable log already carries the membership, and
    // both re-initializing (refused by openraft) and re-joining (already a
    // member) would fail a node that is perfectly able to come back on its own.
    if node
        .is_initialized()
        .await
        .context("reading cluster initialization state")?
    {
        tracing::info!("cluster state present; resuming membership from the durable log");
        return Ok(());
    }

    if cli.cluster.cluster_seeds.is_empty() {
        anyhow::ensure!(
            cli.cluster.cluster_allow_solo,
            "--cluster with no --cluster-seeds would found a new single-node cluster; \
             pass --cluster-allow-solo if that is intended, or give it seeds to join"
        );
        return node
            .cluster_init()
            .await
            .context("bootstrapping a single-node cluster");
    }

    // Retried, and re-resolved on every attempt rather than once at parse time.
    // Both halves matter during a rolling deploy: this node routinely starts
    // before its seeds are accepting, and a headless service's DNS record gains
    // members as the fleet rolls — so a single pass over addresses resolved at
    // startup would fail a node that would have joined a second later, and would
    // pin it to whoever happened to answer first.
    let deadline = Instant::now() + SEED_JOIN_DEADLINE;
    let mut backoff = Duration::from_millis(100);
    let mut failures;
    loop {
        failures = Vec::new();
        for seed in &cli.cluster.cluster_seeds {
            let resolved = match tokio::net::lookup_host(seed).await {
                Ok(addrs) => addrs.collect::<Vec<_>>(),
                Err(e) => {
                    failures.push(format!("{seed}: {e}"));
                    continue;
                }
            };
            for addr in resolved {
                match node.join_via(addr).await {
                    Ok(()) => {
                        tracing::info!(%addr, "joined the cluster through seed");
                        return Ok(());
                    }
                    Err(e) => failures.push(format!("{addr}: {e}")),
                }
            }
        }

        if Instant::now() >= deadline {
            break;
        }
        tracing::warn!(
            attempts = failures.len(),
            ?backoff,
            "no seed admitted this node yet; retrying"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }

    Err(anyhow::anyhow!(
        "could not join the cluster through any seed within {}s ({})",
        SEED_JOIN_DEADLINE.as_secs(),
        failures.join("; ")
    ))
}

#[cfg(test)]
mod tests {
    //! The clustered path must *observe* the admin plane (issue #42).
    //!
    //! These live in the module rather than `tests/` because stopping the OSS
    //! admin plane out of band is the only way to simulate the accept loop dying,
    //! and the handle to do it is private. `tests/clustered.rs` owns the signal
    //! arm of the same race.

    use std::time::Duration;

    use clap::Parser;
    use tempfile::TempDir;

    use super::{ComposedServer, EeCli, start};

    /// A solo clustered node with a short drain, so the leave window is a test
    /// timescale rather than the 10 s production default.
    fn solo_cli(state: &TempDir, leave_timeout: &str) -> EeCli {
        EeCli::try_parse_from([
            "rift-ee-server",
            "--port",
            "0",
            "--metrics-port",
            "0",
            "--cluster",
            "--cluster-bind",
            "127.0.0.1:0",
            "--cluster-probe-bind",
            "127.0.0.1:0",
            "--cluster-secret",
            "issue-42-secret",
            "--cluster-allow-solo",
            "--cluster-leave-timeout",
            leave_timeout,
            "--cluster-state-dir",
            &state.path().to_string_lossy(),
        ])
        .expect("parses")
    }

    async fn probe_status(base: &str, path: &str) -> u16 {
        reqwest::get(format!("http://{base}{path}"))
            .await
            .expect("probe request")
            .status()
            .as_u16()
    }

    /// AC1 + AC2: the accept loop exiting is enough to end the clustered server.
    ///
    /// Before #42 this test could not even be written: `serve_until` awaited only
    /// the signal, so a node whose admin plane had died stayed a live cluster
    /// member until someone sent SIGTERM. The assertion that matters is the
    /// timeout — reaching it means the node is exactly the zombie this fixes.
    #[tokio::test]
    async fn admin_plane_death_ends_the_clustered_server() {
        let state = TempDir::new().expect("tempdir");
        let composed: ComposedServer = start(solo_cli(&state, "1")).await.expect("solo starts");

        // End the admin accept loop before handing ownership over. `shutdown`
        // takes `&self`, so this needs no second handle — and a `wait()` on an
        // already-stopped plane returns immediately, which is the same arm of
        // the race a mid-flight death takes.
        composed.server.shutdown().await;

        // A signal that never fires: the only way out is the admin plane dying.
        let serving = tokio::spawn(composed.serve_until(std::future::pending::<()>()));

        let outcome = tokio::time::timeout(Duration::from_secs(20), serving)
            .await
            .expect("serve_until must return when the admin plane dies, without a signal")
            .expect("the serve task did not panic");

        // A clean stop publishes no accept-loop error, so the raced result is Ok
        // — what is under test is that the result travels at all.
        assert!(
            outcome.is_ok(),
            "a clean admin-plane stop must not be reported as a failure: {outcome:?}"
        );
    }

    /// AC3: §7.1.2 ordering holds on the *wait* arm too.
    ///
    /// A dead admin plane leaves the imposter listeners serving, so the balancer
    /// must still be told to shed this node before any socket closes. Closing
    /// first would turn every in-flight data-plane request into a client error.
    #[tokio::test]
    async fn admin_plane_death_drains_before_closing_listeners() {
        let state = TempDir::new().expect("tempdir");
        let composed: ComposedServer = start(solo_cli(&state, "3")).await.expect("solo starts");
        let probes = composed.probe_addr().expect("probes bound").to_string();

        assert_eq!(probe_status(&probes, "/readyz").await, 200);

        // Kill the admin plane, then hand ownership over: the drain that follows
        // is driven by the death, not by any signal.
        composed.server.shutdown().await;
        let serving = tokio::spawn(composed.serve_until(std::future::pending::<()>()));

        // Inside the drain window the node reports itself away, but liveness must
        // stay up or the orchestrator kills it mid-leave.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            probe_status(&probes, "/readyz").await,
            503,
            "the wait arm must fail readiness before it closes anything"
        );
        assert_eq!(
            probe_status(&probes, "/healthz").await,
            200,
            "liveness must survive the drain triggered by an admin-plane death"
        );

        tokio::time::timeout(Duration::from_secs(20), serving)
            .await
            .expect("the leave completes")
            .expect("the serve task did not panic")
            .expect("a clean admin-plane stop is not an error");
    }
}
