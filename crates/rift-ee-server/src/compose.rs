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
use rift_cluster::{ClusterDecorator, NodeConfig, NodeIdentity, RaftNode, metrics};
use rift_ee::seams::{ImposterManager, RunningServer, ServerBuilder, TlsDefaults};

use crate::cli::EeCli;
use crate::cluster_api::{self, NodeSlot};
use crate::probes::{self, ProbeListener};
use crate::readiness::{GATE_JOINED, Readiness};

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
    /// Samples the fleet gauges. Aborted on shutdown so it cannot outlive the
    /// node it reads.
    metrics_sampler: Option<tokio::task::JoinHandle<()>>,
    cluster_addr: Option<SocketAddr>,
    readiness: Arc<Readiness>,
    leave_timeout: Duration,
}

impl ComposedServer {
    /// The bound admin API address.
    #[must_use]
    pub fn admin_addr(&self) -> SocketAddr {
        self.server.admin_addr()
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

    /// Serve until the admin API stops.
    pub async fn join(self) -> anyhow::Result<()> {
        self.server.join().await
    }

    /// Stop every listener immediately, without a drain.
    pub async fn shutdown(self) {
        if let Some(sampler) = self.metrics_sampler {
            sampler.abort();
        }
        if let Some(probes) = self.probes {
            probes.shutdown().await;
        }
        self.server.shutdown().await;
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

    /// Serve until `signal` resolves, then leave gracefully.
    ///
    /// The listeners are already accepting in their own tasks by the time
    /// [`start`] returns, so this awaits the signal rather than the admin plane.
    /// Racing the two is not expressible against the upstream `RunningServer`:
    /// both `join` and `shutdown` consume it, so a future built from one cannot
    /// give it back for the other. The consequence is that an admin accept-loop
    /// failure does not end the clustered process on its own — it surfaces as a
    /// failing `/readyz` and a dead admin port instead.
    pub async fn serve_until(self, signal: impl Future<Output = ()>) {
        signal.await;
        self.graceful_leave().await;
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
            metrics_sampler: None,
            cluster_addr: None,
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

    let readiness = Arc::new(Readiness::awaiting([GATE_JOINED]));

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

    let slot = NodeSlot::default();
    let node = match RaftNode::start(NodeConfig {
        node_id: identity.node_id(),
        bind,
        advertise: cli.cluster.cluster_advertise,
        data_dir: state_dir,
        secret: cluster.secret,
        routes: cluster_api::routes(Router::new(), slot.clone(), Arc::clone(&readiness)),
    })
    .await
    {
        Ok(node) => Arc::new(node),
        Err(e) => {
            probes.shutdown().await;
            return Err(anyhow::Error::new(e).context("starting the cluster control-plane node"));
        }
    };
    slot.set(&node)?;

    let cluster_addr = node.advertise_addr();
    let leave_timeout = Duration::from_secs(cli.cluster.cluster_leave_timeout);

    // Everything from here can fail with a live node and, later, a live server.
    // `start` is an embedding seam that callers retry, so a failure must not
    // leave the cluster port bound or the redb state dir locked — it would fail
    // the retry too, with an error that hides the real cause.
    match attach_data_plane(cli, &node, &readiness, accept_runtimes).await {
        Ok(server) => Ok(ComposedServer {
            server,
            probes: Some(probes),
            metrics_sampler: Some(spawn_metrics_sampler(Arc::clone(&node))),
            node: Some(node),
            cluster_addr: Some(cluster_addr),
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
    cli: EeCli,
    node: &Arc<RaftNode>,
    readiness: &Arc<Readiness>,
    accept_runtimes: Vec<tokio::runtime::Handle>,
) -> anyhow::Result<RunningServer> {
    join_or_bootstrap(node, &cli).await?;
    readiness.satisfy(GATE_JOINED);

    let manager = Arc::new(
        cluster_manager(&cli, accept_runtimes)
            .context("building the clustered imposter manager")?,
    );
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

    Ok(server)
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

/// Attach to an existing cluster through the seeds, or found one when the
/// operator has said that is what they want.
async fn join_or_bootstrap(node: &RaftNode, cli: &EeCli) -> anyhow::Result<()> {
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
