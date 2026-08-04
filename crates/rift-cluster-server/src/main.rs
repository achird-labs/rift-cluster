//! `rift-cluster-server` — the RiftCluster binary.
//!
//! A thin caller over `rift_cluster_server`, mirroring the open-source `rift`
//! binary's bootstrap: parse, short-circuit the non-server subcommands, install
//! the crypto provider and tracing, resolve the runtime topology, then compose
//! and serve.

use clap::Parser;
use rift_cluster_base::rift_http_proxy::{healthcheck, runtime, script_cli};
use rift_cluster_base::seams::Commands;
use rift_cluster_server::bootstrap;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose;
use rift_cluster_server::probes;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, Layer, fmt, prelude::*};

fn main() -> anyhow::Result<()> {
    let mut cli = EeCli::parse();

    // Both of these must run before any bootstrap: they are self-contained
    // programs that want only their own exit code, and neither should pay for
    // (or perturb) a server bootstrap — `healthcheck` runs on every container
    // health check. Since upstream #827 the PID file is written on the serving
    // path only, so a transient subcommand can no longer clobber a running
    // server's file; skipping the bootstrap is now the whole reason.
    match cli.oss.command.clone() {
        Some(Commands::Script { action }) => return script_cli::dispatch(action),
        Some(Commands::Healthcheck { url, timeout }) => {
            // With no --url, the target follows the mode (#297): this parse
            // read the same RIFT_* environment the server's own did, and
            // healthcheck_url double-checks a "no" against the node itself,
            // because cluster flags given as command-line arguments never
            // reach a healthcheck exec's environment. Passing Some makes
            // dispatch's own host/port fallback unreachable by construction.
            let url = url.unwrap_or_else(|| {
                probes::healthcheck_url(
                    cli.cluster.cluster,
                    cli.cluster.cluster_probe_bind,
                    &cli.oss.host,
                    cli.oss.port,
                )
            });
            return healthcheck::dispatch(Some(url), &cli.oss.host, cli.oss.port, timeout);
        }
        _ => {}
    }

    // `--debug` is the server-flag spelling of debug mode; `RIFT_DEBUG` is the
    // env-var spelling the engine reads through a `OnceLock`-cached read
    // (mirrors upstream's `rift-http-proxy`). Setting it here, before that
    // first read can happen, makes both spellings equivalent.
    //
    // SAFETY: single-threaded — `main` is not `#[tokio::main]`, no runtime is
    // built until `run`, and no thread has been spawned. Placed before anything
    // calls `rift_debug_env()`, which caches its first read, so the flag cannot
    // be observed inconsistently afterwards.
    if cli.oss.debug {
        unsafe { std::env::set_var("RIFT_DEBUG", "1") };
    }

    // Before tracing, because an rcfile may carry `logLevel` — the open-source
    // binary applies it here for the same reason. Any complaint is held until
    // there is a subscriber, so it lands in the log pipeline and not only on a
    // stderr nobody is collecting.
    let rcfile_warning = bootstrap::apply_rcfile(&mut cli);

    rift_cluster_base::rift_http_proxy::install_default_crypto_provider();
    init_tracing(&cli);
    if let Some(warning) = rcfile_warning {
        warn!("{warning}");
    }
    // `save` and `stop` are complete programs; `restart` stops the old process
    // and then falls through to start a new one.
    if bootstrap::dispatch(&mut cli)? == bootstrap::AfterBootstrap::Done {
        return Ok(());
    }

    // After the dispatch, not before it — the one place every serving entry
    // converges, mirroring upstream #827. Written ahead of it, `restart` recorded
    // its own PID and then SIGTERMed itself, and a transient `save` clobbered a
    // running server's file.
    bootstrap::write_pidfile(&cli)?;

    info!(
        version = %rift_cluster_base::version_banner(),
        cluster = cli.cluster.cluster,
        "starting RiftCluster"
    );

    run(cli)
}

fn run(cli: EeCli) -> anyhow::Result<()> {
    // Topology selection mirrors the open-source binary (RFC-712): clap has
    // already merged the RIFT_RUNTIME env fallback, and the platform gate then
    // downgrades or refuses per-core per its own rules.
    let requested = runtime::RuntimeTopology::resolve(cli.oss.runtime.as_deref(), None)
        .map_err(anyhow::Error::msg)?;
    let (topology, platform_warning) =
        runtime::platform_gate(requested, runtime::current_os()).map_err(anyhow::Error::msg)?;
    if let Some(warning) = platform_warning {
        warn!("{warning}");
    }
    info!("Runtime topology: {}", topology.describe());

    match topology {
        runtime::RuntimeTopology::WorkStealing => {
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            tokio_runtime.block_on(serve(cli, Vec::new()))
        }
        runtime::RuntimeTopology::PerCore { workers } => {
            // Unreachable with --cluster: the startup guards refuse the pairing
            // (D-14). This is the open-source path, unchanged.
            let control = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            let workers = runtime::WorkerSet::spawn(workers, cli.oss.runtime_affinity)?;
            let total = workers.worker_count();
            let alive = control.block_on(workers.ping_all());
            if alive.len() != total {
                workers.shutdown();
                anyhow::bail!(
                    "per-core bootstrap: only {}/{total} workers came up; refusing to start degraded",
                    alive.len()
                );
            }
            info!("Per-core workers up: {}", alive.len());
            let result = control.block_on(serve(cli, workers.handles()));
            workers.shutdown();
            result
        }
    }
}

async fn serve(cli: EeCli, accept_runtimes: Vec<tokio::runtime::Handle>) -> anyhow::Result<()> {
    let clustered = cli.cluster.cluster;
    let server = compose::start_with_runtimes(cli, accept_runtimes).await?;
    info!(admin = %server.admin_addr(), "admin API listening");
    if let Some(probes) = server.probe_addr() {
        info!(%probes, "probes listening");
    }
    if let Some(cluster) = server.cluster_addr() {
        info!(%cluster, "cluster port listening");
    }

    if !clustered {
        // Without clustering there is nothing to leave gracefully; keep the
        // open-source binary's behaviour exactly, including ending when the
        // admin accept loop does.
        return server.join().await;
    }

    // SIGTERM is the orchestrator's "you are going away" and must start the
    // graceful leave rather than drop connections; the pod's grace period is
    // what bounds it, so set it to at least twice --cluster-leave-timeout.
    //
    // The admin plane is raced against the signal, so an accept loop that dies
    // on its own ends this node too — and its error is what `serve` returns.
    server
        .serve_until(async {
            termination_signal().await;
            info!("termination signal received; beginning graceful leave");
        })
        .await
}

#[cfg(unix)]
async fn termination_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sigterm) => sigterm,
        Err(e) => {
            // Without SIGTERM there is no graceful leave, and pretending
            // otherwise would let an operator configure a grace period that
            // never gets used.
            tracing::error!(error = %e, "cannot listen for SIGTERM; graceful leave is unavailable");
            return std::future::pending().await;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => {}
        result = tokio::signal::ctrl_c() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "ctrl-c handler failed");
            }
        }
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %e, "ctrl-c handler failed");
        std::future::pending::<()>().await;
    }
}

fn init_tracing(cli: &EeCli) {
    let level = match cli.oss.loglevel.to_lowercase().as_str() {
        "debug" => "debug",
        "warn" | "warning" => "warn",
        "error" => "error",
        _ => "info",
    };
    let filter = if cli.oss.debug { "debug" } else { level };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));

    // `--nologfile` wins over `--log`, matching upstream. A path with no file
    // name yields no layer rather than a logfile named after a directory.
    //
    // Note this is not a general guard: `rolling::never` still panics if the
    // directory cannot be created (`--log /nonexistent-root/x.log`). Upstream
    // behaves identically, and diverging here would be the drift this crate
    // exists to prevent, so it is left alone deliberately.
    let file_layer: Option<Box<dyn Layer<_> + Send + Sync>> = if !cli.oss.nologfile {
        cli.oss.log.as_ref().and_then(|log_path| {
            let dir = log_path.parent().unwrap_or(std::path::Path::new("."));
            let filename = log_path.file_name()?.to_string_lossy().into_owned();
            let file_appender = tracing_appender::rolling::never(dir, filename);
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            // Leaked so the writer outlives this function, mirroring upstream.
            // Returning the guard for `main` to hold would work here — this
            // binary does have shutdown paths — but would diverge; the cost is
            // that the tail of the log is not flushed on exit.
            Box::leak(Box::new(guard));
            Some(fmt::layer().with_writer(non_blocking).boxed())
        })
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .with(file_layer)
        .init();
}
