//! `rift-ee-server` — the Rift Enterprise binary.
//!
//! A thin caller over `rift_ee_server`, mirroring the open-source `rift`
//! binary's bootstrap: parse, short-circuit the non-server subcommands, install
//! the crypto provider and tracing, resolve the runtime topology, then compose
//! and serve.

use clap::Parser;
use rift_ee::rift_http_proxy::{healthcheck, runtime, script_cli};
use rift_ee::seams::Commands;
use rift_ee_server::cli::EeCli;
use rift_ee_server::compose;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

fn main() -> anyhow::Result<()> {
    let cli = EeCli::parse();

    // Both of these must run before any bootstrap: `script` wants only its own
    // exit code, and `healthcheck` would otherwise clobber the running server's
    // --pidfile with the probe's own PID on every container health check.
    match cli.oss.command.clone() {
        Some(Commands::Script { action }) => return script_cli::dispatch(action),
        Some(Commands::Healthcheck { url, timeout }) => {
            return healthcheck::dispatch(url, &cli.oss.host, cli.oss.port, timeout);
        }
        _ => {}
    }

    rift_ee::rift_http_proxy::install_default_crypto_provider();
    init_tracing(&cli);

    let cli = match unsupported_reason(&cli) {
        Some(reason) => anyhow::bail!("{reason}"),
        None => cli,
    };

    info!(
        edition = rift_ee::EDITION,
        version = rift_ee::version(),
        cluster = cli.cluster.cluster,
        "starting Rift Enterprise"
    );

    run(cli)
}

/// Everything the open-source binary supports that this one does not (yet).
///
/// Declining loudly is the point: each of these is implemented in a private
/// function of the open-source binary's `main.rs` rather than behind a library
/// seam, and copying it here would fork behaviour that is meant to stay shared.
/// The gap is tracked upstream as a bootstrap-seam request; until it lands, the
/// `rift` binary is the answer, and an operator is told so rather than getting
/// silently different behaviour.
fn unsupported_reason(cli: &EeCli) -> Option<String> {
    if cli.oss.rcfile.is_some() {
        return Some(
            "--rcfile is not supported by rift-ee-server (the open-source binary owns rcfile \
             parsing); pass the equivalent flags directly"
                .to_owned(),
        );
    }
    match &cli.oss.command {
        Some(Commands::Stop { .. }) => Some("`stop`"),
        Some(Commands::Restart { .. }) => Some("`restart`"),
        Some(Commands::Save { .. }) => Some("`save`"),
        _ => None,
    }
    .map(|subcommand| {
        format!(
            "{subcommand} is not supported by rift-ee-server; use the open-source `rift` binary \
             for it (it drives a running server over its admin API and PID file, and does not \
             depend on the enterprise composition)"
        )
    })
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
    server
        .serve_until(async {
            termination_signal().await;
            info!("termination signal received; beginning graceful leave");
        })
        .await;
    Ok(())
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
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();
}
