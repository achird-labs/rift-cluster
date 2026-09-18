//! `rift-cluster-server` — the RiftCluster binary.
//!
//! A thin caller over `rift_cluster_server`, mirroring the open-source `rift`
//! binary's bootstrap: parse, short-circuit the non-server subcommands, install
//! the crypto provider and tracing, resolve the runtime topology, then compose
//! and serve.

use anyhow::Context as _;
use clap::Parser as _;
use rift_cluster_base::rift_http_proxy::bootstrap::log_filter;
use rift_cluster_base::rift_http_proxy::{healthcheck, runtime, script_cli};
use rift_cluster_base::seams::Commands;
use rift_cluster_server::bootstrap;
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose;
use rift_cluster_server::probes;
use tracing::{info, warn};
use tracing_subscriber::{Layer, fmt, prelude::*};

fn main() -> anyhow::Result<()> {
    let mut cli = EeCli::parse();

    // `script` is a self-contained program that wants only its own exit code,
    // and reads no host or port, so it runs ahead of everything — including the
    // rcfile. (Upstream's order, step for step.)
    if let Some(Commands::Script { action }) = cli.oss.command.clone() {
        return script_cli::dispatch(action);
    }

    // The rcfile comes next: before `healthcheck`, and before tracing.
    //
    // Before tracing, because an rcfile may carry `logLevel`. A refused rcfile
    // aborts startup (D-77): upstream #1114 made the refusal whole-file, so
    // continuing would run with none of its keys — including a
    // `requireAdminAuth` the operator asked for. `?` rather than `{e}` keeps the
    // whole chain: `{e}` names the file but stops at that one layer and drops
    // serde's line and column beneath it. `tests/cli.rs` pins this `?` against
    // the real binary. Unsupported keys are only advisory, so they are also
    // re-emitted below, once there is a subscriber, for a pipeline that is not
    // collecting stderr.
    //
    // Before `healthcheck` (#593, upstream #1133), which computes its target from
    // `--host`/`--port`: a deployment that sets the admin port in an rcfile ran a
    // server on that port and a probe that knocked on 2525 forever. This is not
    // the server bootstrap the probe skips — that is the crypto provider and the
    // subscriber. Reading one small file is the one step whose *output* the probe
    // depends on, and a refused rcfile refuses the probe too: a server started
    // with that file would not have started either, so "unhealthy" is the true
    // answer.
    let rcfile_warnings = bootstrap::apply_rcfile(&mut cli)?;

    // `healthcheck` runs on every container health check, so it must not pay for
    // (or perturb) a server bootstrap. Since upstream #827 the PID file is written
    // on the serving path only, so skipping the bootstrap is now the whole reason.
    if let Some(Commands::Healthcheck { url, timeout }) = cli.oss.command.clone() {
        // With no --url, the target follows the mode (#297): this parse read the
        // same RIFT_* environment the server's own did, and healthcheck_target
        // double-checks a "no" against the node itself, because cluster flags
        // given as command-line arguments never reach a healthcheck exec's
        // environment.
        //
        // The key goes where upstream's dispatch sends it (#1154): to the admin
        // plane it derives from --host/--port, and never to an explicit --url,
        // which it withholds from and names in a 401 verdict. The probe listener
        // is unauthenticated, so the admin secret is not handed to it at all.
        let api_key = cli.oss.api_key.as_deref();
        let (url, key) = match url {
            Some(explicit) => (Some(explicit), api_key),
            None => match probes::healthcheck_target(
                cli.cluster.cluster,
                cli.cluster.cluster_probe_bind,
            ) {
                probes::HealthcheckTarget::ProbeListener(probe) => (Some(probe), None),
                probes::HealthcheckTarget::AdminPlane => (None, api_key),
            },
        };
        return healthcheck::dispatch(url, &cli.oss.host, cli.oss.port, timeout, key);
    }

    // `--debug` is the server-flag spelling of debug mode; `RIFT_DEBUG` is the
    // env-var spelling the engine reads through a `OnceLock`-cached read
    // (mirrors upstream's `rift-http-proxy`). Setting it here, before that
    // first read can happen, makes both spellings equivalent.
    //
    // SAFETY: single-threaded — `main` is not `#[tokio::main]`, no runtime is
    // built until `run`, and no thread has been spawned. Placed before anything
    // calls `rift_debug_env()`, which caches its first read, so the flag cannot
    // be observed inconsistently afterwards. (Neither the rcfile nor the probe
    // above reads it.)
    if cli.oss.debug {
        unsafe { std::env::set_var("RIFT_DEBUG", "1") };
    }

    rift_cluster_base::rift_http_proxy::install_default_crypto_provider();
    // Held for the life of `main` and dropped on every exit path, `?` included:
    // dropping it is what flushes the log file's non-blocking writer (#627,
    // upstream #1155).
    let _log_guard = init_tracing(&cli)?;
    for warning in rcfile_warnings {
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

    // The server removes the PID file it wrote on the way out, success and error
    // alike (#627, upstream #1155); a Ctrl+C, a plain `kill` or a `docker stop`
    // used to leave a stale one behind.
    let written_pidfile = cli.oss.pidfile.clone();
    let result = run(cli);
    if let Some(pidfile) = written_pidfile {
        bootstrap::remove_own_pidfile(&pidfile);
    }
    info!("stopped");
    result
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
            let result = tokio_runtime.block_on(serve(cli, Vec::new()));
            // Bounded, not an implicit drop: a script still running on a
            // blocking thread would otherwise hold a graceful shutdown for
            // ever. See `runtime::BLOCKING_DRAIN` (upstream #1155).
            tokio_runtime.shutdown_timeout(runtime::BLOCKING_DRAIN);
            result
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
            control.shutdown_timeout(runtime::BLOCKING_DRAIN);
            result
        }
    }
}

async fn serve(cli: EeCli, accept_runtimes: Vec<tokio::runtime::Handle>) -> anyhow::Result<()> {
    // Installed before anything starts, so a signal that lands during startup is
    // held rather than dropped — as a container's PID 1, a SIGTERM with no
    // handler is discarded by the kernel (D-89, upstream #1155).
    let mut signals =
        TerminationSignals::install().context("installing the termination-signal handler")?;
    let clustered = cli.cluster.cluster;
    // Startup is raced against the signal rather than made to finish first: a
    // clustered start can spend up to 30 s retrying its seeds or waiting for a
    // leader, and a node told to stop in that window should stop, not join and
    // then leave — which would also outrun `stop`'s ceiling (D-88). Nothing is
    // serving yet, so abandoning the start is the old signal-kills-the-process
    // outcome: crash-equivalent, which a restarting member already tolerates by
    // resuming from its durable log — minus the dependence on not being PID 1
    // (D-89).
    let server = tokio::select! {
        biased;
        signal = signals.recv() => {
            info!(signal, "termination signal received during startup; not starting");
            return Ok(());
        }
        server = compose::start_with_runtimes(cli, accept_runtimes) => server?,
    };
    info!(admin = %server.admin_addr(), "admin API listening");
    if let Some(probes) = server.probe_addr() {
        info!(%probes, "probes listening");
    }
    if let Some(cluster) = server.cluster_addr() {
        info!(%cluster, "cluster port listening");
    }

    // One path for both modes. The admin plane is raced against the signal, so
    // an accept loop that dies on its own ends this node too — and its error is
    // what `serve` returns. Clustered, the signal starts the graceful leave
    // (RFC-001 §7.1.2); size the pod's grace period to at least twice
    // --cluster-leave-timeout. Unclustered there is nothing to leave: the leave
    // window is zero, and what remains is upstream's own shutdown — stop
    // accepting, then `RunningServer::shutdown`. Never the manager's shutdown,
    // which would delete every imposter and unlink its --datadir file: an
    // unclustered `ComposedServer` holds no manager of its own.
    server
        .serve_until(async move {
            let signal = signals.recv().await;
            if clustered {
                info!(
                    signal,
                    "termination signal received; beginning graceful leave"
                );
            } else {
                info!(signal, "termination signal received; shutting down");
            }
        })
        .await
}

/// The termination signals this binary handles: SIGTERM and SIGINT on unix,
/// Ctrl+C elsewhere.
struct TerminationSignals {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
}

impl TerminationSignals {
    /// Register the handlers now. A failed install refuses startup: a server
    /// that silently cannot be stopped gracefully is the defect being fixed.
    fn install() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                term: signal(SignalKind::terminate())?,
                int: signal(SignalKind::interrupt())?,
            })
        }
        #[cfg(windows)]
        {
            Ok(Self {
                ctrl_c: tokio::signal::windows::ctrl_c()?,
            })
        }
    }

    /// Resolve on the first signal, naming it for the log.
    async fn recv(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.term.recv() => "SIGTERM",
                _ = self.int.recv() => "SIGINT",
            }
        }
        #[cfg(windows)]
        {
            self.ctrl_c.recv().await;
            "Ctrl+C"
        }
    }
}

/// Install the subscriber. Returns the file writer's guard, if there is a log
/// file, for `main` to hold: dropping it flushes the writer.
fn init_tracing(
    cli: &EeCli,
) -> anyhow::Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
    // Upstream's rules, called rather than copied (#594, upstream #1134). This
    // used to be a copy of upstream's `main.rs`, and both copies fell through to
    // `info` for a level that does not exist — `trace` included — and mistook an
    // unparseable `RUST_LOG` for an unset one. The seam refuses both, so the two
    // binaries now agree by construction rather than by keeping two copies in
    // step. `--debug` is handled inside it.
    let env_filter = log_filter(&cli.oss)?;

    // `--nologfile` wins over `--log`, matching upstream. A path with no file
    // name yields no layer rather than a logfile named after a directory.
    //
    // Note this is not a general guard: `rolling::never` still panics if the
    // directory cannot be created (`--log /nonexistent-root/x.log`). Upstream
    // behaves identically, and diverging here would be the drift this crate
    // exists to prevent, so it is left alone deliberately.
    let mut log_guard = None;
    let file_layer: Option<Box<dyn Layer<_> + Send + Sync>> = if !cli.oss.nologfile {
        cli.oss.log.as_ref().and_then(|log_path| {
            let dir = log_path.parent().unwrap_or(std::path::Path::new("."));
            let filename = log_path.file_name()?.to_string_lossy().into_owned();
            let file_appender = tracing_appender::rolling::never(dir, filename);
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            // Returned for `main` to hold, as upstream has since #1155. It
            // used to be leaked to mirror upstream's old leak, so lines still
            // queued when the process ended could be lost.
            log_guard = Some(guard);
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
    Ok(log_guard)
}
