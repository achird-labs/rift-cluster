//! Pre-serve bootstrap: rcfile defaults, the PID file, and the non-server
//! subcommands (issue #43).
//!
//! Every step here delegates to `rift_http_proxy::bootstrap`, which upstream
//! promoted out of the `rift` binary's private `main.rs` for exactly this reason
//! (upstream #807). The order below mirrors the open-source binary's `main`
//! step for step — this crate's promise is that `--cluster` off behaves
//! identically, and bootstrap ordering is observable (an rcfile that sets
//! `logLevel` must land *before* tracing initialises, or the setting is lost).
//!
//! It lives in the library rather than `main.rs` so it can be driven by tests
//! without spawning a process, the same split the rest of this crate uses.

use std::path::Path;

use rift_ee::rift_http_proxy::bootstrap::{apply_rcfile_defaults, save_imposters, stop_server};
use rift_ee::seams::Commands;
use tracing::info;

use crate::cli::EeCli;

/// Whether the process still has a server to start once the bootstrap is done.
///
/// A two-state enum rather than a `bool` so a caller cannot mix up which way
/// round the flag runs: `stop` and `save` are complete programs on their own,
/// `restart` deliberately is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum AfterBootstrap {
    /// Go on and start the server.
    Serve,
    /// The subcommand was the whole job; exit successfully.
    Done,
}

/// Apply `--rcfile` defaults to the flattened open-source CLI.
///
/// Non-fatal by design, matching the open-source binary: a bad rcfile warns and
/// startup continues with the flags as given. Changing that to a hard failure
/// here would be a behaviour fork, not a hardening.
///
/// Returns the warning text rather than logging it, because this has to run
/// before tracing is initialised — an rcfile may carry `logLevel`. `eprintln!`
/// alone would be the one piece of operational signal in this binary that never
/// reaches the log pipeline, so the caller re-emits it through `tracing` as soon
/// as there is a subscriber to receive it.
#[must_use]
pub fn apply_rcfile(cli: &mut EeCli) -> Option<String> {
    let rcfile = cli.oss.rcfile.clone()?;
    let e = apply_rcfile_defaults(&mut cli.oss, &rcfile).err()?;
    let warning = format!("failed to load --rcfile {rcfile:?}: {e}");
    eprintln!("Warning: {warning}");
    Some(warning)
}

/// Write this process's PID to `--pidfile`, if one was given.
///
/// Placed here, before the subcommand dispatch, purely to match the open-source
/// binary — the ordering is upstream's and is reproduced rather than improved.
/// Note what that means: the global `--pidfile` and the `stop`/`restart`
/// subcommand's own `--pidfile` are *separate* clap fields, so this does not
/// give `restart` a PID file for the server it goes on to start. See the
/// caveats in `docs/rift-ee-server.md`.
pub fn write_pidfile(cli: &EeCli) -> anyhow::Result<()> {
    let Some(pidfile) = cli.oss.pidfile.as_ref() else {
        return Ok(());
    };
    let pid = std::process::id();
    std::fs::write(pidfile, pid.to_string())?;
    info!("Wrote PID {} to {:?}", pid, pidfile);
    Ok(())
}

/// Reject a PID file body that names something other than a single process.
///
/// Upstream parses straight into `libc::kill`, where `0` means "every process in
/// my process group" and `-1` means "every process I am allowed to signal" — so
/// a truncated or hand-edited PID file turns a stop into a broadcast. Tracked
/// upstream as rift#822; this goes away when that lands.
///
/// Pure, and separate from [`stop_via_pidfile`], so the dangerous values can be
/// tested without a `kill` anywhere on the path. A table-driven test that reached
/// the real signal would fire `kill(0)` at its own process group the moment this
/// check regressed — destroying the CI job rather than reporting a failure.
fn validate_pid(raw: &str) -> anyhow::Result<i32> {
    let pid: i32 = raw
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("PID file does not contain a PID: {e}"))?;
    anyhow::ensure!(
        pid > 0,
        "PID file contains {pid}, which would signal a process group rather than a server; \
         refusing to act on it"
    );
    Ok(pid)
}

/// Stop the server recorded in `pidfile`.
///
/// The guard is **advisory**: upstream's `stop_server` re-reads and re-parses the
/// file itself, so anything able to rewrite it between the two reads is still
/// able to choose the PID. It defends a hand-edited or truncated file, which is
/// the realistic case, not a hostile writer — closing that needs upstream to
/// validate the PID it actually signals (rift#822).
fn stop_via_pidfile(pidfile: &Path) -> anyhow::Result<()> {
    // Upstream's own not-found message, kept ahead of the read so `stop`'s
    // stderr stays identical to the open-source binary's.
    anyhow::ensure!(pidfile.exists(), "PID file not found: {pidfile:?}");
    let raw = std::fs::read_to_string(pidfile)
        .map_err(|e| anyhow::anyhow!("cannot read PID file {pidfile:?}: {e}"))?;
    validate_pid(&raw).map_err(|e| anyhow::anyhow!("{pidfile:?}: {e}"))?;
    stop_server(pidfile)
}

/// Run whichever non-server subcommand was asked for.
///
/// Matched variant by variant with no wildcard, deliberately. This crate exists
/// to track the open-source binary, so a subcommand added upstream must break
/// this build on the next pin bump rather than quietly degrade to "start a
/// server and ignore the arguments" — `tests/cli.rs` compares flag *names* and
/// would stay green through exactly that.
pub fn dispatch(cli: &EeCli) -> anyhow::Result<AfterBootstrap> {
    match &cli.oss.command {
        Some(Commands::Stop { pidfile }) => {
            stop_via_pidfile(pidfile)?;
            Ok(AfterBootstrap::Done)
        }
        Some(Commands::Restart { pidfile }) => {
            stop_via_pidfile(pidfile)?;
            Ok(AfterBootstrap::Serve)
        }
        Some(Commands::Save {
            savefile,
            remove_proxies,
        }) => {
            // `save_imposters` builds its own tokio runtime and blocks on it, so
            // it panics if called from inside one. Checked rather than merely
            // documented: this module is public and the crate advertises itself
            // as in-process test-drivable, so the caller may well be async.
            anyhow::ensure!(
                tokio::runtime::Handle::try_current().is_err(),
                "`save` must be dispatched from sync context: it builds its own runtime"
            );
            save_imposters(&cli.oss.host, cli.oss.port, savefile, *remove_proxies)?;
            Ok(AfterBootstrap::Done)
        }
        // Parses (the CLI is a superset by construction) but is not wired up.
        // Failing here is the point: falling through would start an empty server
        // and silently discard the `--configfile` the operator asked to replay.
        Some(Commands::Replay { configfile }) => anyhow::bail!(
            "`replay` is not implemented by rift-ee-server yet; it would ignore {configfile:?}. \
             Pass --configfile {configfile:?} to start instead, or use the open-source `rift` \
             binary."
        ),
        // Both must run before any bootstrap — `script` wants only its own exit
        // code and `healthcheck` would clobber a running server's --pidfile — so
        // the caller dispatches them and they can never arrive here.
        Some(Commands::Script { .. } | Commands::Healthcheck { .. }) => Err(anyhow::anyhow!(
            "internal error: `script`/`healthcheck` must be dispatched before the bootstrap"
        )),
        Some(Commands::Start) | None => Ok(AfterBootstrap::Serve),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;
    use tempfile::TempDir;

    use super::{AfterBootstrap, apply_rcfile, dispatch, write_pidfile};
    use crate::cli::EeCli;

    fn cli(args: &[&str]) -> EeCli {
        let mut all = vec!["rift-ee-server"];
        all.extend_from_slice(args);
        EeCli::try_parse_from(all).expect("parses")
    }

    fn write(dir: &TempDir, name: &str, body: &str) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write fixture");
        path
    }

    /// AC1: rcfile values fill in fields the operator left at their defaults.
    #[test]
    fn rcfile_fills_defaults() {
        let dir = TempDir::new().expect("tempdir");
        let rc = write(&dir, "rc.json", r#"{"port": 4321, "logLevel": "warn"}"#);
        let mut c = cli(&["--rcfile", &rc.to_string_lossy()]);

        assert!(
            apply_rcfile(&mut c).is_none(),
            "a good rcfile warns about nothing"
        );

        assert_eq!(c.oss.port, 4321);
        assert_eq!(c.oss.loglevel, "warn");
    }

    /// AC1: an explicit flag outranks the rcfile — otherwise a stale rcfile would
    /// silently override what the operator typed on the command line.
    #[test]
    fn rcfile_does_not_override_an_explicit_flag() {
        let dir = TempDir::new().expect("tempdir");
        let rc = write(&dir, "rc.json", r#"{"port": 4321}"#);
        let mut c = cli(&["--port", "9999", "--rcfile", &rc.to_string_lossy()]);

        assert!(apply_rcfile(&mut c).is_none());

        assert_eq!(c.oss.port, 9999);
    }

    /// AC2: a broken rcfile warns and startup continues, matching the OSS binary.
    #[test]
    fn rcfile_invalid_is_not_fatal() {
        let dir = TempDir::new().expect("tempdir");
        let rc = write(&dir, "rc.json", "not json at all");
        let mut c = cli(&["--port", "8080", "--rcfile", &rc.to_string_lossy()]);

        let warning = apply_rcfile(&mut c).expect("a malformed rcfile must be reported");
        assert!(
            warning.contains("rc.json"),
            "the warning must name the file: {warning}"
        );

        assert_eq!(c.oss.port, 8080, "a bad rcfile must not disturb the flags");
    }

    /// AC2: a missing rcfile is likewise non-fatal.
    #[test]
    fn rcfile_missing_is_not_fatal() {
        let dir = TempDir::new().expect("tempdir");
        let mut c = cli(&[
            "--rcfile",
            &dir.path().join("absent.json").to_string_lossy(),
        ]);

        assert!(
            apply_rcfile(&mut c).is_some(),
            "an rcfile the operator named but that does not exist must be reported, not ignored"
        );
    }

    /// AC6: the PID file is what makes `stop`/`restart` mean anything.
    #[test]
    fn pidfile_is_written() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("rift.pid");
        let c = cli(&["--pidfile", &path.to_string_lossy()]);

        write_pidfile(&c).expect("pidfile written");

        let written = std::fs::read_to_string(&path).expect("read back");
        assert_eq!(written.trim(), std::process::id().to_string());
    }

    #[test]
    fn pidfile_absent_is_not_an_error() {
        write_pidfile(&cli(&[])).expect("no --pidfile is not a failure");
    }

    /// AC3: `stop` signals the recorded process and clears the PID file.
    ///
    /// A real child process rather than a fake PID: the whole point is that a
    /// signal is delivered, and a PID that belongs to nothing would pass a test
    /// that never sent one.
    #[cfg(unix)]
    #[test]
    fn stop_signals_and_removes_pidfile() {
        let dir = TempDir::new().expect("tempdir");
        // Ignores SIGTERM's default only if we asked it to; `sleep` does not, so
        // delivery is observable as the child dying early.
        let mut child = std::process::Command::new("sleep")
            .arg("120")
            .spawn()
            .expect("spawn a child to stop");
        let pidfile = write(&dir, "rift.pid", &child.id().to_string());

        super::stop_via_pidfile(&pidfile).expect("stop succeeds");

        let status = child.wait().expect("reap the child");
        assert!(
            !status.success(),
            "the child must have been terminated by the signal, not exited normally"
        );
        assert!(
            !pidfile.exists(),
            "a completed stop must not leave a stale PID file behind"
        );
    }

    /// AC4: `restart` stops the old process but reports that a server still has
    /// to be started — that fall-through is the whole difference from `stop`.
    #[cfg(unix)]
    #[test]
    fn restart_stops_then_continues() {
        let dir = TempDir::new().expect("tempdir");
        let mut child = std::process::Command::new("sleep")
            .arg("120")
            .spawn()
            .expect("spawn a child to restart");
        let pidfile = write(&dir, "rift.pid", &child.id().to_string());
        let c = cli(&["restart", "--pidfile", &pidfile.to_string_lossy()]);

        assert_eq!(
            dispatch(&c).expect("restart dispatches"),
            AfterBootstrap::Serve,
            "restart must fall through to the start path"
        );

        let status = child.wait().expect("reap the child");
        assert!(!status.success(), "restart must stop the old process");
    }

    /// The same journey through `dispatch`'s `Stop` arm, which must instead
    /// report that there is nothing left to start.
    #[cfg(unix)]
    #[test]
    fn dispatch_stop_signals_and_reports_done() {
        let dir = TempDir::new().expect("tempdir");
        let mut child = std::process::Command::new("sleep")
            .arg("120")
            .spawn()
            .expect("spawn a child to stop");
        let pidfile = write(&dir, "rift.pid", &child.id().to_string());
        let c = cli(&["stop", "--pidfile", &pidfile.to_string_lossy()]);

        assert_eq!(dispatch(&c).expect("stop dispatches"), AfterBootstrap::Done);

        let status = child.wait().expect("reap the child");
        assert!(
            !status.success(),
            "stop must terminate the recorded process"
        );
    }

    /// AC7 (rift#822): the values that would turn a stop into a broadcast.
    ///
    /// Deliberately against the pure validator, never `stop_via_pidfile`: if this
    /// check ever regressed, a test that went on to the real `kill` would fire
    /// `kill(0, SIGTERM)` at its own process group — taking down the CI job
    /// instead of reporting a failed assertion.
    #[test]
    fn validate_pid_refuses_anything_that_is_not_one_process() {
        for body in ["0", "-1", "-4321", "", "   ", "not-a-pid", "12x"] {
            let err = super::validate_pid(body)
                .expect_err("a PID that is not a single live process must be refused");
            let msg = err.to_string();
            assert!(
                msg.contains("refusing") || msg.contains("does not contain a PID"),
                "unhelpful refusal for {body:?}: {msg}"
            );
        }
    }

    #[test]
    fn validate_pid_accepts_a_real_pid() {
        let pid = super::validate_pid(&format!(" {} \n", std::process::id())).expect("accepted");
        assert_eq!(pid, std::process::id().cast_signed());
    }

    /// A refused stop leaves the PID file alone — an operator has to be able to
    /// see what was in it. Uses an unparseable body, which upstream's own parse
    /// would reject too, so no `kill` is reachable even if the guard regressed.
    #[test]
    fn refused_stop_leaves_the_pidfile_for_inspection() {
        let dir = TempDir::new().expect("tempdir");
        let pidfile = write(&dir, "junk.pid", "not-a-pid");

        super::stop_via_pidfile(&pidfile).expect_err("an unparseable PID file must be refused");

        assert!(
            pidfile.exists(),
            "a refused stop must not delete the evidence"
        );
    }

    /// Upstream's wording for a missing PID file, which `stop`'s stderr is.
    #[test]
    fn missing_pidfile_reports_upstreams_message() {
        let dir = TempDir::new().expect("tempdir");
        let err = super::stop_via_pidfile(&dir.path().join("absent.pid"))
            .expect_err("a missing PID file is an error");
        assert!(
            err.to_string().contains("PID file not found"),
            "diverged from upstream's message: {err}"
        );
    }

    /// `replay` must fail loudly rather than start an empty server and discard
    /// the configfile the operator asked to replay.
    #[test]
    fn replay_is_refused_rather_than_silently_ignored() {
        let c = cli(&["replay", "--configfile", "saved.json"]);
        let err = dispatch(&c).expect_err("replay is not wired up and must say so");
        assert!(
            err.to_string().contains("saved.json"),
            "the refusal must name the file being ignored: {err}"
        );
    }

    /// AC5: `save` reaches a running admin API and writes its replayable config.
    ///
    /// Deliberately a plain `#[test]`, not `#[tokio::test]`: `save_imposters`
    /// builds its own runtime and panics inside one, and proving that the wiring
    /// calls it from sync context is half the point of the issue. The server
    /// therefore runs on its own thread with its own runtime.
    #[test]
    fn save_writes_imposters_from_sync_context() {
        let dir = TempDir::new().expect("tempdir");
        let savefile = dir.path().join("mb.json");

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let server = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let composed = crate::compose::start(
                    EeCli::try_parse_from(["rift-ee-server", "--port", "0", "--host", "127.0.0.1"])
                        .expect("parses"),
                )
                .await
                .expect("un-clustered server starts");
                // The port the server actually bound, not one reserved and
                // released beforehand — that gap is a real race under a parallel
                // test run.
                ready_tx
                    .send(composed.admin_addr().port())
                    .expect("signal ready");
                let _ = tokio::task::spawn_blocking(move || stop_rx.recv()).await;
                composed.shutdown().await;
            });
        });

        let port = ready_rx.recv().expect("server came up");
        let c = cli(&[
            "--port",
            &port.to_string(),
            "--host",
            "127.0.0.1",
            "save",
            "--savefile",
            &savefile.to_string_lossy(),
        ]);

        let outcome = dispatch(&c).expect("save reaches the admin API from sync context");

        assert_eq!(outcome, AfterBootstrap::Done, "save is a complete program");
        let body = std::fs::read_to_string(&savefile).expect("savefile written");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("the admin API's response is JSON");
        assert!(
            parsed.get("imposters").is_some(),
            "expected a replayable imposter document, got: {body}"
        );

        let _ = stop_tx.send(());
        let _ = server.join();
    }
}
