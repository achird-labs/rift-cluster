//! Two OSS bootstrap steps that parse but must also *act* (issue #66).
//!
//! These spawn the real binary rather than driving `compose::start` in-process,
//! and that is not a stylistic choice: `--debug` works by setting `RIFT_DEBUG`,
//! which the engine reads through a process-global `OnceLock` that caches its
//! first read. Two in-process tests wanting different debug states would race
//! each other inside one test binary and the second would silently observe the
//! first's answer. One process per state is the only way to assert on it —
//! upstream's own `issue_359_response_templating_debug` test says the same.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;

use common::ports::reserve_ports as free_ports;

/// A spawned server that is killed when the test ends, however it ends.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A config file with one templated imposter whose body references a function
/// that does not exist. How that unknown token is rendered is the whole
/// observable difference `--debug` makes (upstream #359).
fn templated_configfile(dir: &Path, imposter_port: u16) -> std::path::PathBuf {
    let path = dir.join("imposters.json");
    let body = serde_json::json!({
        "imposters": [{
            "port": imposter_port,
            "protocol": "http",
            "stubs": [{
                "responses": [{
                    "is": { "statusCode": 200, "body": "{{bogusFunction}}" },
                    "_rift": { "templated": true }
                }]
            }]
        }]
    });
    let mut file = std::fs::File::create(&path).expect("write configfile");
    file.write_all(body.to_string().as_bytes())
        .expect("write configfile body");
    path
}

/// Poll the admin port until the server answers, bounded.
async fn wait_until_up(admin: u16) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .get(format!("http://127.0.0.1:{admin}/imposters"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "the server never came up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Issue #66: `--debug` must reach the *engine*, not only the log filter.
///
/// In debug mode an unknown template token is a loud 500 carrying
/// `x-rift-template-error`. Before this, `--debug` set the tracing filter and
/// nothing else, so the same imposter answered differently under `rift` and
/// `rift-ee-server` — the silent divergence this crate exists to prevent.
#[tokio::test]
async fn debug_makes_template_errors_loud() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let [admin, imposter] = free_ports();
    let config = templated_configfile(dir.path(), imposter);

    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
            .args([
                "--debug",
                "--configfile",
                &config.to_string_lossy(),
                "--port",
                &admin.to_string(),
                "--metrics-port",
                "0",
                "--local-only",
            ])
            .spawn()
            .expect("spawn the server"),
    );
    wait_until_up(admin).await;

    let response = reqwest::get(format!("http://127.0.0.1:{imposter}/"))
        .await
        .expect("the imposter answers");
    assert_eq!(
        response.status().as_u16(),
        500,
        "an unknown template token must fail loudly under --debug"
    );
    assert!(
        response.headers().contains_key("x-rift-template-error"),
        "the response must name the template failure: {:?}",
        response.headers()
    );
}

/// The other half of the same contract: without `--debug`, the identical
/// imposter degrades silently to an empty substitution. A separate process, so
/// the cached `RIFT_DEBUG` read cannot leak between the two.
#[tokio::test]
async fn without_debug_template_errors_degrade_silently() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let [admin, imposter] = free_ports();
    let config = templated_configfile(dir.path(), imposter);

    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
            .args([
                "--configfile",
                &config.to_string_lossy(),
                "--port",
                &admin.to_string(),
                "--metrics-port",
                "0",
                "--local-only",
            ])
            .spawn()
            .expect("spawn the server"),
    );
    wait_until_up(admin).await;

    let response = reqwest::get(format!("http://127.0.0.1:{imposter}/"))
        .await
        .expect("the imposter answers");
    assert_eq!(
        response.status().as_u16(),
        200,
        "without --debug an unknown token substitutes empty, it does not fail"
    );
    assert!(
        !response.headers().contains_key("x-rift-template-error"),
        "the debug-only header must not appear without --debug"
    );
    assert_eq!(
        response.text().await.expect("body"),
        "",
        "the unknown token renders as nothing"
    );
}

/// Issue #66: `--log` must actually write a file.
///
/// It parsed and was stored, and nothing read it — so `--log x.log` produced no
/// file and no warning, which is worse than refusing the flag outright.
#[tokio::test]
async fn log_flag_writes_the_logfile() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let logfile = dir.path().join("rift.log");
    let [admin] = free_ports();

    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
            .args([
                "--log",
                &logfile.to_string_lossy(),
                "--port",
                &admin.to_string(),
                "--metrics-port",
                "0",
                "--local-only",
            ])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the server"),
    );
    let mut console_out = server.0.stdout.take().expect("stdout is piped");
    wait_until_up(admin).await;

    // Polled: the appender writes on a background thread, and the guard is
    // deliberately leaked (as upstream does), so there is no flush-on-exit to
    // synchronise against.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::fs::metadata(&logfile).is_ok_and(|m| m.len() > 0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "--log must write to {}, but it stayed absent or empty",
            logfile.display()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // `--log` *adds* a destination, it does not move one. A file layer that
    // replaced the console layer instead of joining it would satisfy the
    // assertion above and still be wrong, so read the console too — which is
    // stdout, where `fmt::layer()` writes by default. Killed first: the child
    // holds the pipe open for as long as it runs.
    drop(server);
    let mut console = String::new();
    console_out
        .read_to_string(&mut console)
        .expect("read the piped console output");
    assert!(
        console.contains("starting Rift Enterprise"),
        "--log must not silence the console: {console}"
    );
}

/// The default invocation writes no logfile at all.
///
/// Upstream's help text mentions a `mb.log` default that its code does not
/// implement, so the tempting "fix" is a clap `default_value` — which would
/// start littering a logfile into every working directory that runs the
/// binary. The server is given the tempdir as its cwd so such a file would
/// have nowhere to hide.
#[tokio::test]
async fn no_log_flag_writes_no_logfile() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let [admin] = free_ports();

    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
            .args([
                "--port",
                &admin.to_string(),
                "--metrics-port",
                "0",
                "--local-only",
            ])
            .current_dir(dir.path())
            .spawn()
            .expect("spawn the server"),
    );
    wait_until_up(admin).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let logs: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read the working directory")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".log"))
        .collect();
    assert!(
        logs.is_empty(),
        "without --log the server must write no logfile, found {logs:?}"
    );
}

/// `--nologfile` wins over `--log`, matching upstream's precedence.
#[tokio::test]
async fn nologfile_beats_log() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let logfile = dir.path().join("rift.log");
    let [admin] = free_ports();

    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
            .args([
                "--log",
                &logfile.to_string_lossy(),
                "--nologfile",
                "--port",
                &admin.to_string(),
                "--metrics-port",
                "0",
                "--local-only",
            ])
            .spawn()
            .expect("spawn the server"),
    );
    wait_until_up(admin).await;

    // Today the appender is never even constructed when `--nologfile` is set,
    // so the absence holds from process start and this wait is not needed to
    // observe it. It is here for the regression where a file layer *is* built
    // despite the flag: that file would appear asynchronously, and an immediate
    // check would miss it.
    //
    // Note this test only means something paired with
    // `log_flag_writes_the_logfile`: on its own it also passes when `--log` is
    // a no-op, which is exactly the bug being fixed.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !logfile.exists(),
        "--nologfile must suppress the file even when --log names one"
    );
}

/// Upstream #827: the PID file belongs to the **serving** path, so a transient
/// subcommand must never write one.
///
/// A real-binary test on purpose. The write is ordered by `main.rs`, which the
/// `bootstrap` unit tests cannot reach — they call `dispatch` directly, so
/// re-hoisting the write above the dispatch would leave every one of them green
/// while the shipped binary clobbered a running server's PID file again. That is
/// the same reasoning `tests/cli.rs` gives for testing the artifact.
///
/// `save` is the cheapest transient subcommand to drive: pointed at a port
/// nothing is listening on it fails fast, and its failure is beside the point —
/// the assertion is on the PID file it must not have created.
#[test]
fn a_transient_subcommand_writes_no_pidfile() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let pidfile = dir.path().join("rift.pid");
    let savefile = dir.path().join("saved.json");
    let [dead] = free_ports();

    let out = Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
        .args([
            "--pidfile",
            &pidfile.to_string_lossy(),
            "--port",
            &dead.to_string(),
            "save",
            "--savefile",
            &savefile.to_string_lossy(),
        ])
        .output()
        .expect("run the binary");

    assert!(
        !out.status.success(),
        "`save` against a port with no server must fail, or this test proves nothing"
    );
    assert!(
        !pidfile.exists(),
        "`save` must not write a PID file: doing so overwrites a running server's"
    );
}
