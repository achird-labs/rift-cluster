//! The CLI is an open-source superset (issue #10 AC1/AC2): every flag and
//! subcommand the `rift` binary accepts must still parse here, and the cluster
//! flags must be validated before anything binds.

use clap::{CommandFactory, Parser};
use rift_cluster::ConfigError;
use rift_cluster_base::seams::Cli as OssCli;
use rift_cluster_server::cli::EeCli;

/// Long-flag names (`--foo`) a command accepts, ignoring ordering.
fn long_flags(command: &clap::Command) -> Vec<String> {
    let mut names: Vec<String> = command
        .get_arguments()
        .filter_map(|arg| arg.get_long().map(str::to_owned))
        .collect();
    names.sort();
    names
}

fn subcommand_names(command: &clap::Command) -> Vec<String> {
    let mut names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn ee_cli_accepts_every_oss_flag() {
    let oss = OssCli::command();
    let ee = EeCli::command();
    let ee_flags = long_flags(&ee);
    let missing: Vec<_> = long_flags(&oss)
        .into_iter()
        .filter(|flag| !ee_flags.contains(flag))
        .collect();
    assert!(
        missing.is_empty(),
        "cluster CLI is missing open-source flags: {missing:?}"
    );
}

#[test]
fn ee_cli_accepts_every_oss_subcommand() {
    let oss = OssCli::command();
    let ee = EeCli::command();
    assert_eq!(subcommand_names(&ee), subcommand_names(&oss));
}

#[test]
fn ee_cli_adds_the_cluster_flags() {
    let ee = EeCli::command();
    let flags = long_flags(&ee);
    for expected in [
        "cluster",
        "cluster-bind",
        "cluster-bind-public-ok",
        "cluster-advertise",
        "cluster-seeds",
        "cluster-allow-solo",
        "cluster-secret",
        "cluster-secret-file",
        "cluster-insecure",
        "cluster-state-dir",
        "cluster-node-name",
        "cluster-leave-timeout",
        "cluster-probe-bind",
    ] {
        assert!(
            flags.iter().any(|f| f == expected),
            "missing --{expected} in {flags:?}"
        );
    }
}

/// clap's own consistency assertions (duplicate flags, bad defaults, conflicting
/// ids) — flattening two arg sets is exactly where those break.
#[test]
fn ee_cli_is_internally_consistent() {
    EeCli::command().debug_assert();
}

fn parse(args: &[&str]) -> EeCli {
    EeCli::try_parse_from(args).expect("parses")
}

#[test]
fn without_the_master_switch_nothing_cluster_related_is_required() {
    let cli = parse(&["rift-cluster-server"]);
    assert!(!cli.cluster.cluster);
    assert!(!cli.resolve_cluster().expect("validates").enabled);
}

#[test]
fn cluster_without_bind_is_refused() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-secret",
        "s3cret",
    ]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::BindRequired
    );
}

#[test]
fn cluster_without_a_secret_is_refused() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "127.0.0.1:4790",
    ]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::SecretRequired
    );
}

/// Pins D-14: the CLI's own resolve refuses `--cluster --runtime per-core`
/// with `PerCoreUnsupported` before the server composes.
#[test]
fn cluster_with_per_core_runtime_is_refused() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "127.0.0.1:4790",
        "--cluster-secret",
        "s3cret",
        "--runtime",
        "per-core",
    ]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::PerCoreUnsupported
    );
}

/// Pins D-14: `--cluster --intercept-port` is refused with
/// `InterceptUnsupported` at startup.
#[test]
fn cluster_with_intercept_is_refused() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "127.0.0.1:4790",
        "--cluster-secret",
        "s3cret",
        "--intercept-port",
        "8443",
    ]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::InterceptUnsupported
    );
}

#[test]
fn a_complete_cluster_invocation_validates() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "10.0.0.7:4790",
        "--cluster-secret",
        "s3cret",
        "--cluster-seeds",
        "10.0.0.8:4790,10.0.0.9:4790",
    ]);
    assert!(cli.resolve_cluster().expect("validates").enabled);
    assert_eq!(cli.cluster.cluster_seeds.len(), 2);
}

#[test]
fn the_secret_can_come_from_a_file_and_is_trimmed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secret");
    // Trailing newline is what `echo -n` omits and every other tool adds; a
    // secret that differs by a newline authenticates against nothing.
    std::fs::write(&path, "file-secret\n").expect("write secret");
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "10.0.0.7:4790",
        "--cluster-secret-file",
        &path.to_string_lossy(),
    ]);
    let config = cli.resolve_cluster().expect("validates");
    assert_eq!(config.secret.as_deref(), Some("file-secret"));
}

#[test]
fn an_unreadable_secret_file_is_its_own_error_not_an_insecure_cluster() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "10.0.0.7:4790",
        "--cluster-secret-file",
        "/nonexistent/rift/secret",
    ]);
    // Fail closed, and say what actually went wrong: degrading an unreadable
    // secret into "no secret" would either run unauthenticated or blame the
    // operator for a flag they did pass.
    let err = cli.resolve_cluster().expect_err("unreadable secret file");
    assert!(
        matches!(err, ConfigError::SecretFileUnreadable { .. }),
        "{err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("/nonexistent/rift/secret"), "{msg}");
}

#[test]
fn an_explicitly_insecure_cluster_is_allowed_but_marked() {
    let cli = parse(&[
        "rift-cluster-server",
        "--cluster",
        "--cluster-bind",
        "10.0.0.7:4790",
        "--cluster-insecure",
    ]);
    let config = cli.resolve_cluster().expect("validates");
    assert!(config.is_insecure());
}

/// `--version` has to identify the *embedded* open-source Rift, not just this
/// crate: every crate under `vendor/rift` inherits `0.1.0` from that workspace,
/// so a bare crate version tells an operator nothing about which engine is in
/// the binary they are reporting a bug against.
#[test]
fn version_reports_the_edition_and_the_embedded_upstream_rift() {
    let rendered = EeCli::command().render_version();
    assert!(
        rendered.contains(rift_cluster_base::version()),
        "{rendered}"
    );
    assert!(rendered.contains("cluster"), "{rendered}");
    assert!(
        rendered.contains(rift_cluster_base::UPSTREAM_VERSION),
        "the upstream pin must be reported: {rendered}"
    );
    assert!(
        !rendered.contains("rift )"),
        "an empty pin renders as a formatting bug rather than missing info: {rendered}"
    );
}

/// Issue #43: the declines are gone, and they must stay gone.
///
/// The unit tests around `bootstrap` drive the library functions directly, so a
/// guard reintroduced in `main.rs` *in front of* the bootstrap would leave them
/// all green while the shipped binary refused the flag again. This runs the real
/// artifact, which is the only thing that can catch that.
#[test]
fn the_binary_no_longer_declines_rcfile_or_the_pidfile_subcommands() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let rcfile = dir.path().join("rc.json");
    std::fs::write(&rcfile, r#"{"port": 4321}"#).expect("write rcfile");

    // `stop` against a PID file that does not exist: it must fail for that
    // reason, not because the subcommand is refused outright.
    let stopped = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args([
            "stop",
            "--pidfile",
            &dir.path().join("absent.pid").to_string_lossy(),
        ])
        .output()
        .expect("run the binary");
    let stderr = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        !stderr.contains("not supported by rift-cluster-server"),
        "`stop` is implemented now; it must not be declined: {stderr}"
    );
    assert!(
        stderr.contains("PID file not found"),
        "expected the real not-found error, got: {stderr}"
    );

    // `--rcfile` with a bad PID file behind `stop` proves the flag was accepted
    // and parsed rather than rejected before the subcommand ever ran.
    let with_rcfile = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args([
            "--rcfile",
            &rcfile.to_string_lossy(),
            "stop",
            "--pidfile",
            &dir.path().join("absent.pid").to_string_lossy(),
        ])
        .output()
        .expect("run the binary");
    let stderr = String::from_utf8_lossy(&with_rcfile.stderr);
    assert!(
        !stderr.contains("not supported by rift-cluster-server"),
        "`--rcfile` is honoured now; it must not be declined: {stderr}"
    );
}

/// Issue #68: `--cluster-advertise` takes a host:port authority, not only a
/// literal address.
///
/// This is the gate at the CLI boundary — before it, clap rejected every
/// hostname at parse time, so the DNS re-resolution the cluster already
/// implements could never be reached from a real deployment.
#[test]
fn cluster_advertise_accepts_hostname() {
    let cli = EeCli::try_parse_from([
        "rift-cluster-server",
        "--cluster",
        "--cluster-advertise",
        "rift-0.rift-headless.ns.svc.cluster.local:4790",
    ])
    .expect("a Kubernetes headless-service name must be accepted");
    assert_eq!(
        cli.cluster
            .cluster_advertise
            .as_ref()
            .map(std::string::ToString::to_string),
        Some("rift-0.rift-headless.ns.svc.cluster.local:4790".to_owned())
    );
}

#[test]
fn cluster_advertise_rejects_a_value_without_a_port() {
    assert!(
        EeCli::try_parse_from([
            "rift-cluster-server",
            "--cluster",
            "--cluster-advertise",
            "rift-0.rift-headless",
        ])
        .is_err(),
        "peers dial a port, so an authority without one must be refused at parse time"
    );
}

/// Pins D-77 against the **real artifact**, which is the only thing that can
/// catch the regression that matters.
///
/// The unit tests around `bootstrap` drive `apply_rcfile` directly, so replacing
/// the `?` in `main.rs` with `let _ = bootstrap::apply_rcfile(&mut cli);` would
/// restore the exact fail-open D-77 closes — a refused rcfile applying none of
/// its keys, `requireAdminAuth` among them — and leave every one of them green.
/// This is the mirror image of the argument
/// `the_binary_no_longer_declines_rcfile_or_the_pidfile_subcommands` makes for
/// issue #43: a guard reintroduced in `main.rs` is invisible from the library.
///
/// The wrong-typed key is the case worth spawning a process for. It is the one
/// upstream #1114 added, the one that refuses the file *whole*, and the one
/// where continuing is silently insecure rather than merely wrong.
#[test]
fn a_refused_rcfile_refuses_startup_in_the_shipped_binary() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let rcfile = dir.path().join("rc.json");
    // Exactly the pairing from issue #589: a wrong-typed flag beside the
    // security flag it would have taken down with it.
    std::fs::write(&rcfile, r#"{"localOnly": "yes", "requireAdminAuth": true}"#)
        .expect("write rcfile");

    // Spawned and polled rather than `output()`ed, because the regression this
    // guards is not "exits with the wrong code" — it is "does not exit at all".
    // Swallow the refusal and the binary goes on to *serve*, so `output()` would
    // block on a pipe that never closes and hang the suite instead of failing
    // it. Still running past the deadline is therefore the assertion, not an
    // accident of it.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args(["--rcfile", &rcfile.to_string_lossy()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn the binary");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let exited = loop {
        match child.try_wait().expect("poll the child") {
            Some(status) => break Some(status),
            None if std::time::Instant::now() >= deadline => break None,
            None => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    };
    if exited.is_none() {
        let _ = child.kill();
    }
    let out = child
        .wait_with_output()
        .expect("collect the child's output");

    let status = exited.expect(
        "the binary was still running 30s after a refused rcfile: it started a server instead \
         of refusing, which is the fail-open D-77 closes",
    );
    assert!(
        !status.success(),
        "a refused rcfile must refuse startup, not warn and serve: {status:?}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("rc.json"),
        "the refusal must name the file: {stderr}"
    );
    assert!(
        stderr.contains("localOnly"),
        "the refusal must name the offending key: {stderr}"
    );
}

/// The other half of D-77: an rcfile the binary *can* apply must not be turned
/// into a refusal by the change above. Without this, "refuse on error" could be
/// implemented as "refuse whenever --rcfile is given" and the test above would
/// still pass.
///
/// `stop` against an absent PID file is the cheapest complete program that
/// reaches the rcfile: it exits after the bootstrap without binding a port.
#[test]
fn a_good_rcfile_still_starts_and_reports_its_unsupported_keys() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let rcfile = dir.path().join("rc.json");
    std::fs::write(&rcfile, r#"{"port": 4321, "mountebankOnly": 1}"#).expect("write rcfile");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args([
            "--rcfile",
            &rcfile.to_string_lossy(),
            "stop",
            "--pidfile",
            &dir.path().join("absent.pid").to_string_lossy(),
        ])
        .output()
        .expect("run the binary");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("must be"),
        "a well-formed rcfile must not be refused: {stderr}"
    );
    assert!(
        stderr.contains("mountebankOnly"),
        "an unsupported key must still be reported, on stderr, before any subscriber exists: {stderr}"
    );
}

/// A stand-in admin plane: answers `200` to every request on an ephemeral
/// loopback port, for as long as the test runs.
fn healthy_listener() -> u16 {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("local_addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        }
    });
    port
}

/// A loopback address nothing is listening on, so `healthcheck_url`'s
/// clustered-mode detection answers "not clustered" regardless of what else runs
/// on this machine. Bound and dropped, so another process could take the port in
/// between. In this test binary nothing else binds a listener that answers, so the
/// race is theoretical here — but it is not self-reporting: a port reused by
/// something answering `200` on `/healthz` would make the probe test pass without
/// testing the rcfile.
fn closed_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local_addr").to_string()
}

/// Pins #593 (upstream #1133) against the real binary: `healthcheck` probes the
/// port an rcfile sets. It used to compute its target before the rcfile was
/// applied, so a deployment configured through an rcfile ran a server on one port
/// and a container probe that knocked on 2525 forever.
///
/// The power of this test assumes nothing answers `200` on 2525 — true in CI.
/// Under the regression the probe targets 2525 and is refused.
#[test]
fn healthcheck_probes_the_port_an_rcfile_sets() {
    let port = healthy_listener();
    let dir = tempfile::TempDir::new().expect("tempdir");
    let rcfile = dir.path().join("rc.json");
    std::fs::write(&rcfile, format!(r#"{{"port": {port}}}"#)).expect("write rcfile");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args([
            "--rcfile",
            &rcfile.to_string_lossy(),
            "--cluster-probe-bind",
            &closed_addr(),
            "healthcheck",
        ])
        .output()
        .expect("run the binary");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "the probe must reach the rcfile's port {port}, not the default: {stderr}"
    );
}

/// The other half of #593: a refused rcfile refuses the probe. A server started
/// with that file would not have started either, so "unhealthy" is the true
/// answer — and the operator is told which file, not handed a connection error
/// against a port nobody configured.
#[test]
fn a_refused_rcfile_refuses_the_healthcheck_and_names_the_file() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let rcfile = dir.path().join("broken.json");
    std::fs::write(&rcfile, "not json at all").expect("write rcfile");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
        .args([
            "--rcfile",
            &rcfile.to_string_lossy(),
            "--cluster-probe-bind",
            &closed_addr(),
            "healthcheck",
        ])
        .output()
        .expect("run the binary");

    assert!(
        !out.status.success(),
        "a refused rcfile is an unhealthy verdict"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("broken.json"),
        "the verdict must name the rcfile, not a port nobody configured: {stderr}"
    );
}

/// Pins #594 (upstream #1134) against the real binary: an unrecognised
/// `--loglevel` refuses startup with the value named. It used to become `info`
/// silently, in both binaries, because this one carried a copy of upstream's
/// filter logic rather than calling it.
///
/// `stop` against an absent PID file is the cheapest program that passes through
/// tracing initialisation: it reaches `init_tracing`, then fails for its own,
/// distinguishable reason.
#[test]
fn an_unknown_log_level_refuses_startup_and_a_real_one_does_not() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let pidfile = dir.path().join("absent.pid");
    let run = |level: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_rift-cluster-server"))
            .args([
                "--loglevel",
                level,
                "stop",
                "--pidfile",
                &pidfile.to_string_lossy(),
            ])
            .env_remove("RUST_LOG")
            .output()
            .expect("run the binary")
    };

    let refused = run("warnn");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success());
    assert!(
        stderr.contains("warnn"),
        "the refusal must echo the level the operator typed: {stderr}"
    );
    assert!(
        !stderr.contains("PID file not found"),
        "a bad level must be refused before the subcommand runs: {stderr}"
    );

    // `trace` is a real level and must not be refused. This half guards against
    // refusing too much; it does not prove `trace` is *honoured* — the old code
    // turned it into `info` and `stop` still ran, so it passed then too.
    let accepted = run("trace");
    let stderr = String::from_utf8_lossy(&accepted.stderr);
    assert!(
        stderr.contains("PID file not found"),
        "`trace` must pass tracing initialisation and reach the subcommand: {stderr}"
    );
}
