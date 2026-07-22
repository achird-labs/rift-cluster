//! The CLI is an open-source superset (issue #10 AC1/AC2): every flag and
//! subcommand the `rift` binary accepts must still parse here, and the cluster
//! flags must be validated before anything binds.

use clap::{CommandFactory, Parser};
use rift_cluster::ConfigError;
use rift_ee::seams::Cli as OssCli;
use rift_ee_server::cli::EeCli;

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
        "enterprise CLI is missing open-source flags: {missing:?}"
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
    let cli = parse(&["rift-ee-server"]);
    assert!(!cli.cluster.cluster);
    assert!(!cli.resolve_cluster().expect("validates").enabled);
}

#[test]
fn cluster_without_bind_is_refused() {
    let cli = parse(&["rift-ee-server", "--cluster", "--cluster-secret", "s3cret"]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::BindRequired
    );
}

#[test]
fn cluster_without_a_secret_is_refused() {
    let cli = parse(&[
        "rift-ee-server",
        "--cluster",
        "--cluster-bind",
        "127.0.0.1:4790",
    ]);
    assert_eq!(
        cli.resolve_cluster().expect_err("refused"),
        ConfigError::SecretRequired
    );
}

#[test]
fn cluster_with_per_core_runtime_is_refused() {
    let cli = parse(&[
        "rift-ee-server",
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

#[test]
fn cluster_with_intercept_is_refused() {
    let cli = parse(&[
        "rift-ee-server",
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
        "rift-ee-server",
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
        "rift-ee-server",
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
        "rift-ee-server",
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
        "rift-ee-server",
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
    assert!(rendered.contains(rift_ee::version()), "{rendered}");
    assert!(rendered.contains("enterprise"), "{rendered}");
    assert!(
        rendered.contains(rift_ee::UPSTREAM_VERSION),
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
    let stopped = std::process::Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
        .args([
            "stop",
            "--pidfile",
            &dir.path().join("absent.pid").to_string_lossy(),
        ])
        .output()
        .expect("run the binary");
    let stderr = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        !stderr.contains("not supported by rift-ee-server"),
        "`stop` is implemented now; it must not be declined: {stderr}"
    );
    assert!(
        stderr.contains("PID file not found"),
        "expected the real not-found error, got: {stderr}"
    );

    // `--rcfile` with a bad PID file behind `stop` proves the flag was accepted
    // and parsed rather than rejected before the subcommand ever ran.
    let with_rcfile = std::process::Command::new(env!("CARGO_BIN_EXE_rift-ee-server"))
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
        !stderr.contains("not supported by rift-ee-server"),
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
        "rift-ee-server",
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
            "rift-ee-server",
            "--cluster",
            "--cluster-advertise",
            "rift-0.rift-headless",
        ])
        .is_err(),
        "peers dial a port, so an authority without one must be refused at parse time"
    );
}
