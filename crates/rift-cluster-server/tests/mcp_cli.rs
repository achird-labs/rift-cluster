//! Gate for #292 (MCP-A): the `mcp` subcommand must be reachable and discoverable
//! **without** disturbing how any existing invocation parses.
//!
//! Why this file exists at all: `EeCli` flattens the upstream `OssCli`, which already
//! declares clap's single `#[command(subcommand)]` slot (`Commands`: `Start`/`Stop`/
//! `Restart`/`Save`/`Replay`/`Script`/`Healthcheck`). The issue's premise that `EeCli` is
//! "a flat clap parser" is false, so `mcp` is added at the **builder** level instead — and
//! the whole risk of doing that is a regression in the existing parse. These tests are that
//! risk, pinned.

use rift_cluster_server::EeCli;
use rift_cluster_server::mcp::{self, Invocation};

/// The server arm, or a failure naming what turned up instead.
///
/// Local to the gate rather than a method on `Invocation`: a panicking accessor
/// on the library type would be public API existing only for this file.
fn expect_server(invocation: Invocation) -> Box<EeCli> {
    match invocation {
        Invocation::Server(cli) => cli,
        Invocation::Mcp(_) => panic!("expected a server invocation, got the mcp subcommand"),
    }
}

/// `mcp` reaches the MCP arm, with its flags parsed.
#[test]
fn mcp_argv_reaches_the_mcp_arm() {
    let parsed = mcp::parse_from([
        "rift-cluster-server",
        "mcp",
        "--url",
        "https://fleet.example:2525",
        "--api-key-file",
        "/tmp/agent.key",
    ])
    .expect("mcp invocation must parse");

    match parsed {
        Invocation::Mcp(args) => {
            assert_eq!(args.url.as_str(), "https://fleet.example:2525/");
            assert_eq!(args.api_key_file.to_str(), Some("/tmp/agent.key"));
        }
        Invocation::Server(_) => panic!("`mcp` must not parse as a server invocation"),
    }
}

/// AC3 / E14 — the parse of every existing invocation is unchanged. Each of these
/// predates the `mcp` subcommand and must still land on the server arm, with the same
/// field values it had before.
#[test]
fn existing_invocations_parse_identically() {
    // A bare invocation: no subcommand at all.
    let bare = mcp::parse_from(["rift-cluster-server"]).expect("bare invocation must parse");
    let server = expect_server(bare);
    assert!(
        server.oss.command.is_none(),
        "a bare invocation must carry no subcommand"
    );

    // The flag-only shape the compose files and tests use.
    let flags = mcp::parse_from(["rift-cluster-server", "--port", "2525", "--cluster"])
        .expect("flag invocation must parse");
    let server = expect_server(flags);
    assert_eq!(server.oss.port, 2525);
    assert!(server.cluster.cluster, "--cluster must still set the flag");

    // An upstream subcommand must still reach the upstream enum, not the mcp arm.
    let health = mcp::parse_from(["rift-cluster-server", "healthcheck"])
        .expect("healthcheck must still parse");
    let server = expect_server(health);
    assert!(
        matches!(
            server.oss.command,
            Some(rift_cluster_base::seams::Commands::Healthcheck { .. })
        ),
        "healthcheck must still resolve to the upstream Commands::Healthcheck"
    );
}

/// E15 — a subcommand nobody can find is not shipped. `--help` must name `mcp`.
#[test]
fn mcp_subcommand_appears_in_top_level_help() {
    let help = mcp::augmented_command().render_help().to_string();
    assert!(
        help.contains("mcp"),
        "top-level --help must list the mcp subcommand; got:\n{help}"
    );
}

/// E15 — and `mcp --help` must document its own flags, including that the key comes
/// from a file (RFC-006 §9.4 prefers `--api-key-file` over an env var in all docs).
#[test]
fn mcp_help_lists_its_flags() {
    let mut cmd = mcp::augmented_command();
    let sub = cmd
        .find_subcommand_mut("mcp")
        .expect("mcp subcommand must be registered");
    let help = sub.render_help().to_string();

    for flag in ["--url", "--api-key-file"] {
        assert!(
            help.contains(flag),
            "`mcp --help` must document {flag}; got:\n{help}"
        );
    }
}

/// The upstream subcommand names must not have been shadowed by the augmentation.
#[test]
fn upstream_subcommands_are_still_registered() {
    let cmd = mcp::augmented_command();
    let names: Vec<_> = cmd.get_subcommands().map(|s| s.get_name()).collect();

    for expected in ["script", "healthcheck", "mcp"] {
        assert!(
            names.contains(&expected),
            "subcommand `{expected}` must be registered; found {names:?}"
        );
    }
}

/// `mcp` requires both credentials-shaped flags; omitting one is a clean clap error,
/// never a panic and never a half-configured client (AC2).
#[test]
fn mcp_requires_url_and_key_file() {
    let missing_key = mcp::parse_from([
        "rift-cluster-server",
        "mcp",
        "--url",
        "https://fleet.example:2525",
    ]);
    assert!(
        missing_key.is_err(),
        "`mcp` without --api-key-file must be rejected"
    );

    let missing_url = mcp::parse_from(["rift-cluster-server", "mcp", "--api-key-file", "/tmp/k"]);
    assert!(missing_url.is_err(), "`mcp` without --url must be rejected");
}
