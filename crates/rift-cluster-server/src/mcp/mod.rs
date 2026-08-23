//! `rift-cluster-server mcp` — an MCP server over stdio, for coding agents.
//!
//! **Shape** (RFC-006 §8.1): a subcommand, not a second binary. The process is a
//! *client* of a remote admin front over HTTP — it holds no node state, embeds no
//! engine, and runs happily on a laptop against a remote fleet.
//!
//! **Transport**: stdio only. It is what every coding agent launches, it inherits
//! the parent environment for credential delivery, and it opens **no listening
//! port**, so there is no new network surface to threat-model (RFC-006 §10 makes
//! HTTP/SSE an explicit non-goal, not a deferral).
//!
//! # Why the subcommand is registered by hand
//!
//! RFC-006 §8.1 and issue #292 both describe `EeCli` as "a flat clap parser" where
//! "an optional subcommand is purely additive". That is not true of the tree this
//! ships into: `EeCli` `#[command(flatten)]`s the upstream `OssCli`, which already
//! declares `#[command(subcommand)] pub command: Option<Commands>`. clap allows one
//! subcommand slot per command and the derive macro has already spent it, so a
//! second `#[command(subcommand)]` field does not compile.
//!
//! The alternative — adding an `Mcp` variant to upstream's `Commands` — would make
//! this a cross-repo change (upstream PR plus a vendor bump) and would put an
//! enterprise-only feature in the open-source enum. Registering `mcp` on the
//! *builder* instead has neither cost: builder-level subcommands have no one-slot
//! restriction, `mcp` still appears in `--help`, and every existing invocation
//! parses through the same `Command` it always did.

mod args;
mod client;
mod tools;

use clap::{Args as _, CommandFactory as _, FromArgMatches as _};

// Exported because `main.rs` names `McpArgs`, and the end-to-end gate in
// `tests/mcp_session.rs` drives `RiftMcp` over a real protocol session through
// `AdminClient`, asserting on `ToolFailure`. `ApiKey`, `Answer`, `ReadScope` and
// `StartupError` stay module-private — nothing outside `mcp` names them.
pub use args::McpArgs;
pub use client::{AdminClient, ToolFailure, WriteOutcome};
// The write tools' parameter types are named by `tests/mcp_cluster.rs`, which drives the
// tool methods directly rather than over a protocol session — the only way to replay one
// tool call under the *same* request id, which is exactly what the dedup criterion asserts.
pub use tools::{
    CreateParams, DeleteImposterParams, OpStatusParams, Precondition, RiftMcp, RoutesPutParams,
    SetEnabledParams, StubAddParams, StubByIdParams, StubReplaceParams,
};

use crate::cli::EeCli;

/// The name of the subcommand, in one place so the parser and the dispatcher
/// cannot drift apart.
const MCP: &str = "mcp";

/// What an argv resolved to.
#[derive(Debug)]
pub enum Invocation {
    /// Run the MCP server.
    Mcp(Box<McpArgs>),
    /// Everything else: the server, or one of upstream's own subcommands.
    Server(Box<EeCli>),
}

/// `EeCli`'s clap command with the `mcp` subcommand registered on it.
#[must_use]
pub fn augmented_command() -> clap::Command {
    let mcp = McpArgs::augment_args(
        clap::Command::new(MCP)
            .about("Run an MCP server over stdio against a remote cluster's admin API"),
    );
    EeCli::command().subcommand(mcp)
}

/// Parse an argv into either the MCP subcommand or an ordinary server invocation.
///
/// Every non-`mcp` argv is handed to `EeCli::from_arg_matches` unchanged, which is
/// what keeps existing invocations parsing byte-identically.
pub fn parse_from<I, T>(argv: I) -> Result<Invocation, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let matches = augmented_command().try_get_matches_from(argv)?;

    if let Some((MCP, sub)) = matches.subcommand() {
        return Ok(Invocation::Mcp(Box::new(McpArgs::from_arg_matches(sub)?)));
    }

    Ok(Invocation::Server(Box::new(EeCli::from_arg_matches(
        &matches,
    )?)))
}

/// Parse the real process argv, exiting with clap's own message on error.
#[must_use]
pub fn parse() -> Invocation {
    match parse_from(std::env::args_os()) {
        Ok(invocation) => invocation,
        // clap's `exit` renders the same help/usage text `Parser::parse` would, so a
        // bad flag looks identical to how it always has.
        Err(err) => err.exit(),
    }
}

/// Serve MCP over stdio until the client disconnects.
///
/// # Errors
///
/// Returns a [`StartupError`] before serving if the key file cannot be read or is
/// empty, and an I/O error if the stdio transport fails. Neither panics: an agent
/// host gets a diagnosable message on stderr and a non-zero exit, not a backtrace.
pub async fn run(args: McpArgs) -> anyhow::Result<()> {
    use rmcp::ServiceExt as _;

    let client = AdminClient::new(&args)?;
    let service = RiftMcp::new(client).serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reads #292 shipped, unchanged by #293.
    const READ_TOOLS: [&str; 9] = [
        "fleet_health",
        "imposter_get",
        "imposter_list",
        "lint",
        "op_status",
        "requests_query",
        "routes_get",
        "verify",
        "whoami",
    ];

    /// The writes #293 adds. `op_status` is deliberately NOT here: it polls a parked
    /// write and mutates nothing, so it belongs with the reads above.
    const WRITE_TOOLS: [&str; 8] = [
        "imposter_create",
        "imposter_delete",
        "imposter_set_enabled",
        "route_delete",
        "routes_put",
        "stub_add",
        "stub_delete",
        "stub_replace",
    ];

    /// The tool set is exactly the reads (#292) plus the writes (#293) — no more, no
    /// fewer. An extra tool here is a capability no slice authorized.
    #[test]
    fn registers_exactly_the_read_and_write_tools() {
        let router = RiftMcp::tool_router();
        let mut names: Vec<_> = router
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();
        names.sort();

        let mut expected: Vec<_> = READ_TOOLS
            .iter()
            .chain(WRITE_TOOLS.iter())
            .map(|s| (*s).to_owned())
            .collect();
        expected.sort();

        assert_eq!(names, expected);
    }

    /// `read_only_hint` must tell the truth in **both** directions.
    ///
    /// It is what an agent host shows a user when it asks whether to auto-approve a
    /// call, so this is a consent property, not a cosmetic one — and #293 is where it
    /// acquires a second failure mode. A read hinted as a write is merely annoying; a
    /// **write hinted as a read** is a mutation auto-approved on the strength of a
    /// promise that it would not mutate anything, which is why the write half asserts
    /// the hint is absent-or-false rather than just "not required to be true".
    #[test]
    fn the_read_only_hint_partitions_reads_from_writes() {
        for tool in RiftMcp::tool_router().list_all() {
            let name = tool.name.to_string();
            let read_only = tool
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.read_only_hint);

            if READ_TOOLS.contains(&name.as_str()) {
                assert_eq!(
                    read_only,
                    Some(true),
                    "read tool `{name}` must be annotated read-only"
                );
            } else {
                assert!(
                    WRITE_TOOLS.contains(&name.as_str()),
                    "tool `{name}` is in neither list — classify it before shipping it"
                );
                assert_ne!(
                    read_only,
                    Some(true),
                    "write tool `{name}` must NOT be hinted read-only: an agent host would \
                     auto-approve a mutation on the strength of that hint"
                );
            }
        }
    }

    /// Each tool carries a description: it is the only workflow guidance an agent
    /// gets, since the issue rules out Agent Skills packaging.
    #[test]
    fn every_tool_has_a_description() {
        for tool in RiftMcp::tool_router().list_all() {
            let description = tool.description.as_deref().unwrap_or("");
            assert!(
                !description.trim().is_empty(),
                "tool `{}` must carry a description",
                tool.name
            );
        }
    }
}
