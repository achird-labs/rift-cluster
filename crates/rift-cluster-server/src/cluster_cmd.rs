//! `rift-cluster-server cluster …` — operator commands that act on a running fleet over its signed
//! cluster port (#641).
//!
//! A separate parser rather than a variant of the server's subcommand enum: upstream owns that
//! enum, and the server's dispatch matches it without a wildcard on purpose. `main` routes
//! `argv[1] == "cluster"` here before the server parser ever runs, so nothing in this module can
//! start a server.
//!
//! The one command, `remove-node`, retires a member that is gone for good (D-95). It holds the
//! cluster secret, not the admin key, and changes nothing unless the leader finds that no address
//! of the member answers as that member.

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use rift_cluster::{NodeError, NodeId, ProbeSeen, RetireRefusal, RetireReply};

/// `rift-cluster-server cluster`: operator commands for a running fleet.
#[derive(clap::Parser, Debug)]
#[command(name = "rift-cluster-server cluster")]
pub(crate) struct ClusterCommand {
    #[command(subcommand)]
    action: ClusterAction,
}

#[derive(clap::Subcommand, Debug)]
pub(crate) enum ClusterAction {
    /// Retire a member that is gone for good — a lost volume, a retired host, a pod that came back
    /// under a new id. The leader refuses while any of the member's addresses still answers as that
    /// member: a live node is stopped instead, and leaves on its own.
    RemoveNode {
        /// The id of the member to retire, as `/_fleet/members` reports it.
        node_id: NodeId,

        /// The cluster address (`--cluster-advertise`) of any live member. A follower redirects to
        /// the leader, whose advertised cluster address must then be reachable from where this
        /// runs — inside the fleet's network (e.g. `kubectl exec`), not through a port-forward.
        #[arg(long, value_name = "HOST:PORT")]
        via: String,

        /// The fleet's shared cluster secret.
        #[arg(long, value_name = "SECRET", env = "RIFT_CLUSTER_SECRET")]
        cluster_secret: Option<String>,

        /// Read the cluster secret from a file (trimmed).
        #[arg(
            long,
            value_name = "PATH",
            env = "RIFT_CLUSTER_SECRET_FILE",
            conflicts_with = "cluster_secret"
        )]
        cluster_secret_file: Option<PathBuf>,

        /// Send unsigned, for a fleet running with `--cluster-insecure`.
        #[arg(long, env = "RIFT_CLUSTER_INSECURE")]
        cluster_insecure: bool,

        /// Give up after this many seconds.
        #[arg(long, value_name = "SECONDS", default_value_t = 30)]
        timeout: u64,
    },
}

/// Whether `args` (the full argv) is a `cluster` operator command.
#[must_use]
pub fn is_cluster_command(args: &[OsString]) -> bool {
    args.get(1).is_some_and(|arg| arg == "cluster")
}

/// Parse and run a `cluster` command. Prints the outcome on stdout; a refusal or a failure is an
/// `Err`, which exits non-zero with the operator's next step in the message.
///
/// # Errors
///
/// The leader refused, the fleet could not be reached or had no leader, or the secret could not be
/// read.
pub fn run(args: Vec<OsString>) -> anyhow::Result<()> {
    // argv[0] stays the program name and "cluster" becomes this parser's name.
    let args = std::iter::once(OsString::from("rift-cluster-server cluster"))
        .chain(args.into_iter().skip(2));
    let command = <ClusterCommand as clap::Parser>::parse_from(args);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("starting the runtime")?;
    match command.action {
        ClusterAction::RemoveNode {
            node_id,
            via,
            cluster_secret,
            cluster_secret_file,
            cluster_insecure,
            timeout,
        } => {
            let secret = match (cluster_secret, cluster_secret_file) {
                (Some(secret), _) => Some(secret),
                (None, Some(path)) => Some(crate::cli::read_secret_file(&path)?),
                (None, None) => None,
            };
            anyhow::ensure!(
                secret.as_ref().is_some_and(|s| !s.is_empty()) || cluster_insecure,
                "no cluster secret: pass --cluster-secret or --cluster-secret-file (or set \
                 RIFT_CLUSTER_SECRET / RIFT_CLUSTER_SECRET_FILE); --cluster-insecure only for a \
                 fleet that runs without one"
            );
            let reply = runtime
                .block_on(rift_cluster::retire_via(
                    &via,
                    secret.as_deref(),
                    node_id,
                    Duration::from_secs(timeout),
                ))
                .map_err(|e| anyhow::anyhow!(describe_error(&e)))?;
            println!(
                "{}",
                describe_reply(node_id, &reply).map_err(anyhow::Error::msg)?
            );
            Ok(())
        }
    }
}

/// The success text for `reply`, or the refusal text as the error.
fn describe_reply(node_id: NodeId, reply: &RetireReply) -> Result<String, String> {
    match reply {
        RetireReply::Retired {
            m_idx,
            voters,
            evidence,
        } => {
            let mut text = format!(
                "retired node {node_id}: the membership is now at log index {m_idx}, voters {voters:?}"
            );
            for probe in evidence {
                let seen = match &probe.seen {
                    ProbeSeen::NoAnswer => "no answer".to_owned(),
                    ProbeSeen::NoAddress { detail } => format!("no address ({detail})"),
                    ProbeSeen::AnotherNode { node_id } => {
                        format!("answered as node {node_id} (the address was reused)")
                    }
                };
                text.push_str(&format!("\n  {}: {seen}", probe.addr));
            }
            Ok(text)
        }
        RetireReply::NotAMember => Ok(format!(
            "node {node_id} is not a member of the cluster; nothing to retire (if you expected it \
             to be, check the id against /_fleet/members)"
        )),
        RetireReply::Refused(refusal) => Err(match refusal {
            RetireRefusal::IsLeader => format!(
                "refused: node {node_id} is the leader, so it is alive; stop it and it will leave \
                 on its own"
            ),
            RetireRefusal::Reachable { addr } => format!(
                "refused: node {node_id} answers at {addr}, so it is alive; stop it and it will \
                 leave on its own (if it cannot be stopped, fence its host first)"
            ),
            RetireRefusal::Unidentified { addr, detail } => format!(
                "refused: something answers at {addr} but could not say which node it is \
                 ({detail}); make sure node {node_id} is stopped, and upgrade any node older than \
                 this command"
            ),
            RetireRefusal::HeldByFloor => {
                format!("refused: retiring node {node_id} would leave fewer than two voters")
            }
        }),
    }
}

fn describe_error(error: &NodeError) -> String {
    match error {
        NodeError::Unavailable(detail) => format!("no leader could act on the request: {detail}"),
        other => other.to_string(),
    }
}
