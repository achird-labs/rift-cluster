//! The enterprise CLI: every open-source flag, plus the `--cluster*` superset.
//!
//! The open-source [`OssCli`] is flattened rather than restated, so the two can
//! never drift: a flag added upstream appears here on the next pin bump, and the
//! parity test in `tests/cli.rs` fails if flattening ever stops covering it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::LazyLock;

use rift_cluster::{Authority, ClusterConfig, ConfigError, RuntimeTopology};
use rift_ee::seams::Cli as OssCli;

/// The `--cluster*` flags.
#[derive(clap::Args, Debug, Clone)]
pub struct ClusterArgs {
    /// Run this node as part of a cluster (the master switch; every other
    /// --cluster* flag is inert without it)
    #[arg(long, env = "RIFT_CLUSTER")]
    pub cluster: bool,

    /// Address to bind the cluster port on. Required with --cluster; there is no
    /// default because the cluster port must be an explicit decision.
    #[arg(long, value_name = "ADDR", env = "RIFT_CLUSTER_BIND")]
    pub cluster_bind: Option<SocketAddr>,

    /// Acknowledge that --cluster-bind names a publicly reachable interface
    #[arg(long, env = "RIFT_CLUSTER_BIND_PUBLIC_OK")]
    pub cluster_bind_public_ok: bool,

    /// Address peers dial this node on, when it differs from --cluster-bind
    /// (NAT, port mapping, a pod behind a service). Accepts a hostname as well
    /// as a literal address; a hostname is re-resolved on every send, so a
    /// StatefulSet's headless-service DNS entry stays valid across rollouts.
    #[arg(long, value_name = "HOST:PORT", env = "RIFT_CLUSTER_ADVERTISE")]
    pub cluster_advertise: Option<Authority>,

    /// Existing cluster members to join through (comma-separated). Re-resolved
    /// on each attempt, so a DNS name that gains members is picked up.
    #[arg(
        long,
        value_delimiter = ',',
        value_name = "ADDR[,ADDR...]",
        env = "RIFT_CLUSTER_SEEDS"
    )]
    pub cluster_seeds: Vec<String>,

    /// Form a new single-node cluster when no seeds are given. Without it, a
    /// node with no seeds refuses to start rather than silently founding a
    /// second cluster beside the real one.
    #[arg(long, env = "RIFT_CLUSTER_ALLOW_SOLO")]
    pub cluster_allow_solo: bool,

    /// Shared secret authenticating the cluster port
    #[arg(long, value_name = "SECRET", env = "RIFT_CLUSTER_SECRET")]
    pub cluster_secret: Option<String>,

    /// File holding the shared secret (trailing whitespace trimmed)
    #[arg(
        long,
        value_name = "FILE",
        env = "RIFT_CLUSTER_SECRET_FILE",
        conflicts_with = "cluster_secret"
    )]
    pub cluster_secret_file: Option<PathBuf>,

    /// Run the cluster port unauthenticated (requires no secret; audited as
    /// rift_cluster_insecure)
    #[arg(long, env = "RIFT_CLUSTER_INSECURE")]
    pub cluster_insecure: bool,

    /// Directory for this node's cluster state (identity, Raft log, snapshots).
    /// Defaults to `<datadir>/_cluster`.
    #[arg(long, value_name = "DIR", env = "RIFT_CLUSTER_STATE_DIR")]
    pub cluster_state_dir: Option<PathBuf>,

    /// Operator-facing name for this node. Only seeds its first node id; once
    /// minted, the persisted id wins.
    #[arg(long, value_name = "NAME", env = "RIFT_CLUSTER_NODE_NAME")]
    pub cluster_node_name: Option<String>,

    /// Seconds to keep serving in-flight work after SIGTERM before shutting
    /// down. Set the orchestrator's grace period to at least twice this.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 10,
        env = "RIFT_CLUSTER_LEAVE_TIMEOUT"
    )]
    pub cluster_leave_timeout: u64,

    /// Address for the unauthenticated /readyz and /healthz probes
    #[arg(
        long,
        value_name = "ADDR",
        default_value = "0.0.0.0:2526",
        env = "RIFT_CLUSTER_PROBE_BIND"
    )]
    pub cluster_probe_bind: SocketAddr,

    /// What a committed admin write waits for before its 2xx: every Ready
    /// node's applied index (read-your-write anywhere), or only the answering
    /// node's own (read-your-write here)
    #[arg(
        long,
        value_enum,
        value_name = "MODE",
        default_value_t = WriteBarrier::ReadyNodes,
        env = "RIFT_CLUSTER_WRITE_BARRIER"
    )]
    pub cluster_write_barrier: WriteBarrier,

    /// Seconds the write barrier waits before answering anyway with a
    /// Rift-Cluster-Warnings header naming the unapplied nodes. Bounds both
    /// levels: under `none` it caps the wait for this node's own apply, and the
    /// header then names this node
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 2,
        env = "RIFT_CLUSTER_WRITE_BARRIER_TIMEOUT"
    )]
    pub cluster_write_barrier_timeout: u64,

    /// Answer admin writes with an immediate 202 + op id after durably parking
    /// them, instead of waiting for commit + barrier. Poll
    /// `GET /_cluster/ops/:id` for the outcome.
    #[arg(long, env = "RIFT_CLUSTER_ADMIN_ASYNC")]
    pub cluster_admin_async: bool,

    /// Milliseconds between group fsyncs of acknowledged `durability: "async"`
    /// flow-state writes — the bound on what a whole-fleet crash can lose for
    /// imposters that did not opt into `"sync"` (which fsyncs before every ack)
    /// or `"none"` (which never persists)
    #[arg(
        long,
        value_name = "MILLIS",
        default_value_t = 50,
        env = "RIFT_CLUSTER_FLOW_FSYNC_INTERVAL_MS"
    )]
    pub cluster_flow_fsync_interval_ms: u64,
}

/// `--cluster-write-barrier` modes (issue #9 §4).
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBarrier {
    /// Wait until every Ready node has applied the write (the default): a 2xx
    /// means any node serves the new config.
    ReadyNodes,
    /// Answer as soon as the write is committed and applied *locally*. Never
    /// waits on a peer — but not "waits for nothing" either: the answering node
    /// renders the resource it just committed, so it waits for its own apply or
    /// it would answer `404` for a write it durably holds (#99).
    None,
}

/// What `--version` prints: this build's version, its edition, and the
/// open-source Rift embedded in it.
///
/// Built once at first use rather than `concat!`-ed, because `EDITION` and the
/// upstream pin are consts rather than literals.
static VERSION_BANNER: LazyLock<String> = LazyLock::new(rift_ee::version_banner);

/// `rift-ee-server`: the Rift server with enterprise clustering.
#[derive(clap::Parser, Debug)]
#[command(name = "rift-ee-server", version = VERSION_BANNER.as_str(), about)]
pub struct EeCli {
    #[command(flatten)]
    pub oss: OssCli,

    #[command(flatten)]
    pub cluster: ClusterArgs,
}

impl EeCli {
    /// Resolve the cluster flags into a validated [`ClusterConfig`], reading the
    /// secret file if one was given.
    ///
    /// Reading and validating are one step because they fail for the same reason
    /// — the node must not start — and splitting them invites a caller that
    /// resolves without validating.
    pub fn resolve_cluster(&self) -> Result<ClusterConfig, ConfigError> {
        // Every --cluster* flag is inert without the master switch, and that
        // has to include the *side effects* of reading one: a stray
        // --cluster-secret-file on a single node must not fail its startup.
        if !self.cluster.cluster {
            return Ok(ClusterConfig::default());
        }

        let secret = match (
            &self.cluster.cluster_secret,
            &self.cluster.cluster_secret_file,
        ) {
            (Some(secret), _) => Some(secret.clone()),
            (None, Some(path)) => Some(read_secret_file(path)?),
            (None, None) => None,
        };

        let config = ClusterConfig {
            enabled: self.cluster.cluster,
            bind: self.cluster.cluster_bind,
            bind_public_ok: self.cluster.cluster_bind_public_ok,
            secret,
            insecure: self.cluster.cluster_insecure,
            runtime: self.runtime_topology(),
            intercept: self.wants_intercept(),
        };
        config.validate()?;
        Ok(config)
    }

    /// How the open-source flags ask the data plane to be scheduled, as the
    /// cluster guards see it. Anything that is not explicitly per-core is
    /// work-stealing — including a value only the open-source resolver
    /// understands, which it reports on its own.
    fn runtime_topology(&self) -> RuntimeTopology {
        match self.oss.runtime.as_deref() {
            Some(mode) if mode.trim().starts_with("per-core") => RuntimeTopology::PerCore,
            _ => RuntimeTopology::WorkStealing,
        }
    }

    /// Whether the TLS-MITM intercept listener was requested. The config file's
    /// `intercept` block is the other spelling, but it is only readable once the
    /// config has been loaded — well after these guards must have run — so the
    /// composed server re-checks it at start.
    fn wants_intercept(&self) -> bool {
        self.oss.intercept_port.is_some()
    }

    /// Where this node keeps its cluster state.
    #[must_use]
    pub fn cluster_state_dir(&self) -> PathBuf {
        self.cluster
            .cluster_state_dir
            .clone()
            .unwrap_or_else(|| match &self.oss.datadir {
                Some(datadir) => datadir.join("_cluster"),
                None => PathBuf::from(".rift").join("_cluster"),
            })
    }

    /// The node id to propose if this node has never been given one. Derived
    /// from `--cluster-node-name` when set so a redeployed pod with the same
    /// name and a wiped state dir returns as the same node.
    #[must_use]
    pub fn proposed_node_id(&self) -> u64 {
        match &self.cluster.cluster_node_name {
            Some(name) => {
                // Raft treats 0 as a valid id but openraft's metrics use it as
                // "no leader", so keep minted ids away from it.
                xxhash_node_id(name)
            }
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| u64::try_from(d.as_nanos()).ok())
                .unwrap_or(1)
                .max(1),
        }
    }
}

/// A stable, cross-version id for a node name. Deliberately not the std hasher:
/// two nodes deriving different ids from the same name would be two identities.
fn xxhash_node_id(name: &str) -> u64 {
    xxhash_rust::xxh64::xxh64(name.as_bytes(), 0).max(1)
}

fn read_secret_file(path: &std::path::Path) -> Result<String, ConfigError> {
    // Fail closed and say which file: degrading an unreadable secret into "no
    // secret" would either start an unauthenticated cluster port or blame the
    // operator for a flag they did pass.
    let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::SecretFileUnreadable {
        path: path.display().to_string(),
        detail: e.to_string(),
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ConfigError::SecretFileUnreadable {
            path: path.display().to_string(),
            detail: "file is empty".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn state_dir_follows_the_datadir_by_default() {
        let cli =
            EeCli::try_parse_from(["rift-ee-server", "--datadir", "/srv/rift"]).expect("parses");
        assert_eq!(cli.cluster_state_dir(), PathBuf::from("/srv/rift/_cluster"));

        let explicit = EeCli::try_parse_from([
            "rift-ee-server",
            "--datadir",
            "/srv/rift",
            "--cluster-state-dir",
            "/var/lib/rift-cluster",
        ])
        .expect("parses");
        assert_eq!(
            explicit.cluster_state_dir(),
            PathBuf::from("/var/lib/rift-cluster")
        );
    }

    #[test]
    fn a_node_name_yields_a_stable_nonzero_id() {
        assert_eq!(xxhash_node_id("rift-0"), xxhash_node_id("rift-0"));
        assert_ne!(xxhash_node_id("rift-0"), xxhash_node_id("rift-1"));
        assert!(xxhash_node_id("") >= 1);
    }

    #[test]
    fn per_core_is_detected_however_it_is_spelled() {
        for spelling in ["per-core", "per-core=4", " per-core"] {
            let cli =
                EeCli::try_parse_from(["rift-ee-server", "--runtime", spelling]).expect("parses");
            assert_eq!(
                cli.runtime_topology(),
                RuntimeTopology::PerCore,
                "spelling {spelling:?}"
            );
        }
        let cli = EeCli::try_parse_from(["rift-ee-server", "--runtime", "work-stealing"])
            .expect("parses");
        assert_eq!(cli.runtime_topology(), RuntimeTopology::WorkStealing);
    }

    #[test]
    fn an_empty_secret_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secret");
        std::fs::write(&path, "   \n").expect("write");
        assert!(matches!(
            read_secret_file(&path),
            Err(ConfigError::SecretFileUnreadable { .. })
        ));
    }
}
