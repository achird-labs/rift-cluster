//! What a write on this node rides, read back (#394, D-82).
//!
//! Every value here is startup configuration — a `--cluster-*` flag — and nothing sets it at
//! runtime. The admin front acts on it and the members builder reports it, from the same value
//! built once in `compose`, so the report cannot describe a node other than the one answering.

use std::time::Duration;

use crate::cli::{ClusterArgs, WriteBarrier};

/// The write-path settings a node was started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePathSettings {
    /// `--cluster-write-barrier`: what a committed admin write waits for before its 2xx.
    pub barrier: WriteBarrier,
    /// `--cluster-write-barrier-timeout`: how long it waits before answering with a warning.
    pub barrier_timeout: Duration,
    /// `--cluster-admin-async`: answer an admin write with `202` + op id once it is parked.
    pub admin_async: bool,
    /// `--cluster-flow-fsync-interval-ms`: the group-fsync period for `durability: "async"` flow
    /// writes — the bound on what a whole-fleet crash can lose for them. Kept in the flag's unit
    /// so the report is the flag's value, not a conversion of it.
    pub flow_fsync_interval_ms: u64,
}

impl WritePathSettings {
    #[must_use]
    pub fn from_cli(args: &ClusterArgs) -> Self {
        Self {
            barrier: args.cluster_write_barrier,
            barrier_timeout: Duration::from_secs(args.cluster_write_barrier_timeout),
            admin_async: args.cluster_admin_async,
            flow_fsync_interval_ms: args.cluster_flow_fsync_interval_ms,
        }
    }

    /// The `write_path` object `/_cluster/members` carries.
    ///
    /// Units are in the key names and match the flags', so an operator can put the value straight
    /// back on a command line. The barrier is spelled as its flag value, not as the Rust variant.
    #[must_use]
    pub(crate) fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "write_barrier": match self.barrier {
                WriteBarrier::ReadyNodes => "ready-nodes",
                WriteBarrier::None => "none",
            },
            "write_barrier_timeout_seconds": self.barrier_timeout.as_secs(),
            "admin_async": self.admin_async,
            "flow_fsync_interval_ms": self.flow_fsync_interval_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::EeCli;

    fn settings(extra: &[&str]) -> WritePathSettings {
        let mut args = vec!["rift-cluster-server", "--cluster"];
        args.extend_from_slice(extra);
        WritePathSettings::from_cli(&EeCli::try_parse_from(args).expect("parses").cluster)
    }

    /// The shipped defaults, spelled as the flags spell them.
    #[test]
    fn the_defaults_read_back_as_the_flags_document_them() {
        assert_eq!(
            settings(&[]).to_json(),
            serde_json::json!({
                "write_barrier": "ready-nodes",
                "write_barrier_timeout_seconds": 2,
                "admin_async": false,
                "flow_fsync_interval_ms": 50,
            })
        );
    }

    /// Every field follows its own flag — none is read from a neighbour or left at its default.
    #[test]
    fn every_flag_reaches_its_own_field() {
        assert_eq!(
            settings(&[
                "--cluster-write-barrier",
                "none",
                "--cluster-write-barrier-timeout",
                "7",
                "--cluster-admin-async",
                "--cluster-flow-fsync-interval-ms",
                "125",
            ])
            .to_json(),
            serde_json::json!({
                "write_barrier": "none",
                "write_barrier_timeout_seconds": 7,
                "admin_async": true,
                "flow_fsync_interval_ms": 125,
            })
        );
    }

    /// The report spells the barrier exactly as the flag accepts it, so it can be pasted back.
    #[test]
    fn the_barrier_spelling_round_trips_through_the_flag() {
        for spelling in ["ready-nodes", "none"] {
            assert_eq!(
                settings(&["--cluster-write-barrier", spelling]).to_json()["write_barrier"],
                spelling
            );
        }
    }
}
