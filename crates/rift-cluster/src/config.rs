//! Cluster startup configuration and the guards that refuse an unsafe fleet.
//!
//! These checks run before anything binds. Each one exists because the
//! alternative is a cluster that looks healthy and is quietly wrong.

use std::net::{IpAddr, SocketAddr};

/// Whether an address is one the cluster port may bind without an explicit
/// acknowledgment.
///
/// This is an allowlist rather than a "reject the wildcard" check: the threat
/// model delegates confidentiality to network isolation, so anything this
/// function cannot positively classify as private has to be treated as
/// reachable from outside the trust boundary and fail closed.
fn is_private_address(ip: IpAddr) -> bool {
    match ip {
        // `0.0.0.0` / `::` bind every interface, public ones included.
        IpAddr::V4(v4) if v4.is_unspecified() => false,
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) if v6.is_unspecified() => false,
        // `is_unique_local`/`is_unicast_link_local` are still unstable, so
        // match the prefixes directly: fc00::/7 and fe80::/10.
        IpAddr::V6(v6) => {
            let [a, b, ..] = v6.octets();
            v6.is_loopback() || (a & 0xfe) == 0xfc || (a == 0xfe && (b & 0xc0) == 0x80)
        }
    }
}

/// The data plane's accept/runtime topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeTopology {
    /// Multi-threaded work-stealing tokio runtime.
    #[default]
    WorkStealing,
    /// One single-threaded runtime per core with per-worker listeners.
    PerCore,
}

/// Why a cluster refused to start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("--cluster requires --cluster-bind (no default: the cluster port must be explicit)")]
    BindRequired,

    #[error(
        "--cluster-bind {0} is a public interface; pass --cluster-bind-public-ok to acknowledge \
         that gossip and internal RPC will be reachable from outside this host"
    )]
    PublicBindNotAcknowledged(SocketAddr),

    #[error(
        "--cluster requires --cluster-secret or --cluster-secret-file; pass --cluster-insecure \
         to run an unauthenticated cluster port anyway"
    )]
    SecretRequired,

    #[error(
        "--cluster is not supported with --runtime per-core: the state bridge parks caller \
         threads, and a per-core worker has only one thread to park, so a single owner outage \
         would stall every connection pinned to that worker"
    )]
    PerCoreUnsupported,

    #[error(
        "--cluster is not supported with the TLS-MITM intercept listener \
         (--intercept-port or an `intercept` config block): intercept state is per-node and is \
         not replicated, so a clustered fleet would answer the same client differently depending \
         on which node it reached"
    )]
    InterceptUnsupported,

    #[error(
        "--cluster is not supported with --configfile: the file's imposters would load into this \
         node alone, outside the replicated log, and the reconciler would then delete them in \
         favour of the cluster's set — restore state through the admin API of any member instead"
    )]
    ConfigfileUnsupported,

    /// The secret file could not be read or was empty. Kept distinct from
    /// [`Self::SecretRequired`] so an operator who *did* pass the flag is told
    /// about the file rather than about the flag — and so an unreadable secret
    /// can never degrade into an unauthenticated cluster port.
    #[error("--cluster-secret-file {path} could not be read: {detail}")]
    SecretFileUnreadable { path: String, detail: String },
}

/// Everything the cluster needs decided before it starts.
///
/// The `Default` is a single node with clustering off — the same shape an
/// operator gets by passing no `--cluster*` flags at all.
#[derive(Debug, Clone, Default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub bind: Option<SocketAddr>,
    pub bind_public_ok: bool,
    pub secret: Option<String>,
    pub insecure: bool,
    pub runtime: RuntimeTopology,
    /// Whether the data plane was asked to run the TLS-MITM intercept listener.
    pub intercept: bool,
}

impl ClusterConfig {
    /// Apply every startup guard. `--cluster` with `--runtime per-core` or the
    /// intercept listener is refused here, before anything binds (D-14).
    /// Order is deliberate: the cheapest and most
    /// commonly wrong settings report first, so an operator fixing a fresh
    /// deployment sees one clear problem at a time.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            // Everything cluster-related is inert without the master switch,
            // so a stray flag is not an error.
            return Ok(());
        }

        if self.runtime == RuntimeTopology::PerCore {
            return Err(ConfigError::PerCoreUnsupported);
        }

        if self.intercept {
            return Err(ConfigError::InterceptUnsupported);
        }

        let bind = self.bind.ok_or(ConfigError::BindRequired)?;

        if !is_private_address(bind.ip()) && !self.bind_public_ok {
            return Err(ConfigError::PublicBindNotAcknowledged(bind));
        }

        if self.secret.as_ref().is_none_or(|s| s.is_empty()) && !self.insecure {
            return Err(ConfigError::SecretRequired);
        }

        Ok(())
    }

    /// Whether this node runs its cluster port without authentication.
    /// Surfaced as `rift_cluster_insecure` so a fleet can be audited for it.
    #[must_use]
    pub fn is_insecure(&self) -> bool {
        self.enabled && self.secret.as_ref().is_none_or(|s| s.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            bind: Some("10.0.0.5:4790".parse().expect("valid address")),
            secret: Some("shared-secret".into()),
            ..Default::default()
        }
    }

    #[test]
    fn guard_accepts_a_complete_config() {
        assert_eq!(valid().validate(), Ok(()));
        assert!(!valid().is_insecure());
    }

    /// Pins D-14: `--cluster` + `--runtime per-core` is refused at startup — a
    /// per-core worker has one thread to park, so an owner outage would stall
    /// every connection pinned to it.
    #[test]
    fn guard_rejects_per_core_runtime() {
        let config = ClusterConfig {
            runtime: RuntimeTopology::PerCore,
            ..valid()
        };
        assert_eq!(config.validate(), Err(ConfigError::PerCoreUnsupported));
        // The message must name both flags — an operator hits this at 3am.
        let msg = ConfigError::PerCoreUnsupported.to_string();
        assert!(msg.contains("--cluster"), "{msg}");
        assert!(msg.contains("per-core"), "{msg}");
    }

    #[test]
    fn guard_requires_secret_unless_insecure() {
        let config = ClusterConfig {
            secret: None,
            ..valid()
        };
        assert_eq!(config.validate(), Err(ConfigError::SecretRequired));

        let empty = ClusterConfig {
            secret: Some(String::new()),
            ..valid()
        };
        assert_eq!(empty.validate(), Err(ConfigError::SecretRequired));

        let acknowledged = ClusterConfig {
            secret: None,
            insecure: true,
            ..valid()
        };
        assert_eq!(acknowledged.validate(), Ok(()));
        assert!(acknowledged.is_insecure());
    }

    /// Pins D-14: `--cluster` + the intercept listener is refused at startup —
    /// intercept state is per-node and unreplicated.
    #[test]
    fn guard_rejects_intercept_mode() {
        let config = ClusterConfig {
            intercept: true,
            ..valid()
        };
        assert_eq!(config.validate(), Err(ConfigError::InterceptUnsupported));
        let msg = ConfigError::InterceptUnsupported.to_string();
        assert!(msg.contains("--cluster"), "{msg}");
        assert!(msg.contains("intercept"), "{msg}");
    }

    /// #85: the message has to name both flags and the way out, because the
    /// failure it replaces was silent — imposters loaded and then deleted, with
    /// nothing logged. An operator meeting this for the first time needs to
    /// know what to do instead, not just that they cannot do this.
    #[test]
    fn the_configfile_refusal_names_both_flags_and_the_alternative() {
        let msg = ConfigError::ConfigfileUnsupported.to_string();
        assert!(msg.contains("--configfile"), "{msg}");
        assert!(msg.contains("--cluster"), "{msg}");
        assert!(msg.contains("admin API"), "{msg}");
    }

    #[test]
    fn guard_requires_an_explicit_bind() {
        let config = ClusterConfig {
            bind: None,
            ..valid()
        };
        assert_eq!(config.validate(), Err(ConfigError::BindRequired));
    }

    #[test]
    fn guard_requires_acknowledging_a_public_bind() {
        // Not just the wildcard: binding a routable interface directly is the
        // same exposure and must need the same acknowledgment.
        for addr in [
            "0.0.0.0:4790",
            "[::]:4790",
            "203.0.113.5:4790",
            "[2001:db8::1]:4790",
        ] {
            let public: SocketAddr = addr.parse().expect("valid address");
            let config = ClusterConfig {
                bind: Some(public),
                ..valid()
            };
            assert_eq!(
                config.validate(),
                Err(ConfigError::PublicBindNotAcknowledged(public)),
                "address {addr}"
            );

            let acknowledged = ClusterConfig {
                bind_public_ok: true,
                ..config
            };
            assert_eq!(acknowledged.validate(), Ok(()), "address {addr}");
        }
    }

    #[test]
    fn guard_allows_private_binds_without_acknowledgment() {
        for addr in [
            "127.0.0.1:4790",
            "10.0.0.5:4790",
            "172.16.0.1:4790",
            "192.168.1.10:4790",
            "169.254.1.1:4790",
            "[::1]:4790",
            "[fd00::1]:4790",
            "[fe80::1]:4790",
        ] {
            let config = ClusterConfig {
                bind: Some(addr.parse().expect("valid address")),
                ..valid()
            };
            assert_eq!(config.validate(), Ok(()), "address {addr}");
        }
    }

    #[test]
    fn guards_are_inert_without_the_master_switch() {
        // A per-core, secretless, unbound config is fine as long as clustering
        // is off — that is just an ordinary single node.
        let config = ClusterConfig {
            enabled: false,
            runtime: RuntimeTopology::PerCore,
            intercept: true,
            ..Default::default()
        };
        assert_eq!(config.validate(), Ok(()));
        assert!(!config.is_insecure());
    }
}
