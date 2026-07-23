//! The per-imposter flow-state knobs a provider-supplied store reads out of
//! `_rift.flowState` (upstream #845's `extra` passthrough, this repo's #118).
//!
//! Two knobs, both with correct-by-default polarity (the same shape as
//! `--cluster-degraded-mode reject`): reads are owner-authoritative unless an
//! imposter opts into replica staleness, and writes are group-fsynced unless it
//! opts into losing them.
//!
//! Parsing is strict on the values and silent on unrelated keys: `extra` is a
//! generic passthrough, so a key this module does not own is not an error — but
//! a key it *does* own carrying a value it cannot interpret is refused, at
//! admission time, with a 400. Wrong-but-quiet is the failure mode this repo's
//! error rules exist to prevent, and a typo'd `"durabillity": "sync"` costing an
//! imposter its durability silently would be exactly that.

use rift_ee::seams::ImposterConfig;

use super::shard::Durability;

/// Which copy a flow-state read consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Every read is answered by the key's owner — correct under any load
    /// balancing, one LAN RPC when the owner is another node.
    #[default]
    Strong,
    /// Reads stay on the local replica: fast, and at most one replication push
    /// behind the owner. The contract the imposter opted into, not a
    /// degradation.
    Local,
}

/// The parsed `flowState` block, with everything the clustered store needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowConfig {
    pub read_consistency: ReadConsistency,
    pub durability: Durability,
    /// Per-entry TTL, from upstream's `ttlSeconds` (default 300 there).
    /// `None` when non-positive: upstream treats `<= 0` as "expire now" per
    /// key-op, but as a *default* TTL a non-positive value means "no expiry".
    pub ttl_seconds: Option<i64>,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            read_consistency: ReadConsistency::Strong,
            durability: Durability::Async,
            ttl_seconds: Some(300),
        }
    }
}

/// The keys this module owns inside `flowState`'s `extra` map.
const KEY_READ_CONSISTENCY: &str = "readConsistency";
const KEY_DURABILITY: &str = "durability";

impl FlowConfig {
    /// Parse an imposter's `flowState` block. Absent block ⇒ all defaults —
    /// under `--cluster` every imposter gets the clustered store, configured or
    /// not, because scenario state on one node behind a round-robin LB is
    /// wrong for *every* imposter, not just the ones that thought about it.
    ///
    /// # Errors
    ///
    /// A value this module cannot interpret for a key it owns. The message is
    /// client-facing (it becomes the 400's `reason`), so it names the key, the
    /// offending value, and the accepted set.
    pub fn from_imposter(config: &ImposterConfig) -> Result<Self, String> {
        let Some(flow_state) = config.rift.as_ref().and_then(|r| r.flow_state.as_ref()) else {
            return Ok(Self::default());
        };

        let read_consistency = match flow_state.extra.get(KEY_READ_CONSISTENCY) {
            None => ReadConsistency::Strong,
            Some(value) => match value.as_str() {
                Some("strong") => ReadConsistency::Strong,
                Some("local") => ReadConsistency::Local,
                _ => {
                    return Err(format!(
                        "flowState.{KEY_READ_CONSISTENCY} must be \"strong\" or \"local\", got {value}"
                    ));
                }
            },
        };

        let durability = match flow_state.extra.get(KEY_DURABILITY) {
            None => Durability::Async,
            Some(value) => match value.as_str() {
                Some("none") => Durability::None,
                Some("async") => Durability::Async,
                Some("sync") => Durability::Sync,
                _ => {
                    return Err(format!(
                        "flowState.{KEY_DURABILITY} must be \"none\", \"async\" or \"sync\", got {value}"
                    ));
                }
            },
        };

        Ok(Self {
            read_consistency,
            durability,
            ttl_seconds: (flow_state.ttl_seconds > 0).then_some(flow_state.ttl_seconds),
        })
    }

    /// Admission-time validation: parse and discard. Called from
    /// [`crate::control::validate`] so a bad value is refused with a 400
    /// *before* the op is committed — [`FlowStoreProvider::provide`] has no
    /// error channel (it returns `Option`), so by the time the provider sees a
    /// config it must already be valid.
    ///
    /// [`FlowStoreProvider::provide`]: rift_ee::seams::FlowStoreProvider::provide
    ///
    /// # Errors
    ///
    /// Same as [`FlowConfig::from_imposter`].
    pub fn validate(config: &ImposterConfig) -> Result<(), String> {
        Self::from_imposter(config).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn imposter(flow_state: serde_json::Value) -> ImposterConfig {
        serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": flow_state },
        }))
        .expect("imposter config parses")
    }

    #[test]
    fn absent_block_yields_the_correct_by_default_config() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
        }))
        .expect("parses");

        let parsed = FlowConfig::from_imposter(&config).expect("valid");
        assert_eq!(parsed.read_consistency, ReadConsistency::Strong);
        assert_eq!(parsed.durability, Durability::Async);
        assert_eq!(parsed.ttl_seconds, Some(300));
    }

    #[test]
    fn both_knobs_parse() {
        let parsed = FlowConfig::from_imposter(&imposter(serde_json::json!({
            "readConsistency": "local",
            "durability": "sync",
            "ttlSeconds": 60,
        })))
        .expect("valid");

        assert_eq!(parsed.read_consistency, ReadConsistency::Local);
        assert_eq!(parsed.durability, Durability::Sync);
        assert_eq!(parsed.ttl_seconds, Some(60));
    }

    /// The acceptance criterion: an unknown value is a refusal, never a silent
    /// default. A typo'd knob that quietly meant something else would surface
    /// as wrong behaviour with nothing server-side to correlate.
    #[test]
    fn unknown_values_are_refused_with_a_reason_that_names_the_key() {
        let err = FlowConfig::validate(&imposter(serde_json::json!({
            "readConsistency": "eventual",
        })))
        .expect_err("unknown consistency must refuse");
        assert!(err.contains("readConsistency"), "{err}");
        assert!(err.contains("eventual"), "{err}");

        let err = FlowConfig::validate(&imposter(serde_json::json!({
            "durability": "paranoid",
        })))
        .expect_err("unknown durability must refuse");
        assert!(err.contains("durability"), "{err}");

        // Wrong *type*, not just wrong string.
        let err = FlowConfig::validate(&imposter(serde_json::json!({
            "durability": 3,
        })))
        .expect_err("a non-string durability must refuse");
        assert!(err.contains('3'), "{err}");
    }

    /// Keys this module does not own pass through untouched — `extra` is a
    /// generic map, and refusing a stranger's key would make this provider the
    /// arbiter of vocabulary it has no claim to.
    #[test]
    fn unowned_extra_keys_are_ignored() {
        FlowConfig::validate(&imposter(serde_json::json!({
            "someFutureKnob": { "nested": true },
        })))
        .expect("unowned keys are not this module's business");
    }

    #[test]
    fn non_positive_ttl_means_no_default_expiry() {
        let parsed = FlowConfig::from_imposter(&imposter(serde_json::json!({
            "ttlSeconds": 0,
        })))
        .expect("valid");
        assert_eq!(parsed.ttl_seconds, None);
    }
}
