//! The per-imposter flow-state knobs a provider-supplied store reads out of
//! `_rift.flowState` (upstream #845's `extra` passthrough, this repo's #118).
//!
//! Three knobs, all with correct-by-default polarity (the same shape as
//! `--cluster-degraded-mode reject`): reads are owner-authoritative unless an
//! imposter opts into replica staleness, writes are group-fsynced unless it
//! opts into losing them, and flow ids are per-imposter unless it opts into
//! sharing them fleet-wide.
//!
//! Parsing is strict on the values and silent on unrelated keys: `extra` is a
//! generic passthrough, so a key this module does not own is not an error — but
//! a key it *does* own carrying a value it cannot interpret is refused, at
//! admission time, with a 400. Wrong-but-quiet is the failure mode this repo's
//! error rules exist to prevent, and a typo'd `"durabillity": "sync"` costing an
//! imposter its durability silently would be exactly that.

use rift_cluster_base::seams::ImposterConfig;

use super::shard::Durability;

/// Which copy a flow-state read consults.
///
/// The `lowercase` serialization is the same vocabulary [`FlowConfig::from_imposter`] parses, so
/// the knob the admin read publishes (#370) round-trips through the value an operator wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "lowercase")]
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

/// Which imposters share one flow-id namespace (#152, RFC-005 §3.5).
///
/// Flow ids are caller-chosen — `flowIdSource: "header:X-Session"` turns a
/// header value into one — so two unrelated imposters reading the same header
/// produce the same id as a matter of course. Whether that means "the same
/// context" is a deployment's choice, and this is where it makes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContextScope {
    /// Each imposter gets its own namespace. The default, and the behaviour
    /// single-node Rift has always had for free: one `FlowStore` instance per
    /// imposter isolates them without anyone asking.
    #[default]
    Imposter,
    /// One namespace for the whole fleet: imposters deliberately share
    /// contexts, so a suite spanning two mocks can carry one session across
    /// both.
    Fleet,
}

impl ContextScope {
    /// The namespace prefix every flow id carries under this scope.
    ///
    /// `Fleet` renders `f:` rather than passing the id through bare so the
    /// namespaces are disjoint *by construction*: a caller-chosen flow id that
    /// happens to read like `i6400:cart` cannot otherwise be made to collide
    /// with imposter 6400's `cart`.
    #[must_use]
    pub fn prefix_for(self, port: Option<u16>) -> String {
        match (self, port) {
            (Self::Fleet, _) => "f:".to_owned(),
            (Self::Imposter, Some(port)) => format!("i{port}:"),
            // Admission refuses a portless config (`validate_replicable_config`),
            // and upstream's manager assigns the port back into the config
            // before the provider ever sees it, so this is doubly unreachable.
            // It still has to answer something, and the answer must not be the
            // fleet namespace: collapsing an imposter that cannot be identified
            // into the shared one is precisely the bleed this scope exists to
            // prevent. Logged because if a future admission path *does* reach
            // here, every portless imposter would quietly share this one
            // namespace — a silent regression of #152, and this is the only
            // thing that would make it greppable.
            (Self::Imposter, None) => {
                tracing::error!(
                    "imposter-scoped flow store built without a port; falling back to an isolated \
                     placeholder namespace. Every portless imposter shares it — this should be \
                     unreachable, so it means an admission path stopped requiring a port."
                );
                "i?:".to_owned()
            }
        }
    }

    /// The key a flow's state is actually stored and **owned** under.
    ///
    /// The one place this composition lives. Both callers need the identical
    /// string or they disagree about who owns a flow: the store writes under it
    /// ([`super::flow::ClusteredFlowStore`]), and the admin front hashes it to
    /// answer "which node holds this flow" (#359). A second implementation is
    /// exactly the disagreement that would send an operator to the wrong node,
    /// so there is one function and both call it.
    #[must_use]
    pub fn scoped_flow_id(self, port: Option<u16>, flow_id: &str) -> String {
        format!("{}{flow_id}", self.prefix_for(port))
    }
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
    pub scope: ContextScope,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            read_consistency: ReadConsistency::Strong,
            durability: Durability::Async,
            ttl_seconds: Some(300),
            scope: ContextScope::Imposter,
        }
    }
}

/// The keys this module owns inside `flowState`'s `extra` map.
const KEY_READ_CONSISTENCY: &str = "readConsistency";
const KEY_DURABILITY: &str = "durability";
const KEY_CONTEXT_SCOPE: &str = "contextScope";

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

        let scope = match flow_state.extra.get(KEY_CONTEXT_SCOPE) {
            None => ContextScope::Imposter,
            Some(value) => match value.as_str() {
                Some("imposter") => ContextScope::Imposter,
                Some("fleet") => ContextScope::Fleet,
                // Reserved, not unknown: the message names the work that
                // activates it, so a config written for a later build reads as
                // "not yet" rather than as a typo.
                Some("tenant") => {
                    return Err(format!(
                        "flowState.{KEY_CONTEXT_SCOPE} reserved value \"tenant\": tenant scope arrives with RFC-002 (#17); accepted: \"imposter\", \"fleet\""
                    ));
                }
                _ => {
                    return Err(format!(
                        "flowState.{KEY_CONTEXT_SCOPE} must be \"imposter\" or \"fleet\", got {value}"
                    ));
                }
            },
        };

        Ok(Self {
            read_consistency,
            durability,
            ttl_seconds: (flow_state.ttl_seconds > 0).then_some(flow_state.ttl_seconds),
            scope,
        })
    }

    /// Admission-time validation: parse and discard. Called from
    /// [`crate::control::validate`] so a bad value is refused with a 400
    /// *before* the op is committed — [`FlowStoreProvider::provide`] has no
    /// error channel (it returns `Option`), so by the time the provider sees a
    /// config it must already be valid.
    ///
    /// [`FlowStoreProvider::provide`]: rift_cluster_base::seams::FlowStoreProvider::provide
    ///
    /// # Errors
    ///
    /// Same as [`FlowConfig::from_imposter`].
    pub fn validate(config: &ImposterConfig) -> Result<(), String> {
        Self::from_imposter(config).map(|_| ())
    }
}

/// Upstream's default when `flowState.flowIdSource` is absent: each imposter is its own flow.
const DEFAULT_FLOW_ID_SOURCE: &str = "imposter_port";

/// Where a published knob's effective value came from (#370).
///
/// `pub(crate)`: [`ResolvedKnobs`] renders itself, so no caller outside this crate ever names one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnobSource {
    /// The imposter never mentioned the key, so the built-in default applies.
    Default,
    /// The imposter set the key explicitly — whatever value it chose, *including* one that
    /// happens to equal the default.
    Set,
}

impl KnobSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Set => "set",
        }
    }

    /// Provenance is presence of the key, never equality with the default value: an operator who
    /// pinned `durability: "async"` deliberately made a choice, and rendering that as "inherited"
    /// would invite the next operator to go and change a fleet default instead.
    const fn of(present: bool) -> Self {
        if present { Self::Set } else { Self::Default }
    }
}

/// The three per-imposter `_rift` knobs the admin imposter read publishes (#370).
///
/// Upstream's `expose_flow_state` is an allowlist — `backend`, `ttlSeconds` and `flowIdSource`
/// only — because `flowState.redis` can carry a credentialed connection URL. `durability` and
/// `readConsistency` therefore never reach a client by passthrough, and this is what the EE front
/// decorates the read with instead.
///
/// It is built from the *parsed* knobs rather than from the stored document, which is what keeps
/// upstream's redaction intact: there is no path by which `redis` can enter this struct.
/// Provenance is read separately from the raw config, because parsing resolves `None => default`
/// and destroys it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedKnobs {
    durability: Knob<Durability>,
    read_consistency: Knob<ReadConsistency>,
    flow_id_source: Knob<String>,
}

/// One knob's effective value and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Knob<T> {
    value: T,
    source: KnobSource,
}

impl<T: serde::Serialize> Knob<T> {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "value": self.value, "source": self.source.as_str() })
    }
}

impl ResolvedKnobs {
    /// Resolve the three published knobs and where each came from.
    ///
    /// # Errors
    ///
    /// Same as [`FlowConfig::from_imposter`] — a value this module cannot interpret for a key it
    /// owns. Admission refuses those before they commit, so reaching this is a record written out
    /// of band; publishing it as a default would be exactly the wrong-but-quiet answer.
    pub fn from_imposter(config: &ImposterConfig) -> Result<Self, String> {
        let parsed = FlowConfig::from_imposter(config)?;
        let flow_state = config.rift.as_ref().and_then(|r| r.flow_state.as_ref());

        let present = |key: &str| flow_state.is_some_and(|fs| fs.extra.contains_key(key));
        let flow_id_source = flow_state.and_then(|fs| fs.flow_id_source.as_deref());

        Ok(Self {
            durability: Knob {
                value: parsed.durability,
                source: KnobSource::of(present(KEY_DURABILITY)),
            },
            read_consistency: Knob {
                value: parsed.read_consistency,
                source: KnobSource::of(present(KEY_READ_CONSISTENCY)),
            },
            flow_id_source: Knob {
                value: flow_id_source
                    .map_or_else(|| DEFAULT_FLOW_ID_SOURCE.to_owned(), str::to_owned),
                source: KnobSource::of(flow_id_source.is_some()),
            },
        })
    }

    /// Render as the `_rift.flowStateResolved` block.
    ///
    /// `contextScope` is deliberately absent: it belongs to #288, and publishing it here would
    /// quietly claim ownership of a knob another issue is still designing.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "durability": self.durability.to_json(),
            "readConsistency": self.read_consistency.to_json(),
            "flowIdSource": self.flow_id_source.to_json(),
        })
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
    fn context_scope_defaults_to_imposter() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
        }))
        .expect("parses");
        assert_eq!(
            FlowConfig::from_imposter(&config).expect("valid").scope,
            ContextScope::Imposter,
            "the default must be the OSS-parity scope, not the pre-#152 fleet namespace"
        );

        // Present-but-empty `flowState` takes the same default as an absent one.
        assert_eq!(
            FlowConfig::from_imposter(&imposter(serde_json::json!({})))
                .expect("valid")
                .scope,
            ContextScope::Imposter
        );
    }

    #[test]
    fn both_context_scopes_parse() {
        assert_eq!(
            FlowConfig::from_imposter(&imposter(serde_json::json!({ "contextScope": "imposter" })))
                .expect("valid")
                .scope,
            ContextScope::Imposter
        );
        assert_eq!(
            FlowConfig::from_imposter(&imposter(serde_json::json!({ "contextScope": "fleet" })))
                .expect("valid")
                .scope,
            ContextScope::Fleet
        );
    }

    #[test]
    fn an_unknown_context_scope_is_refused_naming_the_key_and_the_accepted_set() {
        let err = FlowConfig::validate(&imposter(serde_json::json!({ "contextScope": "galaxy" })))
            .expect_err("unknown scope must refuse");
        assert!(err.contains("contextScope"), "{err}");
        assert!(err.contains("galaxy"), "{err}");
        assert!(err.contains("imposter"), "{err}");
        assert!(err.contains("fleet"), "{err}");
    }

    /// `tenant` is reserved, not unknown: it is the scope RFC-002 activates, so
    /// the refusal points at that work rather than reading as a typo.
    #[test]
    fn the_reserved_tenant_scope_is_refused_naming_its_successor() {
        let err = FlowConfig::validate(&imposter(serde_json::json!({ "contextScope": "tenant" })))
            .expect_err("tenant scope must refuse");
        assert!(err.contains("contextScope"), "{err}");
        assert!(err.contains("tenant"), "{err}");
        assert!(
            err.contains("#17"),
            "the reserved-op wording names the issue that lifts the reservation: {err}"
        );
    }

    /// The prefixes must be mutually unambiguous, including the defensive
    /// portless arm — its whole justification is that it is *not* the shared
    /// namespace, and nothing else pins that.
    #[test]
    fn scope_prefixes_are_distinct_and_the_portless_arm_never_shares() {
        let fleet = ContextScope::Fleet.prefix_for(Some(6400));
        let portless = ContextScope::Imposter.prefix_for(None);

        assert_eq!(ContextScope::Imposter.prefix_for(Some(6400)), "i6400:");
        assert_eq!(fleet, "f:");
        assert_ne!(
            portless, fleet,
            "an imposter that cannot be identified must not fall into the fleet namespace"
        );
        assert_ne!(portless, ContextScope::Imposter.prefix_for(Some(6400)));

        // The port is rendered in decimal and terminated by a `:`, which is not
        // a digit — that is what makes the scheme uniquely decodable, so a flow
        // id can never be chosen to impersonate another imposter's prefix.
        assert!(ContextScope::Fleet.prefix_for(None).starts_with('f'));
        for port in [1u16, 640, 6400, 65535] {
            let prefix = ContextScope::Imposter.prefix_for(Some(port));
            assert!(prefix.ends_with(':'), "{prefix}");
            assert_eq!(prefix.matches(':').count(), 1, "{prefix}");
        }
    }

    #[test]
    fn non_positive_ttl_means_no_default_expiry() {
        let parsed = FlowConfig::from_imposter(&imposter(serde_json::json!({
            "ttlSeconds": 0,
        })))
        .expect("valid");
        assert_eq!(parsed.ttl_seconds, None);
    }

    /// Issue #370. An imposter that never mentioned `_rift` still reads as three knobs at their
    /// built-in defaults — not as an error, and not as a missing block. The console's panel has to
    /// render *something* for every imposter, and "inherited" is the honest answer.
    #[test]
    fn resolved_knobs_report_defaults_when_the_rift_block_is_absent() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
        }))
        .expect("parses");

        let knobs = ResolvedKnobs::from_imposter(&config).expect("valid");
        let json = knobs.to_json();

        assert_eq!(json["durability"]["value"], "async");
        assert_eq!(json["durability"]["source"], "default");
        assert_eq!(json["readConsistency"]["value"], "strong");
        assert_eq!(json["readConsistency"]["source"], "default");
        assert_eq!(json["flowIdSource"]["value"], "imposter_port");
        assert_eq!(json["flowIdSource"]["source"], "default");
    }

    /// Issue #370. Each knob set explicitly reads back as `set`, carrying the value that was set.
    #[test]
    fn resolved_knobs_report_set_when_the_key_is_present() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": {
                "durability": "sync",
                "readConsistency": "local",
                "flowIdSource": "header:X-Session",
            }},
        }))
        .expect("parses");

        let json = ResolvedKnobs::from_imposter(&config)
            .expect("valid")
            .to_json();

        assert_eq!(
            json["durability"],
            serde_json::json!({"value": "sync", "source": "set"})
        );
        assert_eq!(
            json["readConsistency"],
            serde_json::json!({"value": "local", "source": "set"})
        );
        assert_eq!(
            json["flowIdSource"],
            serde_json::json!({"value": "header:X-Session", "source": "set"})
        );
    }

    /// Issue #370, and the test that matters most. Provenance is **presence of the key**, not
    /// equality with the default value. An operator who deliberately pinned `durability: "async"`
    /// has made a choice, and a panel that renders it as "inherited" invites the next operator to
    /// change the fleet default instead — which is the confusion the whole inherited-vs-set
    /// distinction exists to prevent. Implementing this by comparing against the default is the
    /// obvious shortcut, and this is what fails when someone takes it.
    #[test]
    fn a_knob_set_to_its_own_default_value_still_reads_as_set() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": {
                "durability": "async",
                "readConsistency": "strong",
                "flowIdSource": "imposter_port",
            }},
        }))
        .expect("parses");

        let json = ResolvedKnobs::from_imposter(&config)
            .expect("valid")
            .to_json();

        assert_eq!(json["durability"]["value"], "async");
        assert_eq!(json["durability"]["source"], "set");
        assert_eq!(json["readConsistency"]["value"], "strong");
        assert_eq!(json["readConsistency"]["source"], "set");
        assert_eq!(json["flowIdSource"]["value"], "imposter_port");
        assert_eq!(json["flowIdSource"]["source"], "set");
    }

    /// Issue #370 publishes three knobs. `contextScope` is #288's (RFC-005 S1), and putting it here
    /// would quietly claim ownership of a knob another issue is still designing.
    #[test]
    fn context_scope_is_not_in_the_resolved_block() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": { "contextScope": "fleet" }},
        }))
        .expect("parses");

        let json = ResolvedKnobs::from_imposter(&config)
            .expect("valid")
            .to_json();

        assert!(
            json.get("contextScope").is_none(),
            "contextScope belongs to #288"
        );
        assert_eq!(json.as_object().expect("an object").len(), 3);
    }

    /// Issue #370. A stored value this module cannot interpret is refused rather than published as
    /// a default — the same polarity `from_imposter` already has. Admission should have caught it,
    /// so reaching here means the record predates the check or was written out of band; either way
    /// a knob rendered as "async, inherited" when the document says something else is the
    /// wrong-but-quiet failure this repo's error rules exist to prevent.
    #[test]
    fn resolved_knobs_refuse_a_value_they_cannot_interpret() {
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "_rift": { "flowState": { "durability": "sometimes" }},
        }))
        .expect("parses");

        assert!(ResolvedKnobs::from_imposter(&config).is_err());
    }
}
