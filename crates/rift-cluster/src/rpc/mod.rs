//! Authenticated cluster-internal RPC (RFC-001 §7.3, §11.2).
//!
//! One HMAC-authenticated hyper endpoint per node, carrying whatever the
//! control plane and the state backends register into its [`Router`]. The
//! transport is deliberately protocol-agnostic: it authenticates, negotiates
//! version, and dispatches — it does not know what the payloads mean.

pub mod auth;
pub mod client;
pub mod routes;
pub mod server;

pub use auth::{AuthError, SignedRequest, Signer, Verifier};
pub use client::{
    AlwaysHealthy, Authority, AuthorityError, DnsResolver, PeerHealth, PeerResolver, RpcClient,
    RpcClientConfig, TrackedPeerHealth,
};
pub use routes::{Handler, HandlerFuture, PROTO_VERSION, PrefixHandler, ProtocolVersion, Router};
pub use server::{DEFAULT_MAX_BODY_BYTES, RpcServer, RpcServerConfig};

/// Connect timeout for a peer RPC.
pub const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Overall request timeout for a peer RPC.
pub const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Every way a cluster RPC can fail, typed end to end so call sites can map
/// each one onto the partition/degradation decision table rather than guessing
/// from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RpcError {
    /// The credential was missing, forged, stale, replayed, or unrecordable.
    #[error("unauthorized: {0}")]
    Unauthorized(#[from] AuthError),

    /// The peer speaks an incompatible protocol major.
    #[error("protocol version skew: peer {peer:?}, ours {ours}")]
    VersionSkew {
        peer: Option<ProtocolVersion>,
        ours: ProtocolVersion,
    },

    /// No handler is registered for this method and path.
    #[error("unknown route: {method} {path}")]
    UnknownRoute { method: String, path: String },

    /// The request body exceeded the transport's cap.
    #[error("request body exceeds {limit} bytes")]
    BodyTooLarge { limit: u64 },

    /// The deadline elapsed before the peer answered.
    #[error("request timed out")]
    Timeout,

    /// Connect/read/write failure, or a peer already known to be unhealthy.
    #[error("transport failure: {0}")]
    Transport(String),

    /// Refused locally to protect the data plane: no bridge permit was free.
    #[error("shed: no bridge capacity")]
    Shed,

    /// The request was well-formed transport-wise but the handler refused its
    /// content: a malformed body, or a domain rule the caller violated.
    ///
    /// Distinct from [`Self::Handler`] because the difference is the caller's to
    /// act on — a `BadRequest` fails identically on every retry and names
    /// something the operator can fix, while a `Handler` error is this node
    /// failing at something it should have been able to do.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// The cluster could not commit the write: no quorum, or no reachable
    /// leader. `op_id` is present when the op was durably accepted before the
    /// failure, so a client can poll `GET /_cluster/ops/:id` for its eventual
    /// outcome rather than guessing whether to retry (Ch. 4 write path).
    #[error("unavailable: {detail}")]
    Unavailable {
        detail: String,
        op_id: Option<String>,
    },

    /// The registered handler failed.
    #[error("handler error: {0}")]
    Handler(String),
}

impl RpcError {
    /// Stable label for `rift_cluster_rpc_failures_total{reason}`.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Unauthorized(e) => e.reason(),
            Self::VersionSkew { .. } => "version_skew",
            Self::UnknownRoute { .. } => "unknown_route",
            Self::BodyTooLarge { .. } => "body_too_large",
            Self::Timeout => "timeout",
            Self::Transport(_) => "transport",
            Self::Shed => "shed",
            Self::BadRequest(_) => "bad_request",
            Self::Unavailable { .. } => "unavailable",
            Self::Handler(_) => "handler",
        }
    }

    /// HTTP status a server answers this error with.
    #[must_use]
    pub fn status(&self) -> u16 {
        match self {
            Self::Unauthorized(_) => 401,
            // "Upgrade Required": the peer must move to a compatible major.
            Self::VersionSkew { .. } => 426,
            Self::UnknownRoute { .. } => 404,
            Self::BodyTooLarge { .. } => 413,
            Self::Timeout => 504,
            Self::Transport(_) => 502,
            Self::Shed => 503,
            Self::BadRequest(_) => 400,
            Self::Unavailable { .. } => 503,
            Self::Handler(_) => 500,
        }
    }

    /// Whether retrying the same request against the same peer could succeed.
    /// Authentication and routing failures are deterministic — retrying them
    /// only burns the deadline.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout | Self::Transport(_))
    }

    /// Whether this failure says anything about the *peer being reachable*, as
    /// opposed to the peer answering and declining.
    ///
    /// Only these count against a peer's health. A `Handler` error is proof the
    /// peer is alive — it replied — and an `Unauthorized`/`VersionSkew`/
    /// `UnknownRoute` is a configuration fault that a health cooldown cannot
    /// help with and would only obscure.
    #[must_use]
    pub fn is_liveness_failure(&self) -> bool {
        matches!(self, Self::Timeout | Self::Transport(_) | Self::Shed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_reasons_and_statuses_are_distinct_per_class() {
        let cases = [
            (RpcError::Unauthorized(AuthError::BadMac), "bad_mac", 401),
            (
                RpcError::Unauthorized(AuthError::StaleTimestamp),
                "stale_timestamp",
                401,
            ),
            (
                RpcError::Unauthorized(AuthError::ReplayedNonce),
                "replayed_nonce",
                401,
            ),
            (
                RpcError::Unauthorized(AuthError::NonceCacheFull),
                "nonce_cache_full",
                401,
            ),
            (
                RpcError::VersionSkew {
                    peer: None,
                    ours: PROTO_VERSION,
                },
                "version_skew",
                426,
            ),
            (
                RpcError::UnknownRoute {
                    method: "GET".into(),
                    path: "/x".into(),
                },
                "unknown_route",
                404,
            ),
            (RpcError::BodyTooLarge { limit: 32 }, "body_too_large", 413),
            (RpcError::Timeout, "timeout", 504),
            (RpcError::Transport("reset".into()), "transport", 502),
            (RpcError::Shed, "shed", 503),
            (RpcError::Handler("boom".into()), "handler", 500),
        ];
        for (err, reason, status) in cases {
            assert_eq!(err.reason(), reason, "{err:?}");
            assert_eq!(err.status(), status, "{err:?}");
        }
    }

    #[test]
    fn only_transient_failures_are_retryable() {
        assert!(RpcError::Timeout.is_retryable());
        assert!(RpcError::Transport("reset".into()).is_retryable());
        for err in [
            RpcError::Unauthorized(AuthError::BadMac),
            RpcError::VersionSkew {
                peer: None,
                ours: PROTO_VERSION,
            },
            RpcError::UnknownRoute {
                method: "GET".into(),
                path: "/x".into(),
            },
            RpcError::BodyTooLarge { limit: 32 },
            RpcError::Shed,
            // A refused request fails the same way every time.
            RpcError::BadRequest("bad".into()),
            // Not retryable *by the transport*: the op may already be parked
            // and on its way, so the client polls `GET /_cluster/ops/:id`
            // rather than re-submitting and racing its own write.
            RpcError::Unavailable {
                detail: "no quorum".into(),
                op_id: None,
            },
        ] {
            assert!(!err.is_retryable(), "{err:?}");
        }
    }
}
