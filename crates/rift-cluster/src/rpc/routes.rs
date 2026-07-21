//! Protocol version negotiation and the route registry.
//!
//! The registry ships empty. Membership, config-sync and the state backends
//! register their own endpoints into it, so the transport does not need to know
//! which control-plane design it is carrying.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::RpcError;

/// Header carrying the sender's protocol version.
pub const PROTO_HEADER: &str = "x-rift-cluster-proto";

/// The version this build speaks.
pub const PROTO_VERSION: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

/// Wire protocol version. Within a major, every gossip key and RPC field is
/// additive, so a minor difference is compatible in both directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    /// Parse a `<major>.<minor>` header value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let (major, minor) = value.trim().split_once('.')?;
        Some(Self {
            major: major.parse().ok()?,
            minor: minor.parse().ok()?,
        })
    }

    /// Whether this build can serve a peer announcing `self`.
    #[must_use]
    pub fn is_compatible_with(&self, other: ProtocolVersion) -> bool {
        self.major == other.major
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Check a peer's announced version against this build's.
///
/// A missing header is treated as skew rather than as "assume compatible": a
/// peer that cannot say what it speaks is a peer this build cannot promise to
/// understand.
pub fn negotiate(header: Option<&str>) -> Result<ProtocolVersion, RpcError> {
    let peer = header
        .and_then(ProtocolVersion::parse)
        .ok_or(RpcError::VersionSkew {
            peer: None,
            ours: PROTO_VERSION,
        })?;
    if PROTO_VERSION.is_compatible_with(peer) {
        Ok(peer)
    } else {
        Err(RpcError::VersionSkew {
            peer: Some(peer),
            ours: PROTO_VERSION,
        })
    }
}

/// A handler's response body.
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, RpcError>> + Send>>;

/// One registered endpoint. Handlers receive the raw request body and return
/// the raw response body; encoding is the caller's business, not the
/// transport's.
pub trait Handler: Send + Sync {
    fn call(&self, body: Vec<u8>) -> HandlerFuture;
}

impl<F> Handler for F
where
    F: Fn(Vec<u8>) -> HandlerFuture + Send + Sync,
{
    fn call(&self, body: Vec<u8>) -> HandlerFuture {
        self(body)
    }
}

/// A prefix-registered endpoint: called with the path remainder after its
/// prefix (query string included, if any) plus the request body. Exists for
/// the id-addressed operator routes (`/_cluster/ops/:id`) the exact-match
/// table cannot express.
pub trait PrefixHandler: Send + Sync {
    fn call(&self, suffix: String, body: Vec<u8>) -> HandlerFuture;
}

impl<F> PrefixHandler for F
where
    F: Fn(String, Vec<u8>) -> HandlerFuture + Send + Sync,
{
    fn call(&self, suffix: String, body: Vec<u8>) -> HandlerFuture {
        self(suffix, body)
    }
}

/// Method + path registry for the cluster port.
#[derive(Default, Clone)]
pub struct Router {
    routes: HashMap<(String, String), Arc<dyn Handler>>,
    prefix_routes: Vec<(String, String, Arc<dyn PrefixHandler>)>,
}

impl Router {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an endpoint. Later registrations for the same key replace
    /// earlier ones, so a composed server has one owner per route.
    #[must_use]
    pub fn route(mut self, method: &str, path: &str, handler: Arc<dyn Handler>) -> Self {
        self.routes
            .insert((method.to_ascii_uppercase(), path.to_owned()), handler);
        self
    }

    pub(crate) fn lookup(&self, method: &str, path: &str) -> Option<&Arc<dyn Handler>> {
        self.routes
            .get(&(method.to_ascii_uppercase(), path.to_owned()))
    }

    /// Register a prefix endpoint. Exact routes always win over prefixes, and
    /// the longest matching prefix wins among prefixes, so registration order
    /// never matters.
    #[must_use]
    pub fn route_prefix(
        mut self,
        method: &str,
        prefix: &str,
        handler: Arc<dyn PrefixHandler>,
    ) -> Self {
        self.prefix_routes
            .push((method.to_ascii_uppercase(), prefix.to_owned(), handler));
        self
    }

    pub(crate) fn lookup_prefix(
        &self,
        method: &str,
        path: &str,
    ) -> Option<(&Arc<dyn PrefixHandler>, String)> {
        let method = method.to_ascii_uppercase();
        self.prefix_routes
            .iter()
            .filter(|(m, prefix, _)| *m == method && path.starts_with(prefix.as_str()))
            .max_by_key(|(_, prefix, _)| prefix.len())
            .map(|(_, prefix, handler)| (handler, path[prefix.len()..].to_owned()))
    }

    /// Number of registered endpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len() + self.prefix_routes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty() && self.prefix_routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_minor_drift_accepted() {
        let peer = ProtocolVersion {
            major: PROTO_VERSION.major,
            minor: PROTO_VERSION.minor + 7,
        };
        assert_eq!(negotiate(Some(&peer.to_string())), Ok(peer));
    }

    #[test]
    fn version_major_mismatch_rejected() {
        let peer = ProtocolVersion {
            major: PROTO_VERSION.major + 1,
            minor: 0,
        };
        assert!(matches!(
            negotiate(Some(&peer.to_string())),
            Err(RpcError::VersionSkew { peer: Some(p), .. }) if p == peer
        ));
    }

    #[test]
    fn version_missing_or_unparseable_is_skew() {
        for header in [None, Some("garbage"), Some("1"), Some("x.y")] {
            assert!(
                matches!(negotiate(header), Err(RpcError::VersionSkew { .. })),
                "header {header:?}"
            );
        }
    }

    #[test]
    fn router_starts_empty_and_registers() {
        let router = Router::new();
        assert!(router.is_empty());
        let router = router.route(
            "post",
            "/internal/v1/ping",
            Arc::new(|body: Vec<u8>| -> HandlerFuture { Box::pin(async move { Ok(body) }) }),
        );
        assert_eq!(router.len(), 1);
        // Method matching is case-insensitive; paths are exact.
        assert!(router.lookup("POST", "/internal/v1/ping").is_some());
        assert!(router.lookup("GET", "/internal/v1/ping").is_none());
        assert!(router.lookup("POST", "/internal/v1/pong").is_none());
    }
}
