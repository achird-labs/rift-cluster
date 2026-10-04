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

/// A response body produced as it is sent — the snapshot fetch (D-101), which serves a file
/// that may be tens of MiB and is never worth holding in memory as one buffer.
pub type ByteStream =
    Pin<Box<dyn futures_util::Stream<Item = std::io::Result<hyper::body::Bytes>> + Send>>;

/// What a streamed route answers with.
pub struct StreamReply {
    /// The response's `content-type`.
    pub content_type: &'static str,
    pub body: ByteStream,
}

/// A streamed route's response, or the error that answers instead of it.
pub type StreamFuture = Pin<Box<dyn Future<Output = Result<StreamReply, RpcError>> + Send>>;

/// A prefix-registered endpoint whose response is streamed (D-101). Called with the path remainder
/// after its prefix (query included, as for [`PrefixHandler`]); the request body is not handed on —
/// it is still read, capped and covered by the credential before dispatch, like every request.
pub trait StreamHandler: Send + Sync {
    fn call(&self, suffix: String) -> StreamFuture;
}

impl<F> StreamHandler for F
where
    F: Fn(String) -> StreamFuture + Send + Sync,
{
    fn call(&self, suffix: String) -> StreamFuture {
        self(suffix)
    }
}

/// The part of a request target that names a route: everything before the first
/// `?`.
///
/// The cluster port signs and verifies the whole `path_and_query` string (see
/// `server::dispatch`) — a handler that reads a query must not have an
/// unauthenticated input, so that must not change. But *routing* on the same
/// string made a query defeat the exact-match table entirely: `/x?y=1` 404'd
/// while `/x` resolved, even though they name the same resource (issue #156).
///
/// Deliberately raw: no percent-decoding and no normalization. The route table
/// holds ASCII literals, and decoding here would let two different signed
/// strings collapse onto one route — aliasing between what was verified and
/// what was dispatched, which is exactly the property the signature exists to
/// pin down.
fn route_path(path: &str) -> &str {
    path.split_once('?').map_or(path, |(before, _)| before)
}

/// `path` addressed to member `to` (D-96): the recipient travels as a `to` query parameter, inside
/// the signed `path_and_query`, so it cannot be rewritten in flight. Routing ignores the query, so
/// a receiver that predates the parameter dispatches the same handler.
#[must_use]
pub(crate) fn addressed(path: &str, to: u64) -> String {
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}to={to}")
}

/// The member a request is addressed to, read from the raw query the same way [`route_path`]
/// reads the path: `Ok(None)` when no `to` parameter is present.
///
/// # Errors
///
/// [`RpcError::BadRequest`] when `to` is empty, not plain decimal digits, out of range, or given
/// more than once. A signed request that names its recipient badly is a sender bug, and the gate
/// fails closed rather than treating it as unnamed.
pub(crate) fn recipient(path_and_query: &str) -> Result<Option<u64>, RpcError> {
    let Some((_, query)) = path_and_query.split_once('?') else {
        return Ok(None);
    };
    if query.split('&').any(|pair| pair == "to") {
        return Err(RpcError::BadRequest(
            "the recipient `to` has no value".into(),
        ));
    }
    let mut found = None;
    for value in query.split('&').filter_map(|pair| pair.strip_prefix("to=")) {
        if found.is_some() {
            return Err(RpcError::BadRequest(
                "the recipient `to` is given more than once".into(),
            ));
        }
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(RpcError::BadRequest(format!(
                "the recipient `to={value}` is not a node id"
            )));
        }
        let id = value.parse::<u64>().map_err(|_| {
            RpcError::BadRequest(format!("the recipient `to={value}` is out of range"))
        })?;
        found = Some(id);
    }
    Ok(found)
}

/// Method + path registry for the cluster port.
#[derive(Default, Clone)]
pub struct Router {
    routes: HashMap<(String, String), Arc<dyn Handler>>,
    prefix_routes: Vec<(String, String, Arc<dyn PrefixHandler>)>,
    stream_routes: Vec<(String, String, Arc<dyn StreamHandler>)>,
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
            .get(&(method.to_ascii_uppercase(), route_path(path).to_owned()))
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
        let route_path = route_path(path);
        self.prefix_routes
            .iter()
            .filter(|(m, prefix, _)| *m == method && route_path.starts_with(prefix.as_str()))
            .max_by_key(|(_, prefix, _)| prefix.len())
            // Sliced from the FULL string, not `route_path`: the
            // `PrefixHandler` contract promises the suffix carries the query,
            // and the handlers strip it themselves. Safe because the matched
            // prefix is a prefix of `route_path`, which is a prefix of `path`.
            .map(|(_, prefix, handler)| (handler, path[prefix.len()..].to_owned()))
    }

    /// Register a prefix endpoint whose response is streamed (D-101). Consulted after the exact
    /// and the buffered prefix tables; the longest matching prefix wins, as there.
    #[must_use]
    pub fn route_stream_prefix(
        mut self,
        method: &str,
        prefix: &str,
        handler: Arc<dyn StreamHandler>,
    ) -> Self {
        self.stream_routes
            .push((method.to_ascii_uppercase(), prefix.to_owned(), handler));
        self
    }

    pub(crate) fn lookup_stream_prefix(
        &self,
        method: &str,
        path: &str,
    ) -> Option<(&Arc<dyn StreamHandler>, String)> {
        let method = method.to_ascii_uppercase();
        let route_path = route_path(path);
        self.stream_routes
            .iter()
            .filter(|(m, prefix, _)| *m == method && route_path.starts_with(prefix.as_str()))
            .max_by_key(|(_, prefix, _)| prefix.len())
            .map(|(_, prefix, handler)| (handler, path[prefix.len()..].to_owned()))
    }

    /// Number of registered endpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len() + self.prefix_routes.len() + self.stream_routes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty() && self.prefix_routes.is_empty() && self.stream_routes.is_empty()
    }

    /// Fold `other`'s routes into `self`.
    ///
    /// Exists because a handful of route builders (`flow_routes`, `proxy_routes`) build their
    /// own table from `Router::new()` rather than accepting a `base: Router` to extend — each is a
    /// self-contained subsystem's whole surface, and taking a base would let one subsystem's
    /// builder observe another's routes for no reason. `merge` is how the composition root brings
    /// two such self-contained tables together into the one `Router` a `NodeConfig` can hold,
    /// without either builder needing to know the other exists.
    ///
    /// `other` wins on an exact-path collision, the same "later registration replaces earlier"
    /// contract [`Self::route`] documents — but never on a *prefix* collision: prefix routes are
    /// additive, so two subsystems that each claim a prefix keep both rather than one silently
    /// dropping.
    #[must_use]
    pub fn merge(mut self, other: Router) -> Self {
        self.routes.extend(other.routes);
        self.prefix_routes.extend(other.prefix_routes);
        self.stream_routes.extend(other.stream_routes);
        self
    }
}

#[cfg(test)]
mod merge_tests {
    use std::sync::Arc;

    use super::*;

    fn handler(body: Vec<u8>) -> HandlerFuture {
        Box::pin(async move { Ok(body) })
    }

    fn prefix_handler(_suffix: String, body: Vec<u8>) -> HandlerFuture {
        Box::pin(async move { Ok(body) })
    }

    #[test]
    fn merge_folds_both_tables_routes_and_prefixes() {
        let a = Router::new()
            .route("GET", "/a", Arc::new(handler))
            .route_prefix("GET", "/prefix-a/", Arc::new(prefix_handler));
        let b = Router::new()
            .route("GET", "/b", Arc::new(handler))
            .route_prefix("GET", "/prefix-b/", Arc::new(prefix_handler));

        let merged = a.merge(b);
        assert!(merged.lookup("GET", "/a").is_some());
        assert!(merged.lookup("GET", "/b").is_some());
        assert!(merged.lookup_prefix("GET", "/prefix-a/x").is_some());
        assert!(merged.lookup_prefix("GET", "/prefix-b/x").is_some());
        assert_eq!(merged.len(), 4);
    }

    #[test]
    fn merge_lets_the_argument_win_an_exact_path_collision() {
        let winner: Arc<dyn Handler> = Arc::new(handler);
        let loser: Arc<dyn Handler> = Arc::new(handler);
        let a = Router::new().route("GET", "/x", Arc::clone(&loser));
        let b = Router::new().route("GET", "/x", Arc::clone(&winner));

        let merged = a.merge(b);
        assert_eq!(merged.len(), 1, "the colliding path is one route, not two");
        let found = merged.lookup("GET", "/x").expect("route present");
        assert!(
            Arc::ptr_eq(found, &winner),
            "merge's argument must win an exact-path collision, matching `route`'s own contract"
        );
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

    // -- issue #156: a query string must not defeat routing ------------------

    fn echo() -> Arc<dyn Handler> {
        Arc::new(|body: Vec<u8>| -> HandlerFuture { Box::pin(async move { Ok(body) }) })
    }

    fn echo_suffix() -> Arc<dyn PrefixHandler> {
        Arc::new(|suffix: String, _body: Vec<u8>| -> HandlerFuture {
            Box::pin(async move { Ok(suffix.into_bytes()) })
        })
    }

    /// `dispatch` routes on the same `path_and_query` string it verified the
    /// signature over, so a query string used to miss the exact table entirely.
    /// `/x?y=1` names the same resource as `/x`.
    #[test]
    fn an_exact_route_resolves_with_a_query_string() {
        let router = Router::new().route("GET", "/_cluster/members", echo());
        assert!(router.lookup("GET", "/_cluster/members").is_some());
        assert!(
            router.lookup("GET", "/_cluster/members?probe=1").is_some(),
            "a query string must not turn a registered route into a 404"
        );
        // Only the query is ignored — a different path is still a different route.
        assert!(router.lookup("GET", "/_cluster/member?probe=1").is_none());
        assert!(router.lookup("GET", "/_cluster/members/extra").is_none());
    }

    /// An empty query (`/x?`) is still a query: the path component is `/x`.
    #[test]
    fn an_empty_query_string_still_resolves() {
        let router = Router::new().route("GET", "/x", echo());
        assert!(router.lookup("GET", "/x?").is_some());
    }

    /// The [`PrefixHandler`] contract promises the suffix carries "the path
    /// remainder after its prefix (query string included, if any)", and every
    /// existing handler strips it itself. Matching must ignore the query while
    /// the suffix must keep it — pinned so a later simplification cannot
    /// silently drop the query on the floor.
    #[test]
    fn a_prefix_route_matches_past_a_query_but_keeps_it_in_the_suffix() {
        let router = Router::new().route_prefix("POST", "/_cluster/things/", echo_suffix());
        let (_, suffix) = router
            .lookup_prefix("POST", "/_cluster/things/one/act?force=1")
            .expect("the query must not prevent the prefix from matching");
        assert_eq!(
            suffix, "one/act?force=1",
            "the handler owns its own query semantics, so the suffix carries it"
        );
    }

    /// A query sitting exactly at the prefix boundary yields an empty id, which
    /// is what the handlers already reject — it must not panic, and must not
    /// slice into the middle of a UTF-8 character or match some other route.
    #[test]
    fn a_query_at_the_prefix_boundary_yields_an_empty_id() {
        let router = Router::new().route_prefix("GET", "/_cluster/ops/", echo_suffix());
        let (_, suffix) = router
            .lookup_prefix("GET", "/_cluster/ops/?x=1")
            .expect("the prefix itself still matches");
        assert_eq!(suffix, "?x=1");
    }

    /// Exact-wins-over-prefix is documented on `route_prefix`; a query string
    /// must not flip that ordering. `/_cluster/things?x=1` is the collection, not
    /// a member of `/_cluster/things/`.
    #[test]
    fn a_query_does_not_let_a_prefix_steal_an_exact_route() {
        let router = Router::new()
            .route("GET", "/_cluster/things", echo())
            .route_prefix("GET", "/_cluster/things/", echo_suffix());
        assert!(router.lookup("GET", "/_cluster/things?x=1").is_some());
        assert!(
            router
                .lookup_prefix("GET", "/_cluster/things?x=1")
                .is_none(),
            "the collection route must not fall through to the member prefix"
        );
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

    /// Pins D-96: a request addressed to a member names it in the query, and a path that already
    /// carries one gains the recipient as a further parameter rather than a second `?`.
    #[test]
    fn a_member_address_is_carried_in_the_query() {
        assert_eq!(
            addressed("/internal/v1/raft/append", 3),
            "/internal/v1/raft/append?to=3"
        );
        assert_eq!(
            addressed("/_cluster/flow/get?x=1", 18446744073709551615),
            "/_cluster/flow/get?x=1&to=18446744073709551615"
        );
    }

    /// Pins D-96: the recipient parser reads the raw, undecoded query the way `route_path` does,
    /// and fails closed on anything that is not exactly one decimal id.
    #[test]
    fn the_recipient_is_one_decimal_id_or_absent() {
        assert_eq!(recipient("/internal/v1/raft/vote"), Ok(None));
        assert_eq!(recipient("/internal/v1/raft/vote?to=77"), Ok(Some(77)));
        assert_eq!(recipient("/_cluster/flow/get?x=1&to=77&y=2"), Ok(Some(77)));
        assert_eq!(
            recipient("/p?too=5"),
            Ok(None),
            "a parameter that merely starts with `to` is not it"
        );
        for bad in [
            "/p?to",
            "/p?x=1&to",
            "/p?to=",
            "/p?to=abc",
            "/p?to=-1",
            "/p?to=+7",
            "/p?to=18446744073709551616",
            "/p?to=1&to=2",
            "/p?to=0x10",
            "/p?to=%37%37",
        ] {
            assert!(
                matches!(recipient(bad), Err(RpcError::BadRequest(_))),
                "{bad} must be refused, got {:?}",
                recipient(bad)
            );
        }
    }

    /// Pins D-96's mixed-version claim: a receiver that knows nothing of the recipient still routes
    /// an addressed request to the same handler, because routing ignores the query.
    #[test]
    fn an_addressed_path_routes_to_the_same_handler() {
        let router = Router::new().route(
            "POST",
            "/internal/v1/raft/append",
            Arc::new(|_body: Vec<u8>| -> HandlerFuture { Box::pin(async { Ok(Vec::new()) }) }),
        );
        assert!(
            router
                .lookup("POST", &addressed("/internal/v1/raft/append", 3))
                .is_some()
        );
    }
}
