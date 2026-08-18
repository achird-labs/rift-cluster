//! The MCP server's client half: a thin typed wrapper over `reqwest` pointed at a
//! remote admin front.
//!
//! This process holds no node state and embeds no engine — it is a client, and every
//! tool is one admin API call (RFC-006 §8.1).

use std::time::Duration;

use reqwest::{StatusCode, Url};
use serde::Serialize;

use super::args::{ApiKey, McpArgs, StartupError};

// The admin front's own header names, not re-spelled literals: `decorate` is where
// they are defined and documented, and a second copy here is a drift point that
// would fail silently (a dropped marker reads exactly like a complete answer).
use rift_cluster::decorate::{HEADER_NEXT_INDEX, HEADER_PARTIAL, HEADER_TRUNCATED};

/// Whether an answer describes the whole fleet or only the node that served it.
///
/// An enum rather than a bare string because the distinction is conditional
/// (see [`Answer`]) and a call site that spells it wrong tells an agent a node
/// read is a fleet read — the exact dishonesty #292 was written to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ReadScope {
    /// Merged across the fleet (`savedRequests` merge-on-read, #223/#225/#229).
    Fleet,
    /// Served by the answering node alone.
    Node,
}

/// A tool answer plus the provenance an agent needs to trust it.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct Answer<T> {
    pub data: T,
    /// Absent for reads where the fleet/node distinction does not arise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<ReadScope>,
    /// The `Rift-Cluster-Partial` header, verbatim, when the front stamped one.
    ///
    /// `Option` rather than a defaulted empty string: the header is defined as
    /// present-only-when-partial, so its absence is a domain value ("complete"),
    /// not a missing field to paper over.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<String>,
    /// `x-rift-next-index` — the opaque cursor to pass back as `since`.
    ///
    /// Without this the `since` parameter is undocumentable in practice: an agent
    /// has no other way to obtain a valid cursor, so a journal larger than one page
    /// simply cannot be paged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_index: Option<String>,
    /// `x-rift-truncated` — set when retention dropped entries from this answer.
    ///
    /// Additive-only upstream (present or absent, never `false`). Dropping it would
    /// present a short answer as a complete one, which is the same silence `partial`
    /// exists to prevent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

impl<T> Answer<T> {
    /// An answer with no provenance to report — nothing the front stamped, and no
    /// fleet/node distinction to draw.
    pub fn plain(data: T) -> Self {
        Self {
            data,
            scope: None,
            partial: None,
            next_index: None,
            truncated: None,
        }
    }

    /// An answer carrying every marker the front stamped.
    ///
    /// The single construction path for tool answers, so a new marker cannot be
    /// added to [`Markers`] and then silently forgotten at six of seven call sites.
    pub fn new(data: T, scope: Option<ReadScope>, markers: Markers) -> Self {
        Self {
            data,
            scope,
            partial: markers.partial,
            next_index: markers.next_index,
            truncated: markers.truncated,
        }
    }
}

/// A tool call that failed. Becomes a structured MCP tool error, never a panic.
#[derive(Debug, thiserror::Error)]
pub enum ToolFailure {
    /// The admin front could not be reached at all.
    #[error("could not reach the admin API at {url}: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// The credential was rejected.
    ///
    /// Split out from [`Self::Api`] only so the message can name the two causes a
    /// human actually hits. The body is the API's own and never echoes credentials.
    #[error(
        "the admin API rejected this key (401). Check the key file's contents, and note that \
         the header is the raw key — a `Bearer ` prefix is a different credential. Response: {body}"
    )]
    Unauthorized { body: String },

    /// Any other non-2xx. The body is relayed **verbatim** (RFC-006 §9.4): the API's
    /// error bodies never contain credentials, and summarising them here would strip
    /// the detail the agent needs to correct itself.
    #[error("the admin API answered {status}: {body}")]
    Api { status: u16, body: String },

    /// A 2xx whose body was not the JSON the schema promises.
    ///
    /// Propagated rather than defaulted: a silent `{}` here would surface as a
    /// confusing empty answer in the agent with nothing to correlate server-side.
    ///
    /// The body is carried for the same reason [`Self::Api`] carries one — a serde
    /// position ("expected value at line 1 column 1") does not tell anyone that a
    /// reverse proxy returned an HTML page.
    #[error(
        "the admin API answered {status} with a body that is not valid JSON: {source}. Body: {body}"
    )]
    MalformedBody {
        status: u16,
        body: String,
        #[source]
        source: serde_json::Error,
    },

    /// An endpoint path that does not resolve against the base URL.
    ///
    /// Unreachable for the fixed literals and `u16` ports every call site passes,
    /// and it stays that way only because this is an error rather than a fallback:
    /// resolving to the base URL instead would send a real request to the wrong
    /// endpoint, and a root path that happens to answer `200` would then be
    /// returned to the agent as though it were the resource it asked for.
    #[error("could not resolve the admin path {path:?} against the base URL")]
    BadEndpoint { path: String },
}

/// An authenticated client for one admin front.
#[derive(Debug, Clone)]
pub struct AdminClient {
    base: Url,
    key: ApiKey,
    http: reqwest::Client,
}

impl AdminClient {
    pub fn new(args: &McpArgs) -> Result<Self, StartupError> {
        check_url(&args.url)?;
        let key = ApiKey::load(&args.api_key_file)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(args.timeout_secs))
            .build()
            .map_err(StartupError::Client)?;
        Ok(Self {
            base: normalize_base(args.url.clone()),
            key,
            http,
        })
    }

    /// The absolute URL for an admin path.
    ///
    /// `path` is relative and must not start with `/`: a leading slash makes
    /// `Url::join` discard any path prefix on the base, so
    /// `https://host/rift` + `/imposters` would silently become
    /// `https://host/imposters` and hit nothing.
    pub fn endpoint(&self, path: &str) -> Result<Url, ToolFailure> {
        self.base.join(path).map_err(|_| ToolFailure::BadEndpoint {
            path: path.to_owned(),
        })
    }

    /// `GET` an admin path, returning its JSON body and the front's markers.
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let url = self.endpoint(path)?;
        let request = self
            .http
            .get(url.clone())
            .header(reqwest::header::AUTHORIZATION, self.key.header_value())
            .query(query);
        self.send(request, &url).await
    }

    /// `POST` a JSON body to an admin path.
    pub async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let url = self.endpoint(path)?;
        let request = self
            .http
            .post(url.clone())
            .header(reqwest::header::AUTHORIZATION, self.key.header_value())
            .json(body);
        self.send(request, &url).await
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        url: &Url,
    ) -> Result<(serde_json::Value, Markers), ToolFailure> {
        let response = request
            .send()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;

        let status = response.status();
        let markers = Markers::read(response.headers());

        let body = response
            .text()
            .await
            .map_err(|source| ToolFailure::Transport {
                url: url.to_string(),
                source,
            })?;

        classify(status, body, markers)
    }
}

/// The provenance headers the admin front stamps on a read.
///
/// A struct over the headers, and separate from [`classify`] on purpose: the first
/// version of this gate tested only that `classify` *passes a marker through*, which
/// left the code that actually reads the headers uncovered — deleting the read
/// entirely kept the suite green. Extracting it is what makes the read assertable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Markers {
    pub partial: Option<String>,
    pub next_index: Option<String>,
    pub truncated: Option<String>,
}

impl Markers {
    fn read(headers: &reqwest::header::HeaderMap) -> Self {
        Self {
            partial: header_str(headers, HEADER_PARTIAL),
            next_index: header_str(headers, HEADER_NEXT_INDEX),
            truncated: header_str(headers, HEADER_TRUNCATED),
        }
    }
}

/// One header as a string, or `None`.
///
/// A value whose bytes are not UTF-8 yields `None`. That is a **domain-optional**
/// absence, not a swallowed error: all three of these headers are additive markers
/// whose absence already means something definite, and none of them gates the
/// answer's data. An unreadable one is no more informative than a missing one.
fn header_str(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Turn a status + body into either a parsed answer or a typed failure.
///
/// Split out from [`AdminClient::send`] as a pure function so the status handling
/// is testable without a live server — the branch that decides whether an agent
/// sees the API's real error is not one to leave covered only by integration tests.
fn classify(
    status: StatusCode,
    body: String,
    markers: Markers,
) -> Result<(serde_json::Value, Markers), ToolFailure> {
    if status == StatusCode::UNAUTHORIZED {
        return Err(ToolFailure::Unauthorized { body });
    }
    if !status.is_success() {
        return Err(ToolFailure::Api {
            status: status.as_u16(),
            body,
        });
    }
    // A 2xx with an empty body is a legitimate shape (204, and some deletes), and
    // `null` is its honest JSON rendering — not an error, and not a fabricated `{}`.
    if body.trim().is_empty() {
        return Ok((serde_json::Value::Null, markers));
    }
    // A `match` rather than `map_err`: the failure arm needs to *move* `body` into
    // the error, which it cannot do while `from_str` still borrows it.
    match serde_json::from_str(&body) {
        Ok(value) => Ok((value, markers)),
        Err(source) => Err(ToolFailure::MalformedBody {
            status: status.as_u16(),
            body,
            source,
        }),
    }
}

/// Refuse a base URL this process must not carry a credential to.
///
/// Two checks, both about the key rather than about correctness:
///
/// * **Userinfo is refused.** `Url::to_string()` renders `user:password@host`, and
///   [`ToolFailure::Transport`] interpolates the URL — so a password in `--url`
///   would be printed straight into the agent's transcript. It is the one credential
///   path the [`ApiKey`] redaction cannot cover, because it never becomes an `ApiKey`.
/// * **Plaintext to a non-loopback host warns.** The whole point of `--api-key-file`
///   is that the key is hard to leak; sending it over cleartext HTTP to a remote host
///   undoes that. Loopback is exempt — it is what the tests and a local fleet use.
fn check_url(url: &Url) -> Result<(), StartupError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(StartupError::UrlHasUserinfo);
    }
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    );
    if url.scheme() != "https" && !loopback {
        tracing::warn!(
            host = url.host_str().unwrap_or("<none>"),
            "sending the API key over plaintext HTTP to a non-loopback host; prefer https"
        );
    }
    Ok(())
}

/// Make the base URL a directory URL, so `join` extends its path instead of
/// replacing the last segment.
///
/// `https://host:2525` and `https://host:2525/` must address the same fleet; without
/// this they differ, and the second form is what people paste from a browser.
fn normalize_base(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(raw: &str) -> Url {
        normalize_base(Url::parse(raw).expect("test URL must parse"))
    }

    /// E6 — the two spellings a human produces must address the same endpoint.
    #[test]
    fn base_url_with_and_without_trailing_slash_agree() {
        assert_eq!(
            base("https://fleet.example:2525")
                .join("imposters")
                .unwrap(),
            base("https://fleet.example:2525/")
                .join("imposters")
                .unwrap()
        );
    }

    /// E6 — a base carrying a path prefix must keep it. This is the case the
    /// normalization exists for; without it, `join` replaces `rift` instead of
    /// extending it and every request silently misses the mount point.
    #[test]
    fn base_url_path_prefix_is_preserved() {
        let joined = base("https://fleet.example/rift")
            .join("imposters")
            .unwrap();
        assert_eq!(joined.as_str(), "https://fleet.example/rift/imposters");
    }

    /// A URL carrying userinfo is refused rather than normalized.
    ///
    /// `Url::to_string()` renders `user:password@host`, and the transport error
    /// message interpolates the URL — so a password in `--url` would be printed
    /// straight into the agent's transcript. That is the one credential path the
    /// `ApiKey` redaction does not cover, so it is closed at the door.
    #[test]
    fn a_url_with_userinfo_is_refused() {
        let err = check_url(&Url::parse("https://agent:s3cret@fleet.example:2525").unwrap())
            .expect_err("userinfo must be refused");
        assert!(
            matches!(err, StartupError::UrlHasUserinfo),
            "expected UrlHasUserinfo, got {err:?}"
        );
        let rendered = err.to_string();
        assert!(
            !rendered.contains("s3cret"),
            "the refusal must not echo the password: {rendered}"
        );
    }

    #[test]
    fn a_url_without_userinfo_is_accepted() {
        assert!(check_url(&Url::parse("https://fleet.example:2525").unwrap()).is_ok());
        assert!(check_url(&Url::parse("http://127.0.0.1:2525").unwrap()).is_ok());
    }

    /// E12 — a 401 is its own variant so the message can explain it...
    #[test]
    fn unauthorized_status_is_its_own_failure() {
        let err = classify(
            StatusCode::UNAUTHORIZED,
            r#"{"error":"unauthorized"}"#.to_owned(),
            Markers::default(),
        )
        .expect_err("401 must fail");
        assert!(
            matches!(err, ToolFailure::Unauthorized { .. }),
            "expected Unauthorized, got {err:?}"
        );
    }

    /// E12 — ...and that message must not be a place a key could appear. The only
    /// interpolated value is the API's own body.
    #[test]
    fn unauthorized_message_carries_only_the_api_body() {
        let err = ToolFailure::Unauthorized {
            body: r#"{"error":"unauthorized"}"#.to_owned(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains(r#"{"error":"unauthorized"}"#));
        assert!(
            rendered.contains("Bearer "),
            "the 401 message should name the Bearer-prefix trap: {rendered}"
        );
    }

    /// E10 — the API's error body reaches the agent unaltered. Summarising it here
    /// would strip exactly the detail an agent needs to correct its next call.
    #[test]
    fn api_error_body_is_relayed_verbatim() {
        let body = r#"{"code":"invalid_predicate","detail":"unknown operator 'startsWith'"}"#;
        let err = classify(StatusCode::BAD_REQUEST, body.to_owned(), Markers::default())
            .expect_err("400 must fail");
        match err {
            ToolFailure::Api { status, body: got } => {
                assert_eq!(status, 400);
                assert_eq!(got, body, "the body must be relayed byte-for-byte");
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    /// E11 — a non-2xx with an empty body still reports its status rather than
    /// becoming a parse error or a panic.
    #[test]
    fn empty_error_body_still_relays_status() {
        let err = classify(StatusCode::BAD_GATEWAY, String::new(), Markers::default())
            .expect_err("502 must fail");
        match err {
            ToolFailure::Api { status, body } => {
                assert_eq!(status, 502);
                assert_eq!(body, "");
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    /// E11 — an HTML error page from a proxy in front of the fleet is a realistic
    /// non-JSON body, and must not be mistaken for a malformed *success*.
    #[test]
    fn non_json_error_body_is_an_api_failure_not_a_parse_failure() {
        let err = classify(
            StatusCode::SERVICE_UNAVAILABLE,
            "<html><body>503 Service Unavailable</body></html>".to_owned(),
            Markers::default(),
        )
        .expect_err("503 must fail");
        assert!(
            matches!(err, ToolFailure::Api { status: 503, .. }),
            "expected Api 503, got {err:?}"
        );
    }

    /// A 2xx whose body is not JSON is a distinct, named failure — never a
    /// silently-defaulted empty object — and it carries the body, because a serde
    /// column number does not tell anyone a proxy returned an HTML page.
    #[test]
    fn malformed_success_body_is_reported_with_its_body() {
        let err = classify(
            StatusCode::OK,
            "<html>not json at all</html>".to_owned(),
            Markers::default(),
        )
        .expect_err("a non-JSON 200 must fail");
        // The rendered message must carry the body — a serde column number does not
        // tell anyone a proxy returned an HTML page.
        assert!(
            err.to_string().contains("<html>not json at all</html>"),
            "the rendered message must carry the body: {err}"
        );
        match err {
            ToolFailure::MalformedBody { status, body, .. } => {
                assert_eq!(status, 200);
                assert_eq!(body, "<html>not json at all</html>");
            }
            other => panic!("expected MalformedBody, got {other:?}"),
        }
    }

    /// A 2xx with no body is legitimate and renders as JSON `null`, not as an error.
    #[test]
    fn empty_success_body_is_json_null() {
        let (value, _) = classify(StatusCode::NO_CONTENT, String::new(), Markers::default())
            .expect("an empty 2xx is not a failure");
        assert_eq!(value, serde_json::Value::Null);
    }

    /// E9 — the headers are actually *read* off the response.
    ///
    /// This is the assertion the first draft of this gate was missing: it tested
    /// only that `classify` forwards a marker it is handed, so deleting the read
    /// itself left the suite green.
    #[test]
    fn markers_are_read_from_the_response_headers() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HEADER_PARTIAL,
            reqwest::header::HeaderValue::from_static("node-3 unreachable"),
        );
        headers.insert(
            HEADER_NEXT_INDEX,
            reqwest::header::HeaderValue::from_static("cursor-42"),
        );
        headers.insert(
            HEADER_TRUNCATED,
            reqwest::header::HeaderValue::from_static("true"),
        );

        let markers = Markers::read(&headers);
        assert_eq!(markers.partial.as_deref(), Some("node-3 unreachable"));
        assert_eq!(markers.next_index.as_deref(), Some("cursor-42"));
        assert_eq!(markers.truncated.as_deref(), Some("true"));
    }

    /// E9 — no headers means a complete, unpaged, untruncated answer, and every
    /// marker is `None` rather than an empty string that would read as "partial".
    #[test]
    fn markers_are_none_when_no_header_is_present() {
        assert_eq!(
            Markers::read(&reqwest::header::HeaderMap::new()),
            Markers::default()
        );
    }

    /// The three markers are independent: a paged answer is not a partial one.
    #[test]
    fn next_index_alone_does_not_imply_partial() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HEADER_NEXT_INDEX,
            reqwest::header::HeaderValue::from_static("cursor-42"),
        );
        let markers = Markers::read(&headers);
        assert_eq!(markers.next_index.as_deref(), Some("cursor-42"));
        assert_eq!(markers.partial, None);
        assert_eq!(markers.truncated, None);
    }

    /// E9 — markers survive `classify` on the success path.
    #[test]
    fn markers_reach_the_answer() {
        let markers = Markers {
            partial: Some("node-3 unreachable".to_owned()),
            next_index: Some("cursor-7".to_owned()),
            truncated: Some("true".to_owned()),
        };
        let (_, got) =
            classify(StatusCode::OK, "[]".to_owned(), markers.clone()).expect("200 must succeed");
        assert_eq!(got, markers);
    }

    /// The scope enum's wire spelling is what an agent reads; pin it literally.
    #[test]
    fn read_scope_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&ReadScope::Fleet).unwrap(),
            r#""fleet""#
        );
        assert_eq!(
            serde_json::to_string(&ReadScope::Node).unwrap(),
            r#""node""#
        );
    }

    /// An answer with nothing to qualify carries none of the optional keys, rather
    /// than nulls an agent would have to learn to ignore.
    #[test]
    fn plain_answer_omits_every_optional_field() {
        let json = serde_json::to_string(&Answer::plain(serde_json::json!({"ok": true})))
            .expect("serialize");
        assert_eq!(json, r#"{"data":{"ok":true}}"#);
    }

    /// And an answer that has markers renders all of them, under the names the
    /// tool descriptions tell an agent to look for.
    #[test]
    fn answer_renders_every_marker_it_carries() {
        let answer = Answer::new(
            serde_json::json!([]),
            Some(ReadScope::Fleet),
            Markers {
                partial: Some("node-3 unreachable".to_owned()),
                next_index: Some("cursor-7".to_owned()),
                truncated: Some("true".to_owned()),
            },
        );
        let json = serde_json::to_value(&answer).expect("serialize");
        assert_eq!(json["scope"], "fleet");
        assert_eq!(json["partial"], "node-3 unreachable");
        assert_eq!(json["next_index"], "cursor-7");
        assert_eq!(json["truncated"], "true");
    }
}
