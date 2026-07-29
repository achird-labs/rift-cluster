//! Transport for shipping audit batches to a customer-owned sink (issue
//! #164).
//!
//! A sink is named by a URI, the same shape a [`crate::sources`] provider is:
//! `https://` ships each batch as one webhook `POST`, `s3://<bucket>/<prefix>`
//! ships each batch as one object `PUT`. This module is the transport only —
//! it knows how to serialize a batch onto the wire and nothing about *when*
//! to ship, how large a batch gets, or what happens after a [`SinkTransport`]
//! returns an error. That is deliberate: retry/backoff policy belongs to the
//! caller that owns the schedule, not to the thing that makes one HTTP call.
//!
//! ## Auth
//!
//! Both transports take an optional `auth_ref`, resolved the same way a
//! credentialed imposter source resolves one
//! ([`crate::sources::auth::resolve_off_thread`]): a *named* ref that fails to
//! resolve is a hard error, never a silent fall-through to an unauthenticated
//! request. `None` means the sink was configured with no credential at all —
//! a webhook behind network-level auth, or a bucket reached by an ambient
//! role — and ships unauthenticated/unsigned, which is a deliberate choice by
//! whoever configured the sink, not a swallowed failure.
//!
//! ## Framing
//!
//! Every transport ships the same wire format: JSON Lines — each row's JSON
//! (already serialized by the caller) on its own line, `\n`-separated. One
//! format for both transports means a customer's ingestion side only ever
//! parses one shape, regardless of which sink kind their tenant is configured
//! with.
//!
//! ## Errors never carry the response body
//!
//! Neither transport reads a non-2xx response body into its error — the same
//! choice [`crate::sources::s3`] makes for the same reason: a sink is a
//! customer-controlled endpoint, and echoing what it sent back into our error
//! string (logs, an admin API response) would let a hostile or misconfigured
//! endpoint inject content into places we do not expect attacker-controlled
//! text to land.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;

use crate::sources::auth::{self, CredentialResolver};
use crate::sources::s3::{self, S3Config};

/// Whole-request budget: connect, headers and body. Same figure as
/// [`crate::sources::s3::REQUEST_TIMEOUT`] — there is no reason a sink write
/// should be allowed to hang longer than a source read is.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One shipped batch's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shipped {
    /// How many rows were in the batch.
    pub rows: usize,
    /// The size, in bytes, of the body that was sent on the wire.
    pub bytes: usize,
}

/// A transport that can ship a batch of serialized audit rows.
///
/// `async fn` in a trait is not yet usable as a trait object on this
/// toolchain without `async-trait` — a dependency this crate does not carry
/// (see the module doc's framing note: no new dependency is worth pulling in
/// for one desugaring), so the method is written out in the same
/// `Pin<Box<dyn Future<..> + Send>>` shape
/// [`crate::sources::CredentialedSource::fetch_with_auth`] already uses for
/// exactly this reason.
pub trait SinkTransport: Send + Sync + std::fmt::Debug {
    /// Ship `rows` (already-serialized `AuditRow` JSON, one String per row).
    ///
    /// `batch_start_revision` is the first row's revision. A transport that
    /// needs a deterministic name for the batch (currently only
    /// [`S3Sink`]) derives it from this rather than from wall-clock time or a
    /// random id, so a retried ship of the *same* batch overwrites the same
    /// object instead of leaving a duplicate.
    fn ship<'a>(
        &'a self,
        rows: &'a [String],
        batch_start_revision: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Shipped>> + Send + 'a>>;
}

/// Build the transport for a sink URI.
///
/// # Errors
/// If `uri`'s scheme is neither `https://` nor `s3://`, or if the underlying
/// HTTP client cannot be built.
pub fn transport_for(
    uri: &str,
    auth_ref: Option<&str>,
    resolver: &Arc<dyn CredentialResolver>,
    s3_config: &S3Config,
) -> anyhow::Result<Arc<dyn SinkTransport>> {
    // Trimmed for the same reason `control::validate` trims: the two must
    // classify a given URI identically, and the apply arm stores the trimmed
    // form. Trimming here as well means a record written before that
    // canonicalization still builds.
    let uri = uri.trim();
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = parse_bucket_and_prefix(rest)
            .with_context(|| format!("sink uri {uri:?} is not `s3://<bucket>/<prefix>`"))?;
        let sink = S3Sink::new(
            bucket,
            prefix,
            auth_ref.map(str::to_owned),
            Arc::clone(resolver),
            s3_config.clone(),
        )?;
        return Ok(Arc::new(sink));
    }
    // The cleartext carve-out is `control::is_loopback_http`'s to define, not
    // this function's. Admission and egress ask the same predicate so they
    // cannot drift — a sink `validate` accepts and this factory refuses is a
    // sink that commits cleanly and then silently exports nothing.
    if uri.starts_with("https://") || crate::control::is_loopback_http(uri) {
        let sink = WebhookSink::new(
            uri.to_owned(),
            auth_ref.map(str::to_owned),
            Arc::clone(resolver),
        )?;
        return Ok(Arc::new(sink));
    }
    anyhow::bail!(
        "audit sink uri {uri:?} has an unsupported scheme; this build ships to `https://` and \
         `s3://` sinks only (plus a loopback `http://` collector)"
    );
}

/// Frame `rows` as JSON Lines: each row on its own line, terminated by `\n` —
/// including the last, so a streaming ndjson consumer never has to special-
/// case end-of-batch to know the final row is complete.
fn ndjson(rows: &[String]) -> Vec<u8> {
    let mut body = String::new();
    for row in rows {
        body.push_str(row);
        body.push('\n');
    }
    body.into_bytes()
}

// ---------------------------------------------------------------------------
// Webhook
// ---------------------------------------------------------------------------

/// Ships an audit batch to a customer-owned HTTPS endpoint as one `POST` of
/// JSON Lines.
pub struct WebhookSink {
    url: String,
    auth_ref: Option<String>,
    resolver: Arc<dyn CredentialResolver>,
    client: reqwest::Client,
}

impl std::fmt::Debug for WebhookSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookSink")
            .field("url", &self.url)
            .field("auth_ref", &self.auth_ref)
            .finish_non_exhaustive()
    }
}

impl WebhookSink {
    fn new(
        url: String,
        auth_ref: Option<String>,
        resolver: Arc<dyn CredentialResolver>,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the webhook sink's HTTP client")?;
        Ok(Self {
            url,
            auth_ref,
            resolver,
            client,
        })
    }
}

impl SinkTransport for WebhookSink {
    fn ship<'a>(
        &'a self,
        rows: &'a [String],
        _batch_start_revision: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Shipped>> + Send + 'a>> {
        Box::pin(async move {
            // Fail closed, before anything goes over the wire: a named
            // credential that does not resolve must never fall through to an
            // unauthenticated POST.
            let credential = auth::resolve_off_thread(&self.resolver, self.auth_ref.as_deref())
                .await
                .with_context(|| format!("resolving auth for webhook sink {}", self.url))?;

            let body = ndjson(rows);
            let bytes = body.len();

            let mut request = self
                .client
                .post(&self.url)
                .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                .body(body);
            if let Some(credential) = &credential {
                request = request.header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", credential.expose()),
                );
            }

            let response = request
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("shipping audit batch to {}: {e}", self.url))?;

            let status = response.status();
            if !status.is_success() {
                // The body is never read on this path — see the module doc:
                // a sink endpoint that echoes request details in an error
                // body must not be able to put anything into our error
                // string.
                anyhow::bail!("audit webhook {} returned HTTP {status}", self.url);
            }

            Ok(Shipped {
                rows: rows.len(),
                bytes,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

/// Ships an audit batch to an S3 (or S3-compatible) bucket as one object
/// `PUT` per batch.
pub struct S3Sink {
    bucket: String,
    prefix: String,
    auth_ref: Option<String>,
    resolver: Arc<dyn CredentialResolver>,
    config: S3Config,
    client: reqwest::Client,
}

impl std::fmt::Debug for S3Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Sink")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("auth_ref", &self.auth_ref)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl S3Sink {
    fn new(
        bucket: String,
        prefix: String,
        auth_ref: Option<String>,
        resolver: Arc<dyn CredentialResolver>,
        config: S3Config,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the s3 sink's HTTP client")?;
        Ok(Self {
            bucket,
            prefix,
            auth_ref,
            resolver,
            config,
            client,
        })
    }
}

impl SinkTransport for S3Sink {
    fn ship<'a>(
        &'a self,
        rows: &'a [String],
        batch_start_revision: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Shipped>> + Send + 'a>> {
        Box::pin(async move {
            // Fail closed, before anything goes over the wire — same
            // reasoning as `WebhookSink::ship` and, before it,
            // `S3Source::fetch_with_auth`.
            let credential = auth::resolve_off_thread(&self.resolver, self.auth_ref.as_deref())
                .await
                .with_context(|| format!("resolving auth for s3 sink s3://{}", self.bucket))?;

            let body = ndjson(rows);
            let bytes = body.len();

            let key = object_key(&self.prefix, batch_start_revision);
            let endpoint = self
                .config
                .endpoint
                .clone()
                .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.config.region));
            let canonical_uri = format!(
                "/{}/{}",
                s3::percent_encode_segment(&self.bucket),
                s3::encode_key_path(&key)
            );
            let url = format!("{}{canonical_uri}", endpoint.trim_end_matches('/'));

            let mut request = self.client.put(&url);
            if let Some(credential) = &credential {
                let (access_key_id, secret_access_key) = s3::split_credential(credential.expose())?;
                let parsed = reqwest::Url::parse(&url).map_err(|e| {
                    anyhow::anyhow!(
                        "sink s3://{}/{key} produced an invalid url: {e}",
                        self.bucket
                    )
                })?;
                let host = s3::host_header(&parsed);
                // A PUT signs the *actual* body hash, never `UNSIGNED-PAYLOAD`
                // or the empty-body hash a GET uses — see `s3::sign`'s doc.
                let payload_hash = s3::sha256_hex(&body);
                let now = s3::amz_date(SystemTime::now());
                let signed = s3::sign(&s3::SigningRequest {
                    method: "PUT",
                    access_key_id,
                    secret_access_key,
                    region: &self.config.region,
                    host: &host,
                    canonical_uri: &canonical_uri,
                    payload_hash: &payload_hash,
                    amz_date: &now,
                });
                request = request
                    .header(reqwest::header::HOST, host)
                    .header("x-amz-date", signed.amz_date)
                    .header("x-amz-content-sha256", signed.content_sha256)
                    .header(reqwest::header::AUTHORIZATION, signed.authorization);
            }
            request = request.body(body);

            let response = request.send().await.map_err(|e| {
                anyhow::anyhow!("shipping audit batch to s3://{}/{key}: {e}", self.bucket)
            })?;

            let status = response.status();
            if !status.is_success() {
                // Body not read — same reasoning as `WebhookSink::ship`.
                anyhow::bail!(
                    "audit sink s3://{}/{key} returned HTTP {status}",
                    self.bucket
                );
            }

            Ok(Shipped {
                rows: rows.len(),
                bytes,
            })
        })
    }
}

/// `s3://<bucket>/<prefix>`'s authority split into `(bucket, prefix)`.
///
/// Unlike [`crate::sources::s3`]'s own `parse_s3_uri` (an object *source*,
/// which must name one concrete key), a sink's `<prefix>` may be empty —
/// `s3://bucket` ships every batch to the bucket root — so only a non-empty
/// bucket is required here.
fn parse_bucket_and_prefix(rest: &str) -> anyhow::Result<(String, String)> {
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        anyhow::bail!("names no bucket");
    }
    // A prefix is required, matching `control::validate`'s admission rule
    // exactly. It was optional here first, which meant `s3://bucket` was a URI
    // the fleet refused to accept and this factory would happily have built —
    // the same admission/egress split that already bit the `http://` case, and
    // it would have written objects to a key beginning `/`.
    if prefix.is_empty() {
        anyhow::bail!("names a bucket but no key prefix");
    }
    Ok((bucket.to_owned(), prefix.to_owned()))
}

/// The object key one batch's `PUT` targets: `<prefix>/<revision, zero-padded
/// to 20 digits>.jsonl`.
///
/// Zero-padded because the revision is the fleet's monotonically increasing
/// log index — padding every key to the same width makes lexicographic
/// listing order (what `ListObjectsV2` and every S3 console/CLI give you)
/// equal to revision order, with no need to parse keys back into numbers just
/// to sort them. 20 digits is `u64::MAX`'s width, so no revision this cluster
/// can ever reach overflows the field.
///
/// `prefix` is accepted with or without a trailing `/`: trimming it here,
/// once, is what keeps a configured `.../prefix/` from producing a `//` in
/// the key that a configured `.../prefix` (no slash) would not.
fn object_key(prefix: &str, batch_start_revision: u64) -> String {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{batch_start_revision:020}.jsonl")
    } else {
        format!("{prefix}/{batch_start_revision:020}.jsonl")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- admission and egress must agree -------------------------------------

    /// The regression this locks: `control::validate` accepted a loopback
    /// `http://` collector that `transport_for` then refused to build. The
    /// result was the worst shape available — a sink record that committed
    /// cleanly, looked configured on every node, and exported nothing.
    ///
    /// So the gate is not "loopback works"; it is that the **two** enforcement
    /// points answer identically for every URI. Add a scheme to one and this
    /// fails until you add it to the other.
    #[test]
    fn a_uri_admission_accepts_can_always_be_built_into_a_transport() {
        let resolver: Arc<dyn CredentialResolver> =
            Arc::new(crate::sources::auth::StandardResolver::new(None));
        let s3 = S3Config {
            endpoint: None,
            region: "us-east-1".to_owned(),
        };

        // Whitespace-padded variants are here because their absence is exactly
        // how the third admission/egress split survived: `validate` trims
        // before every check and `transport_for` did not, so a pasted
        // " https://…" was admitted fleet-wide and then failed to build on
        // every pass forever.
        for uri in [
            "https://collector.example/audit",
            "s3://bucket/audit-prefix",
            "http://127.0.0.1:9000/audit",
            "http://localhost:9000/audit",
            "http://[::1]:9000/audit",
            "ftp://collector.example/audit",
            "http://evil.example/audit",
            "s3://bucket",
            " https://collector.example/audit",
            "https://collector.example/audit ",
            "\ts3://bucket/audit-prefix\n",
            " http://127.0.0.1:9000/audit",
            " ftp://collector.example/audit",
        ] {
            let admitted = crate::control::validate(&crate::control::ControlOp::AuditSinkPut {
                tenant: crate::control::TenantId::new(crate::control::FLEET_SCOPE),
                uri: uri.to_owned(),
                auth_ref: None,
                batch_max_rows: crate::control::DEFAULT_AUDIT_BATCH_MAX_ROWS,
            })
            .is_ok();
            let buildable = transport_for(uri, None, &resolver, &s3).is_ok();
            assert_eq!(
                admitted, buildable,
                "{uri:?}: admission says {admitted}, transport says {buildable} — a sink the \
                 fleet accepts and no node can build exports nothing, silently"
            );
        }
    }

    // -- object key construction ---------------------------------------------

    #[test]
    fn object_key_zero_pads_the_revision_to_20_digits() {
        assert_eq!(object_key("audit", 42), "audit/00000000000000000042.jsonl");
    }

    #[test]
    fn object_key_zero_padding_preserves_lexicographic_order() {
        let low = object_key("audit", 9);
        let high = object_key("audit", 10);
        assert!(
            low < high,
            "zero-padded keys must sort the same as their revisions: {low:?} vs {high:?}"
        );
    }

    #[test]
    fn object_key_handles_a_prefix_with_a_trailing_slash() {
        assert_eq!(object_key("audit/", 1), "audit/00000000000000000001.jsonl");
    }

    #[test]
    fn object_key_handles_a_prefix_without_a_trailing_slash() {
        assert_eq!(object_key("audit", 1), "audit/00000000000000000001.jsonl");
    }

    #[test]
    fn object_key_handles_an_empty_prefix() {
        assert_eq!(object_key("", 1), "00000000000000000001.jsonl");
    }

    #[test]
    fn parse_bucket_and_prefix_splits_on_the_first_slash() {
        assert_eq!(
            parse_bucket_and_prefix("bucket/audit/logs").unwrap(),
            ("bucket".to_owned(), "audit/logs".to_owned())
        );
    }

    /// Required, not optional: `control::validate` refuses `s3://bucket`, so a
    /// transport that accepted it would be the admission/egress split this
    /// module's sibling test exists to prevent.
    #[test]
    fn parse_bucket_and_prefix_rejects_a_bucket_with_no_prefix() {
        assert!(parse_bucket_and_prefix("bucket").is_err());
    }

    #[test]
    fn parse_bucket_and_prefix_rejects_an_empty_bucket() {
        assert!(parse_bucket_and_prefix("/audit").is_err());
    }

    // -- JSON Lines framing ---------------------------------------------------

    #[test]
    fn ndjson_frames_n_rows_as_n_newline_separated_lines() {
        let rows = vec![
            r#"{"a":1}"#.to_owned(),
            r#"{"a":2}"#.to_owned(),
            r#"{"a":3}"#.to_owned(),
        ];
        let body = ndjson(&rows);
        let text = String::from_utf8(body).expect("utf8");
        // Every row is terminated by `\n`, including the last, so splitting
        // and dropping the trailing empty segment recovers exactly the rows.
        let lines: Vec<&str> = text.split('\n').collect();
        assert_eq!(lines.last(), Some(&""), "the body must end with `\\n`");
        let lines = &lines[..lines.len() - 1];
        assert_eq!(lines.len(), rows.len());
        assert_eq!(lines, &[r#"{"a":1}"#, r#"{"a":2}"#, r#"{"a":3}"#]);
    }

    #[test]
    fn ndjson_of_no_rows_is_an_empty_body() {
        assert_eq!(ndjson(&[]), Vec::<u8>::new());
    }

    // -- transport_for --------------------------------------------------------

    #[derive(Debug)]
    struct StubResolver;

    impl CredentialResolver for StubResolver {
        fn resolve(
            &self,
            auth_ref: &str,
        ) -> Result<crate::sources::auth::Credential, crate::sources::auth::AuthError> {
            Err(crate::sources::auth::AuthError::Unresolved(
                auth_ref.to_owned(),
                "STUB".to_owned(),
            ))
        }
    }

    #[test]
    fn transport_for_rejects_an_unsupported_scheme() {
        let resolver: Arc<dyn CredentialResolver> = Arc::new(StubResolver);
        let s3_config = S3Config {
            endpoint: None,
            region: "us-east-1".to_owned(),
        };
        let err = transport_for("ftp://example.com/audit", None, &resolver, &s3_config)
            .expect_err("ftp is not a supported sink scheme");
        assert!(err.to_string().contains("unsupported scheme"), "{err}");
    }

    #[test]
    fn transport_for_accepts_https_and_s3_schemes() {
        let resolver: Arc<dyn CredentialResolver> = Arc::new(StubResolver);
        let s3_config = S3Config {
            endpoint: None,
            region: "us-east-1".to_owned(),
        };
        assert!(transport_for("https://example.com/audit", None, &resolver, &s3_config).is_ok());
        assert!(transport_for("s3://bucket/prefix", None, &resolver, &s3_config).is_ok());
    }
}
