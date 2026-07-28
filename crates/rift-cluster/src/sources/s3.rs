//! `s3:` — imposters from an S3 (or S3-compatible) object (issue #136).
//!
//! ## URI shape
//!
//! `s3://<bucket>/<key>`. The request is path-style —
//! `{endpoint}/{bucket}/{key}` — against `config.endpoint` when set, or
//! `https://s3.{region}.amazonaws.com` otherwise. Path-style rather than
//! virtual-hosted-style (`{bucket}.s3.{region}.amazonaws.com`) is what makes a
//! configured `endpoint` (MinIO, a test stub, an in-VPC gateway) actually
//! reachable: those hosts rarely have per-bucket DNS behind them.
//!
//! ## Version
//!
//! `version` is the response's `ETag`, with the surrounding double quotes S3
//! wraps it in stripped. Left quoted, the same object would read as two
//! different versions across a `HeadObject`-style client and this one — which
//! would defeat #134's no-change short circuit for no reason.
//!
//! ## Auth
//!
//! A resolved credential's value is `<access-key-id>:<secret-access-key>` —
//! one string, colon-separated, because [`super::auth::Credential`] carries a
//! single opaque value and an S3 static credential is inherently a pair. The
//! request is signed with AWS Signature Version 4 (implemented directly here
//! with `hmac`+`sha2` — no AWS SDK: one GET, unsigned-payload, is a very small
//! slice of what an SDK signer supports, and pulling one in for it would be
//! exactly the over-engineering this build avoids elsewhere). The secret key
//! itself never reaches a header; only the derived signature and the key *id*
//! do.
//!
//! With no `auth_ref`, the request goes out unsigned — the path to a public
//! object. Ambient credentials (IRSA, an EC2/ECS task role) are **not**
//! implemented in this build: `auth_ref`-backed static keys are the supported
//! credentialed path, and anonymous is the only fallback. A named `auth_ref`
//! that fails to resolve is always an error, never a silent drop to that
//! anonymous path — see [`super::auth`]'s fail-closed rule.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use hmac::{Hmac, Mac};
use rift_ee::seams::{FetchedImposters, SourceMeta, SourceRef, parse_remote_document};
use sha2::{Digest, Sha256};

use super::CredentialedSource;
use super::auth::{self, CredentialResolver};
use super::common::{self, hex_encode};

/// Whole-request budget: connect, headers and body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Where to reach the bucket, and the SigV4 region it is signed for.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// `None` means the real AWS endpoint for `region`; `Some` overrides it —
    /// a MinIO/S3-compatible host, or a test stub.
    pub endpoint: Option<String>,
    pub region: String,
}

/// `s3:` imposter source.
pub struct S3Source {
    resolver: Arc<dyn CredentialResolver>,
    config: S3Config,
    client: reqwest::Client,
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl S3Source {
    /// # Errors
    /// If the underlying HTTP client cannot be built.
    pub fn new(resolver: Arc<dyn CredentialResolver>, config: S3Config) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the s3 source's HTTP client")?;
        Ok(Self {
            resolver,
            config,
            client,
        })
    }
}

impl CredentialedSource for S3Source {
    fn schemes(&self) -> &'static [&'static str] {
        &["s3"]
    }

    fn fetch_with_auth<'a>(
        &'a self,
        r: &'a SourceRef,
        auth_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>> {
        Box::pin(async move {
            // Fail closed, before anything goes over the wire: a named
            // credential that does not resolve must never fall through to an
            // unsigned request. Resolved off the async worker thread: the
            // resolver can do blocking file I/O under the secrets-directory
            // step.
            let credential = auth::resolve_off_thread(&self.resolver, auth_ref).await?;

            let (bucket, key) = parse_s3_uri(&r.uri)?;
            let endpoint = self
                .config
                .endpoint
                .clone()
                .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.config.region));
            // Percent-encoded segment by segment: a raw `#`/`?` in the key
            // would otherwise be read as a URL fragment/query delimiter by
            // both this request and `reqwest::Url::parse` below, silently
            // fetching a different (truncated) key with a legitimate-looking
            // ETag. `canonical_uri` is built from these exact same encoded
            // segments, never re-derived from a parsed `Url`, so the string
            // SigV4 signs is guaranteed byte-identical to what goes on the
            // wire.
            let canonical_uri = format!(
                "/{}/{}",
                percent_encode_segment(&bucket),
                encode_key_path(&key)
            );
            let url = format!("{}{canonical_uri}", endpoint.trim_end_matches('/'));

            let mut request = self.client.get(&url);
            if let Some(credential) = &credential {
                let (access_key_id, secret_access_key) = split_credential(credential.expose())?;
                let parsed = reqwest::Url::parse(&url).map_err(|e| {
                    anyhow::anyhow!("source uri {} produced an invalid url: {e}", r.uri)
                })?;
                let host = host_header(&parsed);
                let signed = sign_get(
                    access_key_id,
                    secret_access_key,
                    &self.config.region,
                    &host,
                    &canonical_uri,
                    amz_date(SystemTime::now()).as_str(),
                );
                request = request
                    .header(reqwest::header::HOST, host)
                    .header("x-amz-date", signed.amz_date)
                    .header("x-amz-content-sha256", signed.content_sha256)
                    .header(reqwest::header::AUTHORIZATION, signed.authorization);
            }

            let response = request
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("fetching s3 source {}: {e}", r.uri))?;

            let status = response.status();
            if !status.is_success() {
                // The body is never read on this path — an object store that
                // echoes request details in an error body must not be able to
                // put anything (a token, in principle) into our error string.
                anyhow::bail!("imposter source {} returned HTTP {status}", r.uri);
            }

            let version = response
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_owned());

            let body = common::read_capped(response, &format!("imposter source {}", r.uri)).await?;
            let loaded = parse_remote_document(&body, &r.uri)
                .map_err(|e| anyhow::anyhow!("imposter source {}: {e}", r.uri))?;

            Ok(FetchedImposters {
                configs: loaded.imposters,
                intercept: loaded.intercept,
                routes: loaded.routes,
                meta: SourceMeta {
                    version,
                    fetched_at: SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

fn parse_s3_uri(uri: &str) -> anyhow::Result<(String, String)> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("source uri {uri:?} is not an s3:// uri"))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("source uri {uri:?} names a bucket but no key"))?;
    if bucket.is_empty() || key.is_empty() {
        anyhow::bail!("source uri {uri:?} is not written `s3://<bucket>/<key>`");
    }
    Ok((bucket.to_owned(), key.to_owned()))
}

/// `<access-key-id>:<secret-access-key>`, the documented shape of an
/// `auth_ref`-resolved S3 credential.
fn split_credential(value: &str) -> anyhow::Result<(&str, &str)> {
    value.split_once(':').ok_or_else(|| {
        anyhow::anyhow!(
            "the resolved s3 credential is not `<access-key-id>:<secret-access-key>`: it has no \
             `:` separator"
        )
    })
}

/// Percent-encode one path *segment* per AWS SigV4's canonical-URI rule:
/// every byte outside the unreserved set (`A-Za-z0-9-_.~`) becomes `%XX`
/// uppercase hex.
///
/// Applied per segment — never to the whole key — because the `/` between
/// segments must stay a literal separator both on the wire and in the
/// canonical URI SigV4 signs; encoding it would change where the request (and
/// AWS) understands the path to split (RFC 3986 §2.1).
fn percent_encode_segment(segment: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// The key's path, percent-encoded segment-by-segment.
///
/// This is what the request URL and the SigV4 canonical URI must *both* show:
/// a raw `#`, `?`, space or non-ASCII byte in the key would otherwise be read
/// as a URL fragment/query delimiter (silently fetching a different,
/// truncated object with a legitimate-looking ETag) or desync the signature
/// from what actually reached the wire (`?` puts an unsigned query on the
/// request, which S3 rejects with a 403 that names nothing about the cause).
fn encode_key_path(key: &str) -> String {
    key.split('/')
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// The `Host` header value a request to `url` will actually carry — SigV4
/// signs this exact string, so it must match what the wire sends, port and
/// all.
fn host_header(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

/// The headers a signed `GET` needs.
struct SignedGet {
    authorization: String,
    amz_date: String,
    content_sha256: String,
}

/// Sign an unsigned-payload `GET` with SigV4.
///
/// `amz_date` is a parameter rather than read from the clock in here, so the
/// pure signing arithmetic below is testable without mocking time.
fn sign_get(
    access_key_id: &str,
    secret_access_key: &str,
    region: &str,
    host: &str,
    canonical_uri: &str,
    amz_date: &str,
) -> SignedGet {
    let date_stamp = &amz_date[..8];
    let payload_hash = sha256_hex(b"");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    // Canonical query string is empty: this provider never signs a query,
    // only headers.
    let canonical_request =
        format!("GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let hashed_canonical_request = sha256_hex(canonical_request.as_bytes());

    let credential_scope = format!("{date_stamp}/{region}/s3/aws4_request");
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}");

    let key = signing_key(secret_access_key, date_stamp, region, "s3");
    let signature = hex_encode(&hmac_sha256(&key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{credential_scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    SignedGet {
        authorization,
        amz_date: amz_date.to_owned(),
        content_sha256: payload_hash,
    }
}

/// The SigV4 signing-key derivation: `AWS4<secret>` → date → region → service
/// → `aws4_request`, each step an HMAC-SHA256 keyed by the previous result.
fn signing_key(secret_access_key: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(
        format!("AWS4{secret_access_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    // An HMAC key may be any length (RFC 2104 pads/hashes as needed), so this
    // cannot fail on the keys this module ever constructs.
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(bytes))
}

/// A Unix timestamp as an AWS SigV4 timestamp, `YYYYMMDD'T'HHMMSS'Z'`.
///
/// No date/time crate is a dependency of this crate, and the one call site
/// here is once per signed fetch, so the (well-known, public-domain) civil
/// calendar arithmetic is implemented directly rather than adding a dependency
/// for it. `civil_from_days` is Howard Hinnant's algorithm
/// (<https://howardhinnant.github.io/date_algorithms.html>), reused unchanged.
fn amz_date(now: SystemTime) -> String {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let days = secs.div_euclid(86400);
    let time_of_day = secs.rem_euclid(86400);
    let (year, month, day) = civil_from_days(days);
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Days since the Unix epoch → a proleptic Gregorian `(year, month, day)`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression lock, not an AWS interop proof: these are fixed inputs run
    /// through our own signing-key derivation and canonical-request hashing,
    /// with the output pinned so a refactor of either step is caught even
    /// though the exact value has not been checked against a published AWS
    /// worked example.
    #[test]
    fn signing_key_and_canonical_hash_are_stable_for_fixed_inputs() {
        let key = signing_key("wJalrXUtnFEMIsupersecret", "20240101", "us-east-1", "s3");
        assert_eq!(key.len(), 32, "HMAC-SHA256 output is always 32 bytes");
        assert_eq!(
            hex_encode(&key),
            "2f89049cd2266813462ff397e8bdff6d1603c26f1ed8e4d7edfcb556fced5d46",
            "the signing key derivation must not silently change shape"
        );

        let canonical_request = "GET\n/bucket/key\n\nhost:example.com\n\
             x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
             x-amz-date:20240101T000000Z\n\nhost;x-amz-content-sha256;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(
            sha256_hex(canonical_request.as_bytes()),
            "e98362dd95c4322fd59b78d40570d1cb640332d455bbc4af4fbc23d4f479f49c",
            "canonical-request hashing must not silently change shape"
        );
    }

    #[test]
    fn empty_payload_hash_is_the_well_known_sha256_of_nothing() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn amz_date_formats_a_known_instant() {
        // 2024-01-01T00:00:00Z, a round number chosen for readability.
        let instant = SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_067_200);
        assert_eq!(amz_date(instant), "20240101T000000Z");
    }

    #[test]
    fn split_credential_requires_the_colon_separator() {
        assert_eq!(
            split_credential("AKIAEXAMPLE:secret").unwrap(),
            ("AKIAEXAMPLE", "secret")
        );
        assert!(split_credential("no-colon-here").is_err());
    }

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        assert_eq!(
            parse_s3_uri("s3://bucket/mocks/imposters.json").unwrap(),
            ("bucket".to_owned(), "mocks/imposters.json".to_owned())
        );
        assert!(parse_s3_uri("s3://bucket").is_err());
        assert!(parse_s3_uri("s3:///key").is_err());
    }

    // -- issue #136 review, B3: a key containing `#`/`?`/space/unicode ------

    /// The unreserved set passes through unchanged; a `/` inside one segment
    /// (there should never be one, since callers split on `/` first) would
    /// still be encoded, which is exactly why encoding happens per segment
    /// rather than on the whole key.
    #[test]
    fn percent_encode_segment_leaves_unreserved_bytes_alone() {
        assert_eq!(percent_encode_segment("abcXYZ019-_.~"), "abcXYZ019-_.~");
    }

    /// A raw `#` or `?` in a key must never survive into the request target
    /// as a literal fragment/query delimiter — that is what let
    /// `s3://bucket/a#b` silently GET `/bucket/a` instead of the object the
    /// operator named.
    #[test]
    fn percent_encode_segment_escapes_url_delimiters() {
        assert_eq!(percent_encode_segment("a#b"), "a%23b");
        assert_eq!(percent_encode_segment("a?b"), "a%3Fb");
        assert_eq!(percent_encode_segment("a b"), "a%20b");
    }

    /// Non-ASCII bytes are percent-encoded byte-for-byte over their UTF-8
    /// representation, uppercase hex, matching AWS's canonical-URI rule.
    #[test]
    fn percent_encode_segment_escapes_non_ascii_utf8_bytes() {
        assert_eq!(percent_encode_segment("héllo"), "h%C3%A9llo");
    }

    /// `/` between segments is the one character `encode_key_path` must
    /// *not* touch: it is the literal separator both the wire request and
    /// SigV4's canonical URI need to agree on.
    #[test]
    fn encode_key_path_preserves_the_segment_separator() {
        assert_eq!(encode_key_path("mocks/a#b.json"), "mocks/a%23b.json");
        assert_eq!(encode_key_path("a/b/c"), "a/b/c");
    }

    /// The documented rule: split on the *first* `:` only, so a secret access
    /// key that itself happens to contain a `:` is not truncated.
    #[test]
    fn split_credential_splits_on_the_first_colon_only() {
        assert_eq!(
            split_credential("AKIAEXAMPLE:sec:ret:with:colons").unwrap(),
            ("AKIAEXAMPLE", "sec:ret:with:colons")
        );
    }
}
