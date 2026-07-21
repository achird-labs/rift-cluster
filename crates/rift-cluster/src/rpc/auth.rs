//! Keyed authentication for cluster-internal RPC (RFC-001 §11.2).
//!
//! Every internal request carries
//! `X-Rift-Cluster-Auth: t=<unix_secs>,n=<nonce>,mac=<hex>` where the MAC is
//! HMAC-SHA256 over the request's timestamp, nonce, method, path and body. This
//! authenticates and integrity-protects peer traffic; it does not encrypt it —
//! confidentiality is delegated to network isolation.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use rand::Rng;
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the credential.
pub const AUTH_HEADER: &str = "x-rift-cluster-auth";

/// Accepted clock skew either side of the receiver's clock.
pub const DEFAULT_SKEW: Duration = Duration::from_secs(30);

/// Nonce-cache bound. Sized for `expected_peak_rpc_rate × replay window`.
pub const DEFAULT_NONCE_CAPACITY: usize = 100_000;

/// Why a credential was refused. Each variant is a distinct, separately
/// testable outcome — callers and metrics distinguish a forged MAC from a
/// clock problem from a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// The header was absent or did not parse as `t=…,n=…,mac=…`.
    #[error("malformed cluster auth header")]
    Malformed,
    /// The MAC did not match the request.
    #[error("cluster auth MAC mismatch")]
    BadMac,
    /// The timestamp fell outside the accepted skew window.
    #[error("cluster auth timestamp outside skew window")]
    StaleTimestamp,
    /// This nonce was already accepted inside the replay window.
    #[error("cluster auth nonce replayed")]
    ReplayedNonce,
    /// The nonce cache is full of live entries, so replay cannot be ruled out.
    #[error("cluster auth nonce cache full")]
    NonceCacheFull,
}

impl AuthError {
    /// Stable label for metrics and logs.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::BadMac => "bad_mac",
            Self::StaleTimestamp => "stale_timestamp",
            Self::ReplayedNonce => "replayed_nonce",
            Self::NonceCacheFull => "nonce_cache_full",
        }
    }
}

/// The parts of a request that are covered by the MAC.
#[derive(Debug, Clone, Copy)]
pub struct SignedRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub body: &'a [u8],
}

/// Build the exact bytes the MAC covers.
///
/// Every field is length-prefixed rather than concatenated: with plain
/// concatenation a field boundary can shift without changing the signed bytes
/// (`t=1,n=23` signs identically to `t=12,n=3`), which would let a peer present
/// a different request under a captured MAC.
fn canonical(ts: u64, nonce: &str, req: SignedRequest<'_>) -> Vec<u8> {
    let ts = ts.to_string();
    let fields: [&[u8]; 5] = [
        ts.as_bytes(),
        nonce.as_bytes(),
        req.method.as_bytes(),
        req.path.as_bytes(),
        req.body,
    ];
    let mut buf = Vec::with_capacity(fields.iter().map(|f| f.len() + 8).sum());
    for field in fields {
        buf.extend_from_slice(&(field.len() as u64).to_le_bytes());
        buf.extend_from_slice(field);
    }
    buf
}

fn mac(key: &[u8], ts: u64, nonce: &str, req: SignedRequest<'_>) -> Vec<u8> {
    // HMAC accepts keys of any length, so this cannot fail for a real secret.
    let mut hmac =
        <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any length");
    hmac.update(&canonical(ts, nonce, req));
    hmac.finalize().into_bytes().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Signs outgoing requests with the cluster secret.
#[derive(Clone)]
pub struct Signer {
    key: Vec<u8>,
}

impl Signer {
    #[must_use]
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            key: secret.as_ref().to_vec(),
        }
    }

    /// Produce the `X-Rift-Cluster-Auth` header value for a request.
    #[must_use]
    pub fn header(&self, req: SignedRequest<'_>) -> String {
        self.header_at(unix_now(), &new_nonce(), req)
    }

    /// Deterministic variant used by tests and by callers that supply their own
    /// clock/nonce source.
    #[must_use]
    pub fn header_at(&self, ts: u64, nonce: &str, req: SignedRequest<'_>) -> String {
        let mac = hex_encode(&mac(&self.key, ts, nonce, req));
        format!("t={ts},n={nonce},mac={mac}")
    }
}

fn new_nonce() -> String {
    let bytes: [u8; 16] = rand::thread_rng().r#gen();
    hex_encode(&bytes)
}

struct Credential<'a> {
    ts: u64,
    nonce: &'a str,
    mac: Vec<u8>,
}

fn parse(header: &str) -> Option<Credential<'_>> {
    let (mut ts, mut nonce, mut mac) = (None, None, None);
    for part in header.split(',') {
        let (key, value) = part.split_once('=')?;
        match key.trim() {
            "t" => ts = value.trim().parse::<u64>().ok(),
            "n" => nonce = Some(value.trim()),
            "mac" => mac = hex_decode(value.trim()),
            _ => return None,
        }
    }
    let nonce = nonce?;
    if nonce.is_empty() {
        return None;
    }
    Some(Credential {
        ts: ts?,
        nonce,
        mac: mac?,
    })
}

/// Bounded replay guard. Entries expire once they fall outside the skew window,
/// because the timestamp check alone rejects anything older.
struct NonceCache {
    capacity: usize,
    ttl_secs: u64,
    seen: Mutex<HashMap<String, u64>>,
}

impl NonceCache {
    fn new(capacity: usize, ttl_secs: u64) -> Self {
        Self {
            capacity,
            ttl_secs,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// `signed_at` is the credential's own timestamp, not the receiver's clock:
    /// an entry may only be dropped once the timestamp check would reject a
    /// replay of it, and that check is against the *signed* time. Keying on
    /// arrival instead would expire a maximally-future-skewed credential one
    /// second before it stopped being replayable.
    fn admit(&self, nonce: &str, signed_at: u64, now: u64) -> Result<(), AuthError> {
        let mut seen = self.seen.lock();
        if seen.contains_key(nonce) {
            return Err(AuthError::ReplayedNonce);
        }
        if seen.len() >= self.capacity {
            seen.retain(|_, signed| now.saturating_sub(*signed) < self.ttl_secs);
            if seen.len() >= self.capacity {
                // Fail closed: with no room to record this nonce we cannot
                // promise it is not a replay, and a mock server that silently
                // stops checking replays is worse than one that refuses.
                return Err(AuthError::NonceCacheFull);
            }
        }
        seen.insert(nonce.to_owned(), signed_at);
        Ok(())
    }
}

/// Verifies incoming requests against the cluster secret.
pub struct Verifier {
    key: Vec<u8>,
    skew_secs: u64,
    nonces: NonceCache,
}

impl Verifier {
    #[must_use]
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self::with_limits(secret, DEFAULT_SKEW, DEFAULT_NONCE_CAPACITY)
    }

    #[must_use]
    pub fn with_limits(secret: impl AsRef<[u8]>, skew: Duration, nonce_capacity: usize) -> Self {
        let skew_secs = skew.as_secs();
        Self {
            key: secret.as_ref().to_vec(),
            skew_secs,
            // A credential is replayable for at most the width of the skew
            // window either side of its own timestamp, so an entry stops
            // mattering strictly after `2 × skew`; the `+ 1` keeps the eviction
            // boundary outside the last second the timestamp check still admits.
            nonces: NonceCache::new(
                nonce_capacity,
                skew_secs.saturating_mul(2).saturating_add(1),
            ),
        }
    }

    /// Verify a credential against the request it claims to cover.
    pub fn verify(&self, header: Option<&str>, req: SignedRequest<'_>) -> Result<(), AuthError> {
        self.verify_at(unix_now(), header, req)
    }

    /// Deterministic variant: `now` is the receiver's clock in unix seconds.
    pub fn verify_at(
        &self,
        now: u64,
        header: Option<&str>,
        req: SignedRequest<'_>,
    ) -> Result<(), AuthError> {
        let header = header.ok_or(AuthError::Malformed)?;
        let cred = parse(header).ok_or(AuthError::Malformed)?;

        if now.abs_diff(cred.ts) > self.skew_secs {
            return Err(AuthError::StaleTimestamp);
        }

        let expected = mac(&self.key, cred.ts, cred.nonce, req);
        if expected.ct_eq(&cred.mac).unwrap_u8() != 1 {
            return Err(AuthError::BadMac);
        }

        // Nonce admission runs last on purpose: it is the only step that
        // consumes a bounded resource, so an unauthenticated peer must not be
        // able to reach it and fill the cache into fail-closed rejection.
        self.nonces.admit(cred.nonce, cred.ts, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "correct horse battery staple";
    const NOW: u64 = 1_800_000_000;

    fn req<'a>(body: &'a [u8]) -> SignedRequest<'a> {
        SignedRequest {
            method: "POST",
            path: "/internal/v1/config/write",
            body,
        }
    }

    fn verifier() -> Verifier {
        Verifier::with_limits(SECRET, DEFAULT_SKEW, 8)
    }

    #[test]
    fn auth_valid_request_verifies() {
        let header = Signer::new(SECRET).header_at(NOW, "n1", req(b"{}"));
        assert_eq!(verifier().verify_at(NOW, Some(&header), req(b"{}")), Ok(()));
    }

    #[test]
    fn auth_rejects_bad_mac() {
        let header = Signer::new("a different secret").header_at(NOW, "n1", req(b"{}"));
        assert_eq!(
            verifier().verify_at(NOW, Some(&header), req(b"{}")),
            Err(AuthError::BadMac)
        );
    }

    #[test]
    fn auth_rejects_tampered_body() {
        let header = Signer::new(SECRET).header_at(NOW, "n1", req(b"{}"));
        assert_eq!(
            verifier().verify_at(NOW, Some(&header), req(br#"{"evil":true}"#)),
            Err(AuthError::BadMac)
        );
    }

    #[test]
    fn auth_rejects_tampered_path() {
        let signer = Signer::new(SECRET);
        let header = signer.header_at(NOW, "n1", req(b"{}"));
        let elsewhere = SignedRequest {
            method: "POST",
            path: "/internal/v1/config/delete",
            body: b"{}",
        };
        assert_eq!(
            verifier().verify_at(NOW, Some(&header), elsewhere),
            Err(AuthError::BadMac)
        );
    }

    #[test]
    fn auth_rejects_stale_timestamp() {
        let signer = Signer::new(SECRET);
        let old = signer.header_at(NOW - 31, "n1", req(b"{}"));
        assert_eq!(
            verifier().verify_at(NOW, Some(&old), req(b"{}")),
            Err(AuthError::StaleTimestamp)
        );
        let future = signer.header_at(NOW + 31, "n2", req(b"{}"));
        assert_eq!(
            verifier().verify_at(NOW, Some(&future), req(b"{}")),
            Err(AuthError::StaleTimestamp)
        );
    }

    #[test]
    fn auth_accepts_within_skew_either_side() {
        let signer = Signer::new(SECRET);
        let v = verifier();
        let behind = signer.header_at(NOW - 30, "n1", req(b"{}"));
        assert_eq!(v.verify_at(NOW, Some(&behind), req(b"{}")), Ok(()));
        let ahead = signer.header_at(NOW + 30, "n2", req(b"{}"));
        assert_eq!(v.verify_at(NOW, Some(&ahead), req(b"{}")), Ok(()));
    }

    #[test]
    fn auth_rejects_replayed_nonce() {
        let v = verifier();
        let header = Signer::new(SECRET).header_at(NOW, "n1", req(b"{}"));
        assert_eq!(v.verify_at(NOW, Some(&header), req(b"{}")), Ok(()));
        assert_eq!(
            v.verify_at(NOW, Some(&header), req(b"{}")),
            Err(AuthError::ReplayedNonce)
        );
    }

    #[test]
    fn auth_nonce_cache_overflow_fails_closed() {
        // Capacity 8, all entries live (same instant) => the 9th cannot be
        // recorded, so it must be refused rather than admitted unchecked.
        let v = verifier();
        let signer = Signer::new(SECRET);
        for i in 0..8 {
            let nonce = format!("n{i}");
            let header = signer.header_at(NOW, &nonce, req(b"{}"));
            assert_eq!(
                v.verify_at(NOW, Some(&header), req(b"{}")),
                Ok(()),
                "nonce {i}"
            );
        }
        let header = signer.header_at(NOW, "overflow", req(b"{}"));
        assert_eq!(
            v.verify_at(NOW, Some(&header), req(b"{}")),
            Err(AuthError::NonceCacheFull)
        );
    }

    #[test]
    fn auth_nonce_cache_does_not_expire_a_still_replayable_credential() {
        // A credential signed at the maximum future skew stays replayable
        // until `now = ts + skew`. Evicting it any earlier would let the
        // replay through in the window between eviction and expiry.
        let v = verifier();
        let signer = Signer::new(SECRET);
        let signed_at = NOW + 30;
        let header = signer.header_at(signed_at, "edge", req(b"{}"));
        assert_eq!(v.verify_at(NOW, Some(&header), req(b"{}")), Ok(()));

        // Fill to capacity so the next admission forces an eviction sweep.
        for i in 0..7 {
            let nonce = format!("filler{i}");
            let filler = signer.header_at(signed_at, &nonce, req(b"{}"));
            assert_eq!(v.verify_at(signed_at, Some(&filler), req(b"{}")), Ok(()));
        }

        // At the last instant the timestamp check still admits this credential,
        // the replay must be caught by the nonce cache rather than admitted.
        let last_admissible = signed_at + 30;
        assert_eq!(
            v.verify_at(last_admissible, Some(&header), req(b"{}")),
            Err(AuthError::ReplayedNonce)
        );
    }

    #[test]
    fn auth_nonce_cache_reclaims_expired_entries() {
        let v = verifier();
        let signer = Signer::new(SECRET);
        for i in 0..8 {
            let nonce = format!("n{i}");
            let header = signer.header_at(NOW, &nonce, req(b"{}"));
            assert_eq!(v.verify_at(NOW, Some(&header), req(b"{}")), Ok(()));
        }
        // Two skew windows later every recorded nonce is outside the replay
        // window, so the cache reclaims and the request is admitted.
        let later = NOW + 61;
        let header = signer.header_at(later, "fresh", req(b"{}"));
        assert_eq!(v.verify_at(later, Some(&header), req(b"{}")), Ok(()));
    }

    #[test]
    fn auth_rejects_missing_and_malformed_headers() {
        let v = verifier();
        assert_eq!(
            v.verify_at(NOW, None, req(b"{}")),
            Err(AuthError::Malformed)
        );
        for bad in [
            "",
            "garbage",
            "t=abc,n=n1,mac=00",
            "t=1,n=,mac=00",
            "t=1,n=n1,mac=zz",
            "t=1,n=n1",
        ] {
            assert_eq!(
                v.verify_at(NOW, Some(bad), req(b"{}")),
                Err(AuthError::Malformed),
                "header {bad:?}"
            );
        }
    }

    #[test]
    fn canonical_form_is_unambiguous_across_field_boundaries() {
        // ("1", "23") and ("12", "3") would collide under plain concatenation.
        let a = canonical(1, "23", req(b""));
        let b = canonical(12, "3", req(b""));
        assert_ne!(a, b);
    }

    #[test]
    fn generated_nonces_do_not_repeat() {
        let a = new_nonce();
        let b = new_nonce();
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
    }
}
