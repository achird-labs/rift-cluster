//! Session-cookie token minting and verification (RFC-006 §5.3, issue #185).
//!
//! Pure logic only — no I/O, no state-machine reads, no [`RaftNode`](rift_cluster::RaftNode).
//! [`crate::admin_front`] owns everything this module does not: parsing the `Cookie` header,
//! rendering `Set-Cookie`, submitting the [`rift_cluster::control::ControlOp::SessionKeyPut`]
//! that mints or rotates the signing key, and the CSRF gate. This module answers exactly one
//! question — is this token a legitimate, unexpired session minted under the fleet's *current*
//! signing-key revision, and if so, whose is it — and nothing else.
//!
//! # What `verify` deliberately does not carry
//!
//! A session proves **authentication only**, and since #550 there is exactly one identity to
//! authenticate as: the holder of the fleet's `--api-key`. [`verify`] therefore answers
//! `Result<(), _>` — there is nothing for it to resolve *to*. It is deliberately not widened
//! back into an identity channel: a cookie that carried authorization data would be a second
//! source of truth for a decision the key already settles.
//!
//! # Token format
//!
//! `v1.<base64url_nopad(payload_json)>.<base64url_nopad(hmac_sha256)>`
//!
//! where the payload is `{"pid": "admin", "iat": <unix secs>, "exp": <unix secs>,
//! "kr": <key revision>}`. `pid` is the fixed subject [`SUBJECT`] — the one administrator a
//! keyed fleet has — kept in the signed span rather than dropped so the format stays readable
//! in a log dump and a future second subject is a value change, not a format change. The HMAC
//! signs the ASCII bytes of `v1.<payload_b64>` — the version tag is inside the signed span, not
//! just a prefix on the wire, so a future `v2` format cannot be replayed as if it were `v1` by
//! an attacker who only controls the tag.
//!
//! `kr` (key revision) is what makes rotation a fleet-wide kill switch with no table to sweep:
//! every node verifies against its own applied [`SessionKey`], and a token minted under
//! revision N fails [`verify`] the instant revision N+1 is committed — on every replica,
//! simultaneously, with no per-session bookkeeping anywhere.

use hmac::{Hmac, Mac};
use rift_cluster::control::SessionKey;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// The token format tag. Part of the signed span (see the module doc) rather than a bare
/// prefix, so it cannot be swapped without invalidating the signature.
const FORMAT_TAG: &str = "v1";

/// The one subject a session token can name (#550, D-73). A keyed fleet has a single
/// administrator — whoever holds `--api-key` — so the payload's `pid` is a constant, not an
/// identity the token resolves. [`verify`] refuses any other value as [`SessionError::Malformed`]
/// rather than ignoring the field: a token naming a subject this build does not know is a token
/// from a format this build does not understand.
pub(crate) const SUBJECT: &str = "admin";

/// 8 hours: a work-day session (RFC-006 §5.3). `admin_front` mints with this TTL and renders
/// the identical value as the `Set-Cookie: Max-Age` — one constant, so the two can never drift
/// apart and hand a client a cookie that outlives (or undershoots) the token inside it.
pub(crate) const SESSION_TTL_SECS: u64 = 8 * 60 * 60;

/// Why [`verify`] refused a token.
///
/// Every variant renders to the client as a bare `401` (see `admin_front`'s cookie-auth
/// branch) — never surfaced on the wire. Telling a caller holding a forged cookie *which*
/// part of the forgery was wrong ("bad signature" vs "expired" vs "wrong revision") is a free
/// oracle for what to fix next; the distinction exists for logs and tests, not clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SessionError {
    #[error("session token is malformed")]
    Malformed,
    #[error("session token signature does not verify")]
    BadSignature,
    #[error("session token has expired")]
    Expired,
    #[error("session token was minted under a since-rotated signing key")]
    WrongRevision,
}

/// The claims signed inside a session token. Field names are the wire spelling — short, because
/// they ride in every request's `Cookie` header.
#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    pid: String,
    iat: u64,
    exp: u64,
    kr: u64,
}

/// Mint a session token for `principal_id`, signed under `key`, valid from `now_secs` for
/// `ttl_secs`.
///
/// Mint always produces a token, so it returns a bare `String` rather than a `Result`. Two
/// steps look fallible but are handled without panicking:
///
/// - `Payload` is plain strings and integers, so its JSON encoding cannot realistically fail;
///   the (unreachable) error case falls back to an empty payload rather than an `.unwrap()`, so
///   a change elsewhere that somehow made this fallible would produce a token nothing can ever
///   [`verify`] instead of panicking mid-login.
/// - `key.key` should always be exactly [`rift_cluster::control::SESSION_KEY_BYTES`] of hex
///   ([`rift_cluster::control::validate`] refuses a [`rift_cluster::control::ControlOp::SessionKeyPut`]
///   that is not, before it is ever committed) but this function does not re-trust that from
///   afar: a decode failure falls back to the raw string's bytes as key material instead of
///   panicking. Either way the fallback is safe because [`verify`] independently re-decodes the
///   *same* `key.key` and refuses outright — as [`SessionError::Malformed`] — the moment it is
///   not valid hex, so a token minted against a bad key can never verify regardless of what
///   bytes this fallback happened to sign with.
///
/// The one step that is genuinely unavoidable is HMAC-SHA256 key construction, which the `hmac`
/// crate's `KeyInit::new_from_slice` exposes as fallible for API-uniformity with algorithms that
/// do have fixed key sizes — HMAC itself accepts a key of *any* length (its own construction
/// hashes an over-long key and pads a short one), so this `expect` documents a proof rather than
/// hoping: see `hmac` 0.12.1's `HmacCore::new_from_slice` (`optim.rs`), which has no `Err` path
/// that mint's fixed-length key input can reach.
#[must_use]
pub(crate) fn mint(key: &SessionKey, now_secs: u64, ttl_secs: u64) -> String {
    let payload = Payload {
        pid: SUBJECT.to_owned(),
        iat: now_secs,
        exp: now_secs.saturating_add(ttl_secs),
        kr: key.revision,
    };
    let payload_json = serde_json::to_vec(&payload).unwrap_or_default();
    let payload_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        payload_json,
    );
    let signed_span = format!("{FORMAT_TAG}.{payload_b64}");

    let key_bytes = hex_decode(&key.key).unwrap_or_else(|| key.key.as_bytes().to_vec());
    let mut mac = HmacSha256::new_from_slice(&key_bytes)
        .expect("HMAC-SHA256 accepts a key of any length — see this function's doc");
    mac.update(signed_span.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, sig);

    format!("{signed_span}.{sig_b64}")
}

/// Verify `token` against `key` as of `now_secs`. `Ok(())` carries nothing — see the module
/// doc for why a session resolves to no identity.
///
/// Checks, in order: well-formed three-part token → signature (constant-time; see below) →
/// the payload decodes → its subject is [`SUBJECT`] → the key revision the token was minted
/// under still matches `key`'s → not expired. Signature verification runs before the payload is
/// ever parsed as JSON: nothing downstream trusts a byte of the claims until the MAC over them
/// has already been accepted.
pub(crate) fn verify(key: &SessionKey, token: &str, now_secs: u64) -> Result<(), SessionError> {
    let mut parts = token.split('.');
    let (Some(tag), Some(payload_b64), Some(sig_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SessionError::Malformed);
    };
    if tag != FORMAT_TAG {
        return Err(SessionError::Malformed);
    }

    let key_bytes = hex_decode(&key.key).ok_or(SessionError::Malformed)?;
    // `new_from_slice` is fallible only for algorithms with a fixed key size; HMAC has none, so
    // this never actually returns `Err` (see `mint`'s doc for the proof), but `verify` already
    // returns a `Result`, so there is no reason to `expect` here rather than propagate.
    let mut mac = HmacSha256::new_from_slice(&key_bytes).map_err(|_| SessionError::BadSignature)?;
    mac.update(format!("{tag}.{payload_b64}").as_bytes());
    let expected = mac.finalize().into_bytes();

    let provided =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, sig_b64)
            .map_err(|_| SessionError::Malformed)?;

    // Constant-time by construction, never `==`: a length or byte-position leak here is a
    // forgery oracle against a MAC the same way it would be against a password hash. `ct_eq`
    // compares every byte regardless of where the first mismatch falls (a short-circuiting `==`
    // on a `Vec`/slice does not, and neither does bailing out early on a length mismatch before
    // comparing bytes at all — so the length check itself is allowed to be non-constant-time,
    // only the *byte* comparison of a same-length MAC must not branch on content).
    let signatures_match = expected.len() == provided.len()
        && bool::from(expected.as_slice().ct_eq(provided.as_slice()));
    if !signatures_match {
        return Err(SessionError::BadSignature);
    }

    let payload_json = base64::Engine::decode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        payload_b64,
    )
    .map_err(|_| SessionError::Malformed)?;
    let payload: Payload =
        serde_json::from_slice(&payload_json).map_err(|_| SessionError::Malformed)?;

    if payload.pid != SUBJECT {
        return Err(SessionError::Malformed);
    }
    // Rotation invalidates every outstanding session at once, with no table to sweep: a token
    // minted under a since-superseded revision is refused here, unconditionally, the instant
    // any node applies the next `SessionKeyPut`.
    if payload.kr != key.revision {
        return Err(SessionError::WrongRevision);
    }
    if now_secs > payload.exp {
        return Err(SessionError::Expired);
    }

    Ok(())
}

/// Lowercase hex, no separators. Hand-rolled for the same reason
/// `rift_cluster::control`'s private decoder is: a dependency for one 32-byte key is not worth
/// carrying, and this module already needs the decode half for [`verify`].
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The inverse of [`hex_encode`]. `None` for anything that is not clean lowercase-or-uppercase
/// hex of even length — never partially decoded.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    // Explicit alphabet check: `from_str_radix` accepts a leading sign, so `"+0"` would decode as a
    // zero byte. Kept identical to `control::hex_decode`'s guard — these two must agree, or a key
    // admitted by the state machine could fail to decode here and silently sign with the fallback.
    if !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key(revision: u64) -> SessionKey {
        SessionKey {
            key: hex_encode(&[0x42; 32]),
            revision,
        }
    }

    #[test]
    fn round_trip_mints_and_verifies() {
        let key = test_key(1);
        let token = mint(&key, 1_000, SESSION_TTL_SECS);
        assert_eq!(verify(&key, &token, 1_000), Ok(()));
    }

    #[test]
    fn expired_token_is_refused() {
        let key = test_key(1);
        let token = mint(&key, 1_000, 10);
        // One second past `exp` (1_010).
        assert_eq!(verify(&key, &token, 1_011), Err(SessionError::Expired));
        // Exactly at `exp` still verifies — `exp` is inclusive.
        assert!(verify(&key, &token, 1_010).is_ok());
    }

    #[test]
    fn rotating_the_key_invalidates_every_outstanding_session() {
        let old_key = test_key(1);
        let token = mint(&old_key, 1_000, SESSION_TTL_SECS);
        // Same key bytes, but the revision moved on (a rotation committed) — the token must stop
        // verifying immediately, with no table of outstanding sessions swept to make that true.
        let rotated = SessionKey {
            key: old_key.key.clone(),
            revision: 2,
        };
        assert_eq!(
            verify(&rotated, &token, 1_000),
            Err(SessionError::WrongRevision)
        );
    }

    #[test]
    fn tampered_payload_fails_the_signature_check() {
        let key = test_key(1);
        let token = mint(&key, 1_000, SESSION_TTL_SECS);
        let mut parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        // Forge a payload claiming a longer session, re-encoded the same way the real one was,
        // but signed by nobody who holds the key.
        let forged = Payload {
            pid: SUBJECT.to_owned(),
            iat: 1_000,
            exp: 1_000 + SESSION_TTL_SECS * 100,
            kr: 1,
        };
        let forged_json = serde_json::to_vec(&forged).expect("payload serializes");
        let forged_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            forged_json,
        );
        parts[1] = &forged_b64;
        let forged_token = parts.join(".");
        assert_eq!(
            verify(&key, &forged_token, 1_000),
            Err(SessionError::BadSignature)
        );
    }

    #[test]
    fn tampered_signature_is_rejected() {
        let key = test_key(1);
        let token = mint(&key, 1_000, SESSION_TTL_SECS);
        let mut parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        // Flip the signature to some other well-formed-but-wrong base64url value.
        let flipped = if parts[2].starts_with('A') { "B" } else { "A" };
        let doctored = format!("{}{}", flipped, &parts[2][1..]);
        parts[2] = &doctored;
        let doctored_token = parts.join(".");
        assert_eq!(
            verify(&key, &doctored_token, 1_000),
            Err(SessionError::BadSignature)
        );
    }

    #[test]
    fn malformed_token_is_refused() {
        let key = test_key(1);
        for garbage in ["", "not-a-token", "v1.onlyonepart", "v2.a.b", "v1.a.b.c"] {
            assert_eq!(
                verify(&key, garbage, 1_000),
                Err(SessionError::Malformed),
                "token {garbage:?} must be refused"
            );
        }
    }

    /// A token whose subject is not [`SUBJECT`] is refused even when it is perfectly signed:
    /// this build knows one administrator, and a payload naming another is a payload from a
    /// format it does not understand. Signed with the real key on purpose — the point is that
    /// the subject check is a check, not a side effect of the MAC failing.
    #[test]
    fn a_correctly_signed_token_naming_another_subject_is_refused() {
        let key = test_key(1);
        let payload = Payload {
            pid: "key:attacker".to_owned(),
            iat: 1_000,
            exp: 1_000 + SESSION_TTL_SECS,
            kr: 1,
        };
        let payload_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            serde_json::to_vec(&payload).expect("payload serializes"),
        );
        let signed_span = format!("{FORMAT_TAG}.{payload_b64}");
        let mut mac = HmacSha256::new_from_slice(&hex_decode(&key.key).expect("hex key"))
            .expect("HMAC accepts any key length");
        mac.update(signed_span.as_bytes());
        let sig_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            mac.finalize().into_bytes(),
        );
        assert_eq!(
            verify(&key, &format!("{signed_span}.{sig_b64}"), 1_000),
            Err(SessionError::Malformed)
        );
    }

    #[test]
    fn hex_round_trips() {
        let bytes = [0u8, 1, 254, 255, 0x42];
        assert_eq!(hex_decode(&hex_encode(&bytes)).as_deref(), Some(&bytes[..]));
    }
}
