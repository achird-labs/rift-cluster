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
//! `v2.<base64url_nopad(payload_json)>.<base64url_nopad(hmac_sha256)>`
//!
//! where the payload is `{"pid": "admin", "iat": <unix secs>, "exp": <unix secs>,
//! "kr": <key revision>}`. `pid` is the fixed subject [`SUBJECT`] — the one administrator a
//! keyed fleet has — kept in the signed span rather than dropped so the format stays readable
//! in a log dump and a future second subject is a value change, not a format change. The HMAC
//! signs the ASCII bytes of `v2.<payload_b64>` — the version tag is inside the signed span, not
//! just a prefix on the wire, so one format cannot be replayed as if it were another by an
//! attacker who only controls the tag.
//!
//! The tag moved from `v1` to `v2` in D-85 because the *meaning* of the signed span changed with
//! the key it is signed under (below), not because its shape did. A cookie minted by an earlier
//! build therefore reads as [`SessionError::Malformed`] — a format this build does not speak —
//! rather than as a forgery, which is the honest description of it.
//!
//! # The two bounds on a session, and why neither needs a table
//!
//! `kr` (key revision) is what makes rotation a fleet-wide kill switch with no table to sweep:
//! every node verifies against its own applied [`SessionKey`], and a token minted under
//! revision N fails [`verify`] the instant revision N+1 is committed — on every replica,
//! simultaneously, with no per-session bookkeeping anywhere. `POST /session/rotate` is what
//! commits that N+1 (D-85); before it existed the mechanism had no caller.
//!
//! The second bound is the signing key itself. It is **not** the replicated record: it is that
//! record bound to this node's `--api-key` ([`SigningKey::derive`]). A cookie is a stand-in for
//! the key it was exchanged for, so it must not outlive it — a node started with a different
//! `--api-key` derives a different MAC key and refuses every cookie minted under the old one,
//! with no Raft write, no stored fingerprint and nothing for two nodes to disagree about beyond
//! what they already disagree about mid-roll: which bearer they accept.

use std::fmt;

use hmac::{Hmac, Mac};
use rift_cluster::control::{SESSION_KEY_BYTES, SessionKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// The token format tag. Part of the signed span (see the module doc) rather than a bare
/// prefix, so it cannot be swapped without invalidating the signature.
const FORMAT_TAG: &str = "v2";

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
    /// Not about a token at all: the fleet's *stored* signing-key record is not usable, so this
    /// node cannot mint or verify anything. Its own variant rather than [`SessionError::Malformed`]
    /// because it is the one variant an operator actually reads — it reaches a `500` body and the
    /// node's log while nobody can log in — and "session token is malformed" would point them at a
    /// token that does not exist.
    #[error(
        "the fleet's stored session-signing key is not {SESSION_KEY_BYTES} bytes of hex; no session can be minted or verified on this node"
    )]
    UnusableSigningKey,
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

/// The domain separator mixed into every derived MAC key. The NUL terminates the domain rather
/// than the key: with a fixed domain and the key trailing, two different keys already produce two
/// different inputs, so the separator earns its keep only if a second domain is ever added — which
/// is exactly when forgetting it would silently make two derivations collide.
const API_KEY_BINDING_DOMAIN: &[u8] = b"rift-session/api-key-binding\0";

/// The key a session token is actually signed with: the fleet's replicated [`SessionKey`] record
/// **bound to this node's `--api-key`** (D-85).
///
/// [`SigningKey::derive`] is the only way to obtain one, so signing with the bare record is
/// unrepresentable rather than merely discouraged — there is no path to [`mint`] or [`verify`]
/// that skips the binding, which is what makes "a cookie is accepted exactly where, and for as
/// long as, the key it was exchanged for is" a property of the type instead of a rule every call
/// site has to remember.
pub(crate) struct SigningKey {
    mac_key: [u8; MAC_KEY_BYTES],
    /// The revision of the record this was derived from — the `kr` claim [`mint`] stamps and
    /// [`verify`] matches.
    revision: u64,
}

/// HMAC-SHA256's output width, which is also the width of the key derived from it.
const MAC_KEY_BYTES: usize = 32;

impl fmt::Debug for SigningKey {
    /// Never renders `mac_key`. A derived MAC key mints cookies just as well as the record it
    /// came from, and a `{:?}` in a log line or an error is the cheapest possible way to leak one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey")
            .field("mac_key", &"<redacted>")
            .field("revision", &self.revision)
            .finish()
    }
}

impl SigningKey {
    /// Bind `record` to `api_key`: `HMAC-SHA256(record_key_bytes, DOMAIN || api_key)`.
    ///
    /// [`SessionError::UnusableSigningKey`] iff `record.key` is not exactly [`SESSION_KEY_BYTES`] of hex —
    /// **the same rule [`rift_cluster::control::validate`] applies before the record is ever
    /// committed**, restated here rather than assumed, so the two can only disagree by one of
    /// them changing. Reaching that arm means the fleet's applied state is not what the control
    /// plane admits: an invariant breach, not anything a caller controls.
    ///
    /// The length half of the check is not pedantry. `hex_decode("")` succeeds with empty key
    /// material, and HMAC accepts a key of any length, so without it an empty record would derive
    /// a perfectly functional signing key and every session under it would work — the breach
    /// hidden behind sessions that verify. The same goes for falling back to the raw string's
    /// bytes on a decode failure, which is why this reports instead.
    pub(crate) fn derive(record: &SessionKey, api_key: &str) -> Result<Self, SessionError> {
        let key_bytes = hex_decode(record.key.expose())
            .filter(|bytes| bytes.len() == SESSION_KEY_BYTES)
            .ok_or(SessionError::UnusableSigningKey)?;
        // `new_from_slice` is fallible only for algorithms with a fixed key size; HMAC has none —
        // its construction hashes an over-long key and pads a short one — so this has no
        // reachable `Err` (see `hmac` 0.12.1's `HmacCore::new_from_slice`). `derive` already
        // returns a `Result`, so it propagates rather than proving the point with an `expect`.
        let mut mac =
            HmacSha256::new_from_slice(&key_bytes).map_err(|_| SessionError::UnusableSigningKey)?;
        mac.update(API_KEY_BINDING_DOMAIN);
        mac.update(api_key.as_bytes());
        Ok(Self {
            mac_key: mac.finalize().into_bytes().into(),
            revision: record.revision,
        })
    }
}

/// Mint a session token signed under `key`, valid from `now_secs` for `ttl_secs`. The subject is
/// the constant [`SUBJECT`] — there has been nothing to name since #550.
///
/// Mint always produces a token, so it returns a bare `String` rather than a `Result`. The one
/// hex decode that could fail already happened in [`SigningKey::derive`], which is the only way
/// to hold the `key` argument at all, so the two steps left that *look* fallible are handled
/// without panicking:
///
/// - `Payload` is plain strings and integers, so its JSON encoding cannot realistically fail;
///   the (unreachable) error case falls back to an empty payload rather than an `.unwrap()`, so
///   a change elsewhere that somehow made this fallible would produce a token nothing can ever
///   [`verify`] instead of panicking mid-login.
/// - HMAC-SHA256 key construction, which the `hmac` crate's `KeyInit::new_from_slice` exposes as
///   fallible for API-uniformity with algorithms that do have fixed key sizes — HMAC itself
///   accepts a key of *any* length (its own construction hashes an over-long key and pads a short
///   one), so this `expect` documents a proof rather than hoping: see `hmac` 0.12.1's
///   `HmacCore::new_from_slice` (`optim.rs`), which has no `Err` path a fixed-length key can
///   reach.
#[must_use]
pub(crate) fn mint(key: &SigningKey, now_secs: u64, ttl_secs: u64) -> String {
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

    let mut mac = HmacSha256::new_from_slice(&key.mac_key)
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
pub(crate) fn verify(key: &SigningKey, token: &str, now_secs: u64) -> Result<(), SessionError> {
    let mut parts = token.split('.');
    let (Some(tag), Some(payload_b64), Some(sig_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SessionError::Malformed);
    };
    if tag != FORMAT_TAG {
        return Err(SessionError::Malformed);
    }

    // `new_from_slice` is fallible only for algorithms with a fixed key size; HMAC has none, so
    // this never actually returns `Err` (see `mint`'s doc for the proof), but `verify` already
    // returns a `Result`, so there is no reason to `expect` here rather than propagate.
    let mut mac =
        HmacSha256::new_from_slice(&key.mac_key).map_err(|_| SessionError::BadSignature)?;
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
    use rift_cluster::control::SessionKeyHex;

    /// The replicated record, before it is bound to anything.
    fn test_record(revision: u64) -> SessionKey {
        SessionKey {
            key: SessionKeyHex::new(hex_encode(&[0x42; 32])),
            revision,
        }
    }

    /// The key a node actually signs with: [`test_record`] bound to one fixed `--api-key`. Every
    /// test below that does not care about the binding goes through here, so the binding is
    /// exercised by the whole suite rather than only by the tests that name it.
    fn test_key(revision: u64) -> SigningKey {
        SigningKey::derive(&test_record(revision), "the-fleet-api-key").expect("the record is hex")
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
        let token = mint(&test_key(1), 1_000, SESSION_TTL_SECS);
        // Same key bytes and the same `--api-key`, but the revision moved on (a rotation
        // committed) — the token must stop verifying immediately, with no table of outstanding
        // sessions swept to make that true.
        let rotated = SigningKey::derive(
            &SessionKey {
                key: test_record(1).key,
                revision: 2,
            },
            "the-fleet-api-key",
        )
        .expect("the record is hex");
        assert_eq!(
            verify(&rotated, &token, 1_000),
            Err(SessionError::WrongRevision)
        );
    }

    /// Pins D-85: a cookie is a stand-in for the `--api-key` it was exchanged for, so a node
    /// running a different key must refuse it — with no replicated state, no fingerprint and no
    /// coordination, which is what makes "changing `--api-key` ends sessions" free.
    ///
    /// The third assertion is the one that makes the binding a binding rather than a decoration:
    /// the *unbound* record cannot verify a bound token either, so there is no path back to
    /// signing with the raw record.
    #[test]
    fn a_token_minted_under_one_api_key_is_refused_under_another() {
        let record = test_record(1);
        let old = SigningKey::derive(&record, "the-old-api-key").expect("hex");
        let new = SigningKey::derive(&record, "the-new-api-key").expect("hex");
        let token = mint(&old, 1_000, SESSION_TTL_SECS);

        assert_eq!(verify(&old, &token, 1_000), Ok(()));
        assert_eq!(
            verify(&new, &token, 1_000),
            Err(SessionError::BadSignature),
            "the same record under a different --api-key must not verify"
        );
        assert_eq!(
            verify(
                &SigningKey::derive(&record, "").expect("hex"),
                &token,
                1_000
            ),
            Err(SessionError::BadSignature),
            "an empty --api-key is a different key, not an unbound one"
        );
    }

    /// A record `control::validate` should never have admitted. `derive` reports it rather than
    /// falling back to usable key material: a token signed with fallback bytes would verify
    /// against itself, which is worse than a login that fails loudly. The variant is
    /// [`SessionError::UnusableSigningKey`], not `Malformed` — the fault is the fleet's stored
    /// key, and the operator reading the message has no token in hand to be confused about.
    ///
    /// `""` and a short-but-clean-hex record are in the list on purpose — both decode fine, and
    /// HMAC would happily take either as a key, so only the length check refuses them.
    #[test]
    fn deriving_from_a_record_that_is_not_hex_or_the_wrong_length_is_refused() {
        for bad in [
            "",
            "zz",
            "abc",
            "0x42",
            "42",
            &"ab".repeat(SESSION_KEY_BYTES + 1),
        ] {
            assert_eq!(
                SigningKey::derive(
                    &SessionKey {
                        key: SessionKeyHex::new(bad.to_owned()),
                        revision: 1,
                    },
                    "the-fleet-api-key",
                )
                .err(),
                Some(SessionError::UnusableSigningKey),
                "record {bad:?} must not derive a signing key"
            );
        }
    }

    /// A cookie minted by a pre-D-85 build carries the `v1` tag. It must read as a format this
    /// build does not speak — not as a forgery — because the signed span's *meaning* changed, not
    /// its shape. The tag is checked before the MAC, so this is `Malformed` rather than
    /// `BadSignature`.
    #[test]
    fn a_v1_token_is_refused_as_a_format_this_build_does_not_speak() {
        let key = test_key(1);
        let token = mint(&key, 1_000, SESSION_TTL_SECS);
        let v1 = token
            .strip_prefix(FORMAT_TAG)
            .map(|rest| format!("v1{rest}"))
            .expect("a minted token starts with the format tag");
        assert_eq!(verify(&key, &v1, 1_000), Err(SessionError::Malformed));
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
        for garbage in ["", "not-a-token", "v2.onlyonepart", "v2.a.b", "v2.a.b.c"] {
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
        let mut mac =
            HmacSha256::new_from_slice(&key.mac_key).expect("HMAC accepts any key length");
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
