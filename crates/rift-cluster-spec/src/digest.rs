use sha2::{Digest, Sha256};

/// sha256 over the spec bytes exactly as supplied.
///
/// Deliberately over the raw bytes and not a normalised re-serialisation: the digest is what
/// re-import compares to decide "unchanged", and a normalising digest would call two different
/// documents the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpecDigest([u8; 32]);

impl SpecDigest {
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hasher.finalize().into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            // Writing into the buffer rather than `push_str(&format!(..))` avoids 32 throwaway
            // allocations; `write!` to a String cannot fail.
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

impl std::fmt::Display for SpecDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// The deterministic generator behind schema-driven response synthesis.
///
/// Seeded `sha256(digest ‖ operation_id ‖ status)` (RFC-004 §3.2) so the same spec bytes yield the
/// same body on every node and on every re-import — which is what lets a re-import diff mean
/// "the spec changed" rather than "the generator rolled differently".
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn seeded(digest: &SpecDigest, operation: &str, status: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(digest.as_bytes());
        hasher.update(operation.as_bytes());
        hasher.update(status.as_bytes());
        let seed: [u8; 32] = hasher.finalize().into();
        let mut first = [0u8; 8];
        first.copy_from_slice(&seed[..8]);
        Self(u64::from_le_bytes(first))
    }

    /// splitmix64 — a fixed, dependency-free sequence. The exact algorithm is part of the output
    /// contract (the golden files pin it), so changing it is a deliberate re-baseline, not a tidy-up.
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`, or 0 when `n` is 0.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        usize::try_from(self.next_u64() % n as u64).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_renders_as_lowercase_hex() {
        let digest = SpecDigest::of(b"");
        assert_eq!(
            digest.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "sha256 of the empty input",
        );
        assert_eq!(digest.to_string(), digest.to_hex());
    }

    #[test]
    fn the_same_seed_inputs_produce_the_same_sequence() {
        let digest = SpecDigest::of(b"spec");
        let mut a = Rng::seeded(&digest, "op", "200");
        let mut b = Rng::seeded(&digest, "op", "200");
        let drawn: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let again: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(drawn, again);
    }

    #[test]
    fn each_operation_and_status_gets_its_own_stream() {
        let digest = SpecDigest::of(b"spec");
        let first = Rng::seeded(&digest, "op", "200").next_u64();
        assert_ne!(first, Rng::seeded(&digest, "op", "404").next_u64());
        assert_ne!(first, Rng::seeded(&digest, "other", "200").next_u64());
        assert_ne!(
            first,
            Rng::seeded(&SpecDigest::of(b"other"), "op", "200").next_u64()
        );
    }
}
