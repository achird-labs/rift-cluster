//! Deterministic response-body synthesis (RFC-004 §3.2).
//!
//! Everything here is a pure function of the seed, so the same spec bytes produce the same body on
//! every node and on every re-import — which is what makes a re-import diff mean "the spec changed".

use serde_json::{Map, Value};

use crate::digest::Rng;
use crate::schema::{SchemaNode, SchemaShape, TextFormat};

/// Elements rendered for one array, however large its `minItems`. A hostile spec declaring
/// `minItems: 1000000` is the width half of RFC-004 §8's "bounded generation"; the depth cap only
/// bounds the other half.
const MAX_ARRAY_ITEMS: usize = 100;

/// Values one *document's* bodies may contain in total. The last backstop: even inside the depth
/// cap and the array cap, a wide-and-deep schema multiplies out. Document-wide for the same reason
/// as [`crate::schema::MAX_SCHEMA_NODES`] — a per-body budget multiplies by the number of
/// responses, which the 4 MiB input cap permits tens of thousands of.
pub(crate) const MAX_BODY_NODES: usize = 200_000;

/// Fixed instants rather than seeded ones: a body whose timestamps moved between two compiles of
/// identical bytes would report drift that did not happen.
const FIXED_DATE: &str = "2024-01-01";
const FIXED_DATE_TIME: &str = "2024-01-01T00:00:00Z";

/// `budget` is owned by the caller and spans the whole document — see [`MAX_BODY_NODES`].
pub(crate) fn synthesize(node: &SchemaNode, rng: &mut Rng, budget: &mut usize) -> Value {
    render(node, rng, budget)
}

fn render(node: &SchemaNode, rng: &mut Rng, budget: &mut usize) -> Value {
    if *budget == 0 {
        return Value::Null;
    }
    *budget -= 1;

    match &node.shape {
        SchemaShape::Unconstrained => Value::Null,
        SchemaShape::Boolean => Value::Bool(rng.next_u64() & 1 == 1),
        SchemaShape::Integer {
            minimum,
            maximum,
            enumeration,
        } => {
            if !enumeration.is_empty() {
                let index = rng.below(enumeration.len());
                return Value::from(enumeration[index]);
            }
            Value::from(integer_in(*minimum, *maximum, rng))
        }
        SchemaShape::Number {
            minimum,
            maximum,
            enumeration,
        } => {
            if !enumeration.is_empty() {
                let index = rng.below(enumeration.len());
                return Value::from(enumeration[index]);
            }
            let low = minimum.unwrap_or(0.0);
            let high = maximum.unwrap_or(low + 1000.0);
            let span = (high - low).max(0.0);
            // Two decimal places: enough to look like a real number, few enough that the value
            // round-trips through JSON identically on every platform.
            let offset =
                f64::from(u32::try_from(rng.next_u64() % 100_000).unwrap_or(0)) / 100_000.0;
            let scaled = ((low + span * offset) * 100.0).round() / 100.0;
            // A spec is attacker-influenceable (RFC-004 §8), and bounds near `f64::MAX` overflow
            // the scaling above to infinity. `Value::from` turns a non-finite float into `null`,
            // which would then trip the self-check with a message about the wrong thing entirely —
            // so clamp to something finite here, where the cause is visible.
            Value::from(if scaled.is_finite() {
                scaled
            } else if low.is_finite() {
                low
            } else {
                0.0
            })
        }
        SchemaShape::Text {
            format,
            enumeration,
        } => {
            if !enumeration.is_empty() {
                let index = rng.below(enumeration.len());
                return Value::from(enumeration[index].clone());
            }
            Value::from(text(*format, rng))
        }
        SchemaShape::Array { items, min_items } => {
            let count = (*min_items).min(MAX_ARRAY_ITEMS);
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                if *budget == 0 {
                    break;
                }
                out.push(render(items, rng, budget));
            }
            Value::Array(out)
        }
        SchemaShape::Object { properties, .. } => {
            let mut out = Map::new();
            for (name, prop) in properties {
                if *budget == 0 {
                    break;
                }
                out.insert(name.clone(), render(prop, rng, budget));
            }
            Value::Object(out)
        }
    }
}

fn integer_in(minimum: Option<i64>, maximum: Option<i64>, rng: &mut Rng) -> i64 {
    let low = minimum.unwrap_or(0);
    let high = maximum.unwrap_or_else(|| low.saturating_add(999));
    if high <= low {
        return low;
    }
    let span = high.abs_diff(low).saturating_add(1);
    let offset = i64::try_from(rng.next_u64() % span).unwrap_or(0);
    low.saturating_add(offset)
}

fn text(format: TextFormat, rng: &mut Rng) -> String {
    let n = rng.next_u64();
    match format {
        TextFormat::Date => FIXED_DATE.to_string(),
        TextFormat::DateTime => FIXED_DATE_TIME.to_string(),
        TextFormat::Uuid => format!(
            "{:08x}-{:04x}-4{:03x}-a{:03x}-{:012x}",
            n & 0xffff_ffff,
            (n >> 8) & 0xffff,
            (n >> 16) & 0xfff,
            (n >> 28) & 0xfff,
            n & 0xffff_ffff_ffff,
        ),
        TextFormat::Email => format!("user{:04x}@example.com", n & 0xffff),
        // Base64 of the generated text, so a `format: byte` field decodes rather than exploding in
        // a consumer that trusts the declared format.
        TextFormat::Byte => base64_of(&format!("bytes-{:04x}", n & 0xffff)),
        TextFormat::Plain => format!("string-{:04x}", n & 0xffff),
    }
}

/// Standard base64 with padding. Small enough not to justify a dependency, and pinning it here
/// keeps the golden files independent of any encoder's default alphabet.
fn base64_of(input: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).map_or(0, |b| u32::from(*b));
        let b2 = chunk.get(2).map_or(0, |b| u32::from(*b));
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 63] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::SpecDigest;
    use crate::schema::SchemaNode;

    fn rng() -> Rng {
        Rng::seeded(&SpecDigest::of(b"spec"), "op", "200")
    }

    #[test]
    fn an_unconstrained_schema_renders_null() {
        assert_eq!(
            synthesize(
                &SchemaNode::unconstrained(),
                &mut rng(),
                &mut MAX_BODY_NODES.clone()
            ),
            Value::Null
        );
    }

    #[test]
    fn an_enumeration_is_always_drawn_from_the_declared_set() {
        let node = SchemaNode {
            nullable: false,
            shape: SchemaShape::Text {
                format: TextFormat::Plain,
                enumeration: vec!["a".into(), "b".into()],
            },
        };
        for seed in 0..32u8 {
            let mut r = Rng::seeded(&SpecDigest::of(&[seed]), "op", "200");
            let value = synthesize(&node, &mut r, &mut MAX_BODY_NODES.clone());
            assert!(
                ["a", "b"].contains(&value.as_str().expect("string")),
                "{value}"
            );
        }
    }

    #[test]
    fn an_integer_stays_within_its_declared_bounds() {
        let node = SchemaNode {
            nullable: false,
            shape: SchemaShape::Integer {
                minimum: Some(10),
                maximum: Some(12),
                enumeration: vec![],
            },
        };
        for seed in 0..64u8 {
            let mut r = Rng::seeded(&SpecDigest::of(&[seed]), "op", "200");
            let value = synthesize(&node, &mut r, &mut MAX_BODY_NODES.clone())
                .as_i64()
                .expect("integer");
            assert!((10..=12).contains(&value), "{value} escaped [10, 12]");
        }
    }

    /// The width half of the §8 bound: `minItems` is honoured, but not without limit.
    #[test]
    fn array_rendering_is_capped_however_large_min_items_is() {
        let node = SchemaNode {
            nullable: false,
            shape: SchemaShape::Array {
                items: Box::new(SchemaNode {
                    nullable: false,
                    shape: SchemaShape::Boolean,
                }),
                min_items: 1_000_000,
            },
        };
        let rendered = synthesize(&node, &mut rng(), &mut MAX_BODY_NODES.clone());
        assert_eq!(rendered.as_array().expect("array").len(), MAX_ARRAY_ITEMS);
    }

    #[test]
    fn base64_matches_the_standard_alphabet_and_padding() {
        assert_eq!(base64_of("f"), "Zg==");
        assert_eq!(base64_of("fo"), "Zm8=");
        assert_eq!(base64_of("foo"), "Zm9v");
        assert_eq!(base64_of("foobar"), "Zm9vYmFy");
    }
}
