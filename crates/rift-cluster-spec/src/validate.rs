//! Static validation of a stub's `is` body against the response schema it claims to implement.
//!
//! Two callers, one rule set: the compiler runs it over its own output and *fails compilation*
//! (RFC-004 §3.2), and edit-time admission runs it over a hand-written body and *warns*. The
//! severity difference belongs to the caller — a deliberately-divergent hand-written stub is a
//! legitimate fixture, a self-inconsistent generated one is a bug.

use serde_json::Value;

use crate::schema::{SchemaNode, SchemaShape};
use crate::{CompiledOperation, StatusKey};

/// One way a body disagrees with its schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// RFC 6901 JSON pointer to the offending value, e.g. `/tags/0`. Empty for the whole body.
    pub pointer: String,
    pub kind: ViolationKind,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationKind {
    /// The operation never declares this status, so there is no contract to check against.
    UnknownStatus,
    MissingRequired,
    TypeMismatch,
    NotInEnum,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let at = if self.pointer.is_empty() {
            "(body)"
        } else {
            &self.pointer
        };
        write!(f, "{at}: {}", self.detail)
    }
}

/// Check `body` against the schema `op` declares for `status`.
///
/// An empty result means the body is consistent with the contract. A response that declares no
/// schema constrains nothing and yields no violations — reporting one there would make every
/// schemaless operation unusable.
#[must_use]
pub fn validate_stub_response(
    op: &CompiledOperation,
    status: &StatusKey,
    body: &Value,
) -> Vec<Violation> {
    let Some(response) = op.response(status) else {
        return vec![Violation {
            pointer: String::new(),
            kind: ViolationKind::UnknownStatus,
            detail: format!("operation {} declares no {status} response", op.id.as_str()),
        }];
    };

    check_body(&response.schema, body)
}

/// The same check without an operation index, for the compiler's self-check over a body it is in
/// the middle of emitting.
pub(crate) fn check_body(node: &SchemaNode, body: &Value) -> Vec<Violation> {
    let mut out = Vec::new();
    check(node, body, &mut String::new(), &mut out);
    out
}

fn check(node: &SchemaNode, value: &Value, pointer: &mut String, out: &mut Vec<Violation>) {
    if node.is_unconstrained() {
        return;
    }
    if value.is_null() && node.nullable {
        return;
    }

    match &node.shape {
        SchemaShape::Unconstrained => {}
        SchemaShape::Boolean => {
            if !value.is_boolean() {
                out.push(mismatch(pointer, "boolean", value));
            }
        }
        SchemaShape::Integer { enumeration, .. } => {
            if !(value.is_i64() || value.is_u64()) {
                out.push(mismatch(pointer, "integer", value));
            } else if !enumeration.is_empty()
                && !value.as_i64().is_some_and(|v| enumeration.contains(&v))
            {
                out.push(not_in_enum(pointer, value));
            }
        }
        SchemaShape::Number { enumeration, .. } => {
            if !value.is_number() {
                out.push(mismatch(pointer, "number", value));
            } else if !enumeration.is_empty()
                // Exact equality, deliberately: the spec's enum lists the values it accepts, and a
                // tolerance here would accept ones it does not.
                && !value.as_f64().is_some_and(|v| enumeration.contains(&v))
            {
                out.push(not_in_enum(pointer, value));
            }
        }
        SchemaShape::Text { enumeration, .. } => {
            let Some(text) = value.as_str() else {
                out.push(mismatch(pointer, "string", value));
                return;
            };
            if !enumeration.is_empty() && !enumeration.iter().any(|e| e == text) {
                out.push(not_in_enum(pointer, value));
            }
        }
        SchemaShape::Array { items, .. } => {
            let Some(elements) = value.as_array() else {
                out.push(mismatch(pointer, "array", value));
                return;
            };
            for (index, element) in elements.iter().enumerate() {
                let restore = pointer.len();
                pointer.push('/');
                pointer.push_str(&index.to_string());
                check(items, element, pointer, out);
                pointer.truncate(restore);
            }
        }
        SchemaShape::Object {
            properties,
            required,
        } => {
            let Some(map) = value.as_object() else {
                out.push(mismatch(pointer, "object", value));
                return;
            };
            for name in required {
                if !map.contains_key(name) {
                    out.push(Violation {
                        pointer: pointer.clone(),
                        kind: ViolationKind::MissingRequired,
                        detail: format!("required property {name:?} is absent"),
                    });
                }
            }
            // Properties the schema does not name are permitted: OpenAPI objects are open unless
            // `additionalProperties: false`, and flagging them would make every extension a warning.
            for (name, prop) in properties {
                let Some(present) = map.get(name) else {
                    continue;
                };
                let restore = pointer.len();
                pointer.push('/');
                pointer.push_str(&escape_pointer_token(name));
                check(prop, present, pointer, out);
                pointer.truncate(restore);
            }
        }
    }
}

fn mismatch(pointer: &str, expected: &str, actual: &Value) -> Violation {
    Violation {
        pointer: pointer.to_string(),
        kind: ViolationKind::TypeMismatch,
        detail: format!("expected {expected}, found {}", type_name(actual)),
    }
}

fn not_in_enum(pointer: &str, actual: &Value) -> Violation {
    Violation {
        pointer: pointer.to_string(),
        kind: ViolationKind::NotInEnum,
        detail: format!("{actual} is not one of the declared enum values"),
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_f64() => "number",
        Value::Number(_) => "integer",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// RFC 6901: `~` and `/` are escaped inside a pointer token, in that order.
fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_tokens_are_rfc_6901_escaped() {
        assert_eq!(escape_pointer_token("a/b"), "a~1b");
        assert_eq!(escape_pointer_token("m~n"), "m~0n");
        assert_eq!(escape_pointer_token("~/"), "~0~1");
    }

    #[test]
    fn a_nullable_field_accepts_null() {
        let node = SchemaNode {
            nullable: true,
            shape: SchemaShape::Text {
                format: crate::schema::TextFormat::Plain,
                enumeration: vec![],
            },
        };
        let mut out = Vec::new();
        check(&node, &Value::Null, &mut String::new(), &mut out);
        assert_eq!(out, vec![]);
    }

    #[test]
    fn a_non_nullable_field_rejects_null() {
        let node = SchemaNode {
            nullable: false,
            shape: SchemaShape::Boolean,
        };
        let mut out = Vec::new();
        check(&node, &Value::Null, &mut String::new(), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, ViolationKind::TypeMismatch);
    }

    #[test]
    fn nested_violations_carry_a_pointer_to_the_exact_element() {
        let node = SchemaNode {
            nullable: false,
            shape: SchemaShape::Array {
                items: Box::new(SchemaNode {
                    nullable: false,
                    shape: SchemaShape::Boolean,
                }),
                min_items: 1,
            },
        };
        let mut out = Vec::new();
        check(
            &node,
            &serde_json::json!([true, "nope"]),
            &mut String::new(),
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pointer, "/1");
    }
}
