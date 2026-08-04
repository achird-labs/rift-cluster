//! One normalised view of a response schema, resolved once and used by both synthesis and
//! validation.
//!
//! Sharing the tree is the point: a body generated against one reading of the schema and checked
//! against another would let the compiler's own self-check pass on output that is wrong, which is
//! exactly the inconsistency the self-check exists to catch.

use openapiv3::{Components, ReferenceOr, Schema, SchemaKind, Type};

/// Recursion floor (RFC-004 §3.2). A self-referential schema renders `null` here instead of
/// recursing until the compiler dies.
pub(crate) const MAX_DEPTH: usize = 8;

/// Total normalised nodes one *document* may produce, across every response in it.
///
/// Depth alone does not bound a schema that *branches* — 100 properties nested 8 deep is 100^8 —
/// so §8's "bounded generation" needs a node budget as well as a depth cap. It must be
/// document-wide rather than per-response: a per-response budget multiplies by the response count,
/// and a 4 MiB spec holds tens of thousands of responses, so the real bound would be
/// `responses × budget` — gigabytes of retained tree, on the accepting node, before anything is
/// committed. Past the budget, subtrees normalise to `Unconstrained`.
pub(crate) const MAX_SCHEMA_NODES: usize = 200_000;

/// A response schema reduced to what synthesis and validation actually need.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaNode {
    /// OpenAPI 3.0 `nullable`. Carried so validation does not report a violation for a `null` the
    /// spec explicitly permits.
    pub nullable: bool,
    pub shape: SchemaShape,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaShape {
    /// No schema, an unresolvable reference, or the depth/budget floor. Constrains nothing:
    /// synthesis renders `null` and validation stays silent.
    Unconstrained,
    Boolean,
    Integer {
        minimum: Option<i64>,
        maximum: Option<i64>,
        enumeration: Vec<i64>,
    },
    Number {
        minimum: Option<f64>,
        maximum: Option<f64>,
        enumeration: Vec<f64>,
    },
    Text {
        format: TextFormat,
        enumeration: Vec<String>,
    },
    Array {
        items: Box<SchemaNode>,
        min_items: usize,
    },
    Object {
        /// Declaration order, so a re-compile of unchanged bytes walks them identically.
        properties: Vec<(String, SchemaNode)>,
        required: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextFormat {
    Plain,
    Date,
    DateTime,
    Uuid,
    Email,
    Byte,
}

impl SchemaNode {
    #[must_use]
    pub fn unconstrained() -> Self {
        Self {
            nullable: false,
            shape: SchemaShape::Unconstrained,
        }
    }

    pub(crate) fn is_unconstrained(&self) -> bool {
        self.shape == SchemaShape::Unconstrained
    }
}

/// Resolve and flatten a response schema into a `SchemaNode`.
///
/// `budget` is owned by the caller and spans the whole document — see [`MAX_SCHEMA_NODES`].
pub(crate) fn normalize(
    schema: &ReferenceOr<Schema>,
    components: Option<&Components>,
    budget: &mut usize,
) -> SchemaNode {
    resolve(schema, components, 0, budget)
}

fn resolve(
    schema: &ReferenceOr<Schema>,
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
) -> SchemaNode {
    match schema {
        ReferenceOr::Reference { reference } => follow(reference, components, depth, budget),
        ReferenceOr::Item(item) => normalize_schema(item, components, depth, budget),
    }
}

fn resolve_boxed(
    schema: &ReferenceOr<Box<Schema>>,
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
) -> SchemaNode {
    match schema {
        ReferenceOr::Reference { reference } => follow(reference, components, depth, budget),
        ReferenceOr::Item(item) => normalize_schema(item, components, depth, budget),
    }
}

/// Follow a local `#/components/schemas/<name>` reference. External references never reach here —
/// they are refused before compilation starts (RFC-004 §3.1) — so anything unresolvable at this
/// point is a dangling in-document pointer, which constrains nothing rather than failing the import.
fn follow(
    reference: &str,
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
) -> SchemaNode {
    let Some(name) = reference.strip_prefix("#/components/schemas/") else {
        return SchemaNode::unconstrained();
    };
    let Some(target) = components.and_then(|c| c.schemas.get(name)) else {
        return SchemaNode::unconstrained();
    };
    resolve(target, components, depth, budget)
}

fn normalize_schema(
    schema: &Schema,
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
) -> SchemaNode {
    if depth >= MAX_DEPTH || *budget == 0 {
        return SchemaNode::unconstrained();
    }
    *budget -= 1;
    let nullable = schema.schema_data.nullable;

    let shape = match &schema.schema_kind {
        SchemaKind::Type(Type::Boolean(_)) => SchemaShape::Boolean,
        SchemaKind::Type(Type::Integer(t)) => SchemaShape::Integer {
            minimum: t.minimum,
            maximum: t.maximum,
            enumeration: t.enumeration.iter().flatten().copied().collect(),
        },
        SchemaKind::Type(Type::Number(t)) => SchemaShape::Number {
            minimum: t.minimum,
            maximum: t.maximum,
            enumeration: t.enumeration.iter().flatten().copied().collect(),
        },
        SchemaKind::Type(Type::String(t)) => SchemaShape::Text {
            format: text_format(t),
            enumeration: t.enumeration.iter().flatten().cloned().collect(),
        },
        SchemaKind::Type(Type::Array(t)) => SchemaShape::Array {
            items: Box::new(match &t.items {
                Some(items) => resolve_boxed(items, components, depth + 1, budget),
                None => SchemaNode::unconstrained(),
            }),
            min_items: t.min_items.unwrap_or(1),
        },
        SchemaKind::Type(Type::Object(t)) => SchemaShape::Object {
            // `additionalProperties` renders nothing (RFC-004 §3.2): it describes keys the spec
            // never names, so there is no honest value to invent for them.
            properties: t
                .properties
                .iter()
                .map(|(name, prop)| {
                    (
                        name.clone(),
                        resolve_boxed(prop, components, depth + 1, budget),
                    )
                })
                .collect(),
            required: t.required.clone(),
        },
        // A composed schema is reduced to one concrete shape deterministically: `allOf` merges,
        // and a choice takes the first branch. Picking by seed instead would make the chosen branch
        // an invisible input to every re-import diff.
        SchemaKind::AllOf { all_of } => {
            return merge_all_of(all_of, components, depth, budget, nullable);
        }
        SchemaKind::OneOf { one_of } => {
            return first_branch(one_of, components, depth, budget, nullable);
        }
        SchemaKind::AnyOf { any_of } => {
            return first_branch(any_of, components, depth, budget, nullable);
        }
        SchemaKind::Not { .. } | SchemaKind::Any(_) => SchemaShape::Unconstrained,
    };

    SchemaNode { nullable, shape }
}

fn first_branch(
    branches: &[ReferenceOr<Schema>],
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
    nullable: bool,
) -> SchemaNode {
    match branches.first() {
        Some(branch) => {
            let mut node = resolve(branch, components, depth, budget);
            node.nullable |= nullable;
            node
        }
        None => SchemaNode::unconstrained(),
    }
}

fn merge_all_of(
    branches: &[ReferenceOr<Schema>],
    components: Option<&Components>,
    depth: usize,
    budget: &mut usize,
    nullable: bool,
) -> SchemaNode {
    let mut properties: Vec<(String, SchemaNode)> = Vec::new();
    let mut required: Vec<String> = Vec::new();
    let mut saw_object = false;
    let mut fallback = SchemaNode::unconstrained();

    for branch in branches {
        let node = resolve(branch, components, depth, budget);
        match node.shape {
            SchemaShape::Object {
                properties: props,
                required: req,
            } => {
                saw_object = true;
                for (name, value) in props {
                    // A later branch redefining a property wins, matching how allOf is read.
                    if let Some(existing) = properties.iter_mut().find(|(n, _)| *n == name) {
                        existing.1 = value;
                    } else {
                        properties.push((name, value));
                    }
                }
                for name in req {
                    if !required.contains(&name) {
                        required.push(name);
                    }
                }
            }
            // A non-object branch cannot be merged into a property bag; keep the first one as the
            // shape in case every branch turns out to be a scalar.
            _ if fallback.is_unconstrained() => fallback = node,
            _ => {}
        }
    }

    if saw_object {
        SchemaNode {
            nullable,
            shape: SchemaShape::Object {
                properties,
                required,
            },
        }
    } else {
        fallback.nullable |= nullable;
        fallback
    }
}

fn text_format(t: &openapiv3::StringType) -> TextFormat {
    use openapiv3::{StringFormat, VariantOrUnknownOrEmpty};
    match &t.format {
        VariantOrUnknownOrEmpty::Item(StringFormat::Date) => TextFormat::Date,
        VariantOrUnknownOrEmpty::Item(StringFormat::DateTime) => TextFormat::DateTime,
        VariantOrUnknownOrEmpty::Item(StringFormat::Byte) => TextFormat::Byte,
        VariantOrUnknownOrEmpty::Unknown(name) => match name.as_str() {
            "uuid" => TextFormat::Uuid,
            "email" => TextFormat::Email,
            "date" => TextFormat::Date,
            "date-time" => TextFormat::DateTime,
            _ => TextFormat::Plain,
        },
        _ => TextFormat::Plain,
    }
}
