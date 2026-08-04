/// Why a spec could not be compiled.
///
/// The first four variants classify a fault in the *input*. `SelfCheck` is different in kind: it
/// reports that the compiler's own output contradicts the contract the same compilation emitted.
/// It is separate rather than folded into `Parse` because calling an internal inconsistency a parse
/// error would send the reader looking for a syntax problem that is not there.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompileError {
    #[error("unsupported spec version {found}: v1 compiles OpenAPI 3.0.x only")]
    UnsupportedVersion { found: String },

    #[error("spec exceeds {max} bytes")]
    TooLarge { max: usize },

    #[error("external $ref {reference:?} refused: remote references do not replicate")]
    ExternalRef { reference: String },

    #[error("parse: {0}")]
    Parse(String),

    #[error(
        "self-check: the body compiled for {operation} {status} violates its own schema: {detail}"
    )]
    SelfCheck {
        operation: String,
        status: String,
        detail: String,
    },
}
