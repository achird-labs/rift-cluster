//! Rift Enterprise Edition.
//!
//! This crate is the entry point for proprietary functionality that builds on
//! top of the open-source Rift core (vendored under `vendor/rift`). The
//! open-source crates are re-exported here so enterprise code depends on a
//! single facade rather than reaching into the submodule directly.

pub use rift_core;
pub use rift_types;

/// Build edition marker, surfaced in banners and `--version` output.
pub const EDITION: &str = "enterprise";

/// Returns the semantic version of this enterprise build.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
