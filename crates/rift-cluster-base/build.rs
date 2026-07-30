//! Captures which open-source Rift this cluster build embeds.
//!
//! The vendored crates all inherit `0.1.0` from `vendor/rift`'s own workspace,
//! so their `CARGO_PKG_VERSION` says nothing about which Rift is in the binary.
//! The submodule pin does, and it is the thing an operator needs to correlate a
//! running cluster binary with an upstream release or bug report.

use std::path::PathBuf;
use std::process::Command;

/// Reported when the pin cannot be determined. Deliberately a visible marker
/// rather than a plausible-looking version: a wrong version in a banner is worse
/// than an obviously absent one, because it gets pasted into bug reports.
const UNKNOWN: &str = "unknown";

/// Both the override a packager sets and the variable the crate reads. One name
/// so the two can never drift apart.
const UPSTREAM_ENV: &str = "RIFT_UPSTREAM_VERSION";

fn main() {
    // A packager who knows the pin can state it: a container build, a distro
    // build and a release job all commonly build from a context with no git
    // history at all, and `unknown` is the honest-but-useless answer there.
    println!("cargo:rerun-if-env-changed={UPSTREAM_ENV}");
    if let Ok(explicit) = std::env::var(UPSTREAM_ENV)
        && !explicit.trim().is_empty()
    {
        println!("cargo:rustc-env={UPSTREAM_ENV}={}", explicit.trim());
        return;
    }

    let vendored = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("vendor")
        .join("rift");

    // Re-run when the pin moves. The gitlink file under .git/modules is not
    // reliably present (worktrees, fresh clones), so watch the submodule path
    // itself, which changes whenever the checkout does.
    println!("cargo:rerun-if-changed={}", vendored.display());

    let pin = describe(&vendored).unwrap_or_else(|| UNKNOWN.to_owned());
    println!("cargo:rustc-env={UPSTREAM_ENV}={pin}");
}

/// `git describe` the vendored checkout: an exact tag when the pin is a release
/// (`v0.15.0`), otherwise the nearest tag plus the commit.
///
/// Every failure here is non-fatal by design — a source tarball, a build image
/// without git, or a shallow clone are all legitimate ways to build this crate,
/// and none of them should break the build over a banner string.
fn describe(vendored: &std::path::Path) -> Option<String> {
    if !vendored.join(".git").exists() && !vendored.join("Cargo.toml").exists() {
        return None;
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(vendored)
        .args(["describe", "--tags", "--always", "--dirty"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let described = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!described.is_empty()).then_some(described)
}
