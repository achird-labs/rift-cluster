//! Tripwire: the clustered manager must not silently fall behind upstream's
//! (issue #30).
//!
//! `ServerBuilder::start()` builds an `ImposterManager` itself when no manager
//! is injected. `compose::cluster_manager` hand-mirrors that construction,
//! because `.manager()` replaces it **wholesale** — the seam is all-or-nothing,
//! so anything upstream adds inside its `None` arm is simply absent on the
//! clustered path.
//!
//! That divergence is silent by nature: it appears at a submodule pin bump, in
//! a file nobody in this repo edited, and costs a feature rather than a
//! compile. Nothing else in the suite can see it, so this test watches the
//! relationship between the two construction sites directly.
//!
//! ## What it asserts
//!
//! The builder calls used by upstream are a **subset** of those used by
//! `cluster_manager`, and the surplus is exactly the set of deliberate
//! enterprise additions. Comparing the *set of call names* rather than a
//! snapshot of the source means reformatting, comment edits and reordering do
//! not fire it, while a genuinely new `with_*` does.
//!
//! ## What it deliberately does not assert
//!
//! - **Arguments.** Upstream reads `cli.default_tls_cert`, the clustered path
//!   reads `cli.oss.default_tls_cert`; if upstream changed *what* it passes to
//!   a call both sites make, this stays green. The compiler covers part of that
//!   gap — `cluster_manager` writes `TlsDefaults { .. }` exhaustively, so a new
//!   field there breaks the build — and that division of labour is intended.
//! - **Receivers.** Extraction is scoped to the two regions, not to the manager
//!   binding, so a `with_*` call on some *other* builder inside either region is
//!   attributed to the manager. That direction fails loudly rather than
//!   silently; if a red build here names a call you do not recognise, check
//!   which receiver it belongs to before mirroring it.
//! - **Non-`with_*` additions.** Every `ImposterManager` builder method is
//!   `with_*` today, which is what makes the heuristic sound. An upstream
//!   addition shaped `.enable_x()` or a bare statement would be invisible.
//!
//! ## When it fires
//!
//! It names the call that diverged. Mirror it into `cluster_manager`, or — if
//! the clustered path deliberately does not want it — add it to
//! `INTENTIONALLY_NOT_MIRRORED` with a comment saying why. Do not delete the
//! test: the whole point is that the decision becomes explicit rather than
//! accidental.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Builder calls the clustered path adds on top of upstream's construction.
const ENTERPRISE_ADDITIONS: &[&str] = &[
    // Stamps `Rift-Cluster-*` headers onto every response (issue #9).
    "with_response_decorator",
];

/// Upstream builder calls the clustered path deliberately declines to mirror.
///
/// Empty today, and that is the desired state. An entry here is a standing
/// decision that the clustered path is *better off* without an upstream
/// default, and it must carry the reason. Entries are checked for staleness —
/// see `declined_entries_are_still_real`.
const INTENTIONALLY_NOT_MIRRORED: &[&str] = &[];

const UPSTREAM_SERVER_RS: &str = "vendor/rift/crates/rift-http-proxy/src/server.rs";
const EE_COMPOSE_RS: &str = "crates/rift-ee-server/src/compose.rs";

/// Anchors the extraction. Kept as constants because every panic below quotes
/// them: a maintainer hitting a red build needs to know what was searched for.
const MATCH_ANCHOR: &str = "let manager = match self.manager {";
const NONE_ARM: &str = "None => {";
const CLUSTER_MANAGER_FN: &str = "fn cluster_manager(";

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/rift-ee-server.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root resolves from CARGO_MANIFEST_DIR/../..")
}

/// Read a repo file with its comments blanked out.
///
/// Stripping here rather than at match time keeps brace-walking honest too: a
/// `{` inside a comment would otherwise unbalance the block extraction and
/// silently hand back the wrong region. `strip_comments` preserves byte
/// offsets, so anchors found in the result index the same positions.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    let src = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "read {} : {e}. If the file moved in an upstream reorg, re-ground \
             this test's paths against the new layout rather than deleting it \
             (issue #30).",
            path.display()
        )
    });
    strip_comments(&src)
}

fn to_set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// Blank out `//` and `/* */` comments, preserving byte offsets and string
/// literals.
///
/// Two reasons, both about not going blind. A comment that merely *mentions* a
/// call name would otherwise be counted as a call — on the clustered side that
/// inflates the set and masks a genuine gap, a silent pass in exactly the case
/// this file exists to catch. And a brace inside a comment would unbalance the
/// block walk, handing back a region that is not the one being watched.
///
/// Idempotent, so callers may strip again without harm.
fn strip_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                // Copy the string literal verbatim, honouring escapes.
                out.push('"');
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        out.push_str(&src[i..=i + 1]);
                        i += 2;
                        continue;
                    }
                    let c = bytes[i];
                    out.push(c as char);
                    i += 1;
                    if c == b'"' {
                        break;
                    }
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    out.push(' ');
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let mut depth = 1;
                out.push_str("  ");
                i += 2;
                while i < bytes.len() && depth > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        out.push_str("  ");
                        i += 2;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        out.push_str("  ");
                        i += 2;
                    } else {
                        out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                        i += 1;
                    }
                }
            }
            _ => {
                let ch = src[i..].chars().next().expect("byte index is a boundary");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// The `{ … }` block whose opening brace is at `open`, inclusive of both braces.
fn block_at(src: &str, open: usize, what: &str) -> String {
    assert_eq!(
        src.as_bytes().get(open),
        Some(&b'{'),
        "block_at({what}) was not given an opening brace"
    );
    let mut depth = 0usize;
    for (i, b) in src.as_bytes().iter().enumerate().skip(open) {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return src[open..=i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("braces do not balance while extracting {what} (issue #30)");
}

/// Collect `ImposterManager::<ctor>` and `.with_<call>` names from a snippet.
///
/// Assumes the literal spelling `ImposterManager::` — a type alias or a
/// fully-qualified `<crate::ImposterManager>::` path would not match. That is
/// guarded by `extraction_finds_the_known_construction`, which fails if the
/// known calls stop being seen.
fn builder_calls(snippet: &str) -> BTreeSet<String> {
    let snippet = strip_comments(snippet);
    let mut calls = BTreeSet::new();

    for (idx, _) in snippet.match_indices("ImposterManager::") {
        let rest = &snippet[idx + "ImposterManager::".len()..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            calls.insert(name);
        }
    }

    for (idx, _) in snippet.match_indices(".with_") {
        let rest = &snippet[idx + 1..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            continue;
        }
        // A call, not a field access on something merely named `with_*`.
        // `::` admits the turbofish form `.with_x::<T>(v)`.
        let after = &rest[name.len()..];
        if after.starts_with('(') || after.starts_with("::") {
            calls.insert(name);
        }
    }

    calls
}

/// The body of the `None => { … }` arm of upstream's manager match.
///
/// The search for the arm is bounded to the match's own block. An unbounded
/// scan could latch onto some unrelated `None => {` later in the file and go on
/// passing while watching the wrong code — the failure mode this whole file
/// exists to prevent.
fn upstream_none_arm() -> String {
    let src = read(UPSTREAM_SERVER_RS);
    let anchor = src.find(MATCH_ANCHOR).unwrap_or_else(|| {
        panic!(
            "upstream {UPSTREAM_SERVER_RS} no longer contains `{MATCH_ANCHOR}`. \
             The manager-construction seam this test watches has been \
             restructured -- re-ground the extraction against the new shape \
             rather than deleting the test (issue #30)."
        )
    });

    let match_block = block_at(&src, anchor + MATCH_ANCHOR.len() - 1, "the manager match");
    let arm = match_block.find(NONE_ARM).unwrap_or_else(|| {
        panic!(
            "upstream's manager match no longer has a `{NONE_ARM}` arm. If it \
             became a braceless arm or moved, re-ground this test against the \
             new construction shape (issue #30)."
        )
    });

    block_at(&match_block, arm + NONE_ARM.len() - 1, "the None arm")
}

/// The body of `compose::cluster_manager`.
fn cluster_manager_body() -> String {
    let src = read(EE_COMPOSE_RS);
    let start = src.find(CLUSTER_MANAGER_FN).unwrap_or_else(|| {
        panic!(
            "`{CLUSTER_MANAGER_FN}` is gone from {EE_COMPOSE_RS}. If the \
             clustered path no longer hand-mirrors upstream's construction, \
             this test has served its purpose and can go with it (issue #30)."
        )
    });
    let open = src[start..].find('{').expect("cluster_manager has a body") + start;
    block_at(&src, open, "cluster_manager's body")
}

/// Every builder call upstream makes is also made on the clustered path.
///
/// This is the direction that matters: an upstream addition the clustered path
/// misses is a silently lost feature.
#[test]
fn cluster_manager_mirrors_every_upstream_builder_call() {
    let upstream = builder_calls(&upstream_none_arm());
    let clustered = builder_calls(&cluster_manager_body());

    assert!(
        !upstream.is_empty(),
        "extracted no builder calls from upstream's None arm -- the extraction \
         broke, and a tripwire that extracts nothing passes forever (issue #30)"
    );

    let declined = to_set(INTENTIONALLY_NOT_MIRRORED);
    let missing: Vec<&String> = upstream
        .difference(&clustered)
        .filter(|c| !declined.contains(*c))
        .collect();

    assert!(
        missing.is_empty(),
        "cluster_manager has fallen behind upstream's manager construction: \
         {missing:?} present in ServerBuilder::start's `None` arm but absent \
         from compose::cluster_manager. Injecting a manager replaces upstream's \
         construction wholesale, so the clustered path has silently lost this. \
         Mirror it in cluster_manager, or record it in \
         INTENTIONALLY_NOT_MIRRORED with the reason (issue #30).\n  \
         upstream:  {upstream:?}\n  clustered: {clustered:?}"
    );
}

/// The clustered path's surplus over upstream is exactly the declared set.
///
/// The mirror of the test above: it keeps `ENTERPRISE_ADDITIONS` honest, so
/// what enterprise adds cannot quietly grow either.
#[test]
fn cluster_manager_surplus_is_declared() {
    let upstream = builder_calls(&upstream_none_arm());
    let clustered = builder_calls(&cluster_manager_body());

    let surplus: BTreeSet<String> = clustered.difference(&upstream).cloned().collect();

    assert_eq!(
        surplus,
        to_set(ENTERPRISE_ADDITIONS),
        "the clustered path's additions over upstream have drifted from \
         ENTERPRISE_ADDITIONS. If enterprise added a call, list it there with a \
         comment saying what it is for. If upstream *removed* one it used to \
         make, decide whether the clustered path should drop it too -- either \
         way the difference between the two sites stays a stated decision \
         (issue #30)."
    );
}

/// Entries in the escape hatch still describe reality.
///
/// Without this the hatch rots: an entry whose upstream call disappeared, or
/// which `cluster_manager` started mirroring after all, would sit there
/// suppressing a check nobody remembers waiving.
#[test]
fn declined_entries_are_still_real() {
    let upstream = builder_calls(&upstream_none_arm());
    let clustered = builder_calls(&cluster_manager_body());

    for entry in INTENTIONALLY_NOT_MIRRORED {
        assert!(
            upstream.contains(*entry),
            "stale INTENTIONALLY_NOT_MIRRORED entry `{entry}`: upstream no \
             longer makes this call, so the waiver suppresses nothing -- drop \
             it (issue #30)"
        );
        assert!(
            !clustered.contains(*entry),
            "`{entry}` is waived as not-mirrored, but cluster_manager now calls \
             it -- drop the waiver so the check applies again (issue #30)"
        );
    }
}

/// The extraction itself works -- a tripwire that silently stops seeing its
/// target passes forever and protects nothing.
#[test]
fn extraction_finds_the_known_construction() {
    let upstream = builder_calls(&upstream_none_arm());
    for expected in ["with_datadir", "with_tls_defaults", "with_accept_runtimes"] {
        assert!(
            upstream.contains(expected),
            "upstream extraction no longer sees `{expected}`. Either the \
             extraction broke, or upstream deliberately removed the call -- \
             check which before adjusting this list. Got {upstream:?} (issue #30)"
        );
    }

    let clustered = builder_calls(&cluster_manager_body());
    for expected in [
        "with_datadir",
        "with_tls_defaults",
        "with_accept_runtimes",
        "with_response_decorator",
    ] {
        assert!(
            clustered.contains(expected),
            "cluster_manager extraction no longer sees `{expected}`. Either the \
             extraction broke, or the call was removed -- check which. Got \
             {clustered:?} (issue #30)"
        );
    }
}

/// Reformatting does not fire the tripwire.
///
/// The value of comparing call *names* is that noisy edits — rustfmt reflowing
/// a chain, a comment landing mid-builder, arguments changing — stay silent. If
/// this fails, the extraction has become text-sensitive and will cry wolf on
/// the next bump.
#[test]
fn extraction_ignores_formatting_noise() {
    let terse = builder_calls("ImposterManager::with_datadir(d).with_tls_defaults(t)");
    let reflowed = builder_calls(
        "ImposterManager::with_datadir(
             d,
         )
         .with_tls_defaults(TlsDefaults {
             default_cert,
             default_key,
             allow_self_signed: true,
         })",
    );
    assert_eq!(terse, reflowed);
}

/// A name that only appears in a comment is not counted as a call.
///
/// This direction is the dangerous one: an inflated *clustered* set would mask
/// a real gap and pass.
#[test]
fn extraction_ignores_comments() {
    let calls = builder_calls(
        "ImposterManager::with_datadir(d)
         // TODO: mirror .with_tls_defaults(t) from upstream
         /* and .with_accept_runtimes(r) too */",
    );
    assert_eq!(calls, to_set(&["with_datadir"]), "{calls:?}");
}

/// A `//` sequence inside a string literal does not start a comment.
#[test]
fn extraction_does_not_mistake_urls_for_comments() {
    let calls = builder_calls(
        r#"ImposterManager::with_datadir(d).with_tls_defaults("https://example.com/x")"#,
    );
    assert_eq!(
        calls,
        to_set(&["with_datadir", "with_tls_defaults"]),
        "{calls:?}"
    );
}

/// A field or binding merely *named* `with_*` is not mistaken for a call.
#[test]
fn extraction_ignores_non_calls() {
    let calls = builder_calls("let x = cfg.with_datadir; other.with_tls_defaults(t)");
    assert!(!calls.contains("with_datadir"), "{calls:?}");
    assert!(calls.contains("with_tls_defaults"), "{calls:?}");
}

/// A turbofish call is still a call.
#[test]
fn extraction_sees_turbofish_calls() {
    let calls = builder_calls("ImposterManager::with_datadir(d).with_thing::<Foo>(v)");
    assert!(calls.contains("with_thing"), "{calls:?}");
}

/// The comparison has teeth: a simulated upstream addition is caught.
///
/// Proves the subset assertion fails when it should, without waiting for a real
/// upstream bump to find out.
#[test]
fn a_new_upstream_call_would_be_caught() {
    let upstream = builder_calls(
        "ImposterManager::with_datadir(d)
            .with_tls_defaults(t)
            .with_accept_runtimes(r)
            .with_brand_new_upstream_thing(x)",
    );
    let clustered = builder_calls(&cluster_manager_body());

    let missing: Vec<&String> = upstream.difference(&clustered).collect();
    assert_eq!(
        missing,
        vec!["with_brand_new_upstream_thing"],
        "the subset comparison must flag an upstream call the clustered path \
         does not make"
    );
}
