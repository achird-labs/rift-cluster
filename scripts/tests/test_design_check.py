"""Behavioural tests for scripts/design-check.py against a synthetic repo.

Run: python3 -m unittest scripts/tests/test_design_check.py

design-check: ignore-file (the fixtures below cite ids that exist only in the synthetic repo)
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "design-check.py"

spec = importlib.util.spec_from_file_location("design_check", SCRIPT)
dc = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules["design_check"] = dc  # dataclasses in 3.14 look the module up by name
spec.loader.exec_module(dc)


REGISTER = """# Decision register

## Register

### ~~D-1 — Old way~~
- **Status:** superseded
- **Decided:** 2026-01-01 · RFC-001
- **Superseded by:** D-2

Old text.

### D-2 — New way
- **Status:** active
- **Decided:** 2026-02-01 · ADR-001
- **Supersedes:** D-1
- **Amends:** RFC-001 §2.1
- **Code:** crates/x/src/new.rs

New text.

### D-3 — Unbuilt thing
- **Status:** pending
- **Decided:** 2026-03-01 · #10
- **Implemented by:** #10 (open)

Pending text.
"""

RFC = """# RFC-001 — Test

## 1. Summary

hello

## 2. Design

### 2.1 The part D-2 changed

> **Amended by D-2** (2026-02-01): see the register.

body

### 2.2 Untouched

body
"""

SEAMS = "# Ch.11\n\n| U-1 | thing |\n"

INDEX = """["docs/rfc/RFC-001-test.md"]
status = "current"
verified_sha = ""
verified_on = ""
code = ["crates/x/src/**"]
"""


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True).stdout


class Fixture:
    def __init__(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "docs/decisions").mkdir(parents=True)
        (self.root / "docs/rfc").mkdir()
        (self.root / "docs/adr").mkdir()
        (self.root / "docs/architecture").mkdir()
        (self.root / "crates/x/src").mkdir(parents=True)
        self.write("docs/decisions/DECISIONS.md", REGISTER)
        self.write("docs/rfc/RFC-001-test.md", RFC)
        self.write("docs/adr/ADR-001-thing.md", "# ADR-001\n")
        self.write("docs/architecture/11-upstream-boundary.md", SEAMS)
        self.write("docs/design-index.toml", INDEX)
        self.write("crates/x/src/new.rs", "// D-2 lives here\n")
        self.write(
            "crates/x/src/lib.rs",
            "/// Pins D-2: the new way holds.\n#[test]\nfn new_way_holds() {}\n\n// cites RFC-001 §2.2 and ADR-001 and U-1\n",
        )
        git(self.root, "init", "-q")
        git(self.root, "-c", "user.email=t@t", "-c", "user.name=t", "add", ".")
        git(self.root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")

    def write(self, rel: str, text: str) -> None:
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def run(self, *args: str) -> tuple[int, str]:
        out = subprocess.run([sys.executable, str(SCRIPT), "--root", str(self.root), *args], capture_output=True, text=True)
        return out.returncode, out.stdout + out.stderr


class RegisterParsing(unittest.TestCase):
    def test_parses_ids_status_and_links(self):
        f = Fixture()
        decisions, findings = dc.parse_register(f.root)
        self.assertEqual(sorted(decisions), ["D-1", "D-2", "D-3"])
        self.assertEqual(decisions["D-1"].status, "superseded")
        self.assertEqual(decisions["D-1"].superseded_by, ["D-2"])
        self.assertEqual(decisions["D-2"].supersedes, ["D-1"])
        self.assertEqual(decisions["D-2"].amends, ["RFC-001 §2.1"])
        self.assertEqual(decisions["D-2"].code, ["crates/x/src/new.rs"])
        self.assertEqual([x for x in findings if x.level == "error"], [])

    def test_superseded_without_strike_or_backlink_is_malformed(self):
        f = Fixture()
        f.write("docs/decisions/DECISIONS.md", REGISTER.replace("### ~~D-1 — Old way~~", "### D-1 — Old way"))
        _, findings = dc.parse_register(f.root)
        self.assertTrue(any(x.code == "register-malformed" and "struck" in x.message for x in findings))
        f.write("docs/decisions/DECISIONS.md", REGISTER.replace("- **Superseded by:** D-2\n", ""))
        _, findings = dc.parse_register(f.root)
        self.assertTrue(any("does not list 'Superseded by: D-2'" in x.message for x in findings))

    def test_missing_code_anchor_is_an_error(self):
        f = Fixture()
        (f.root / "crates/x/src/new.rs").unlink()
        _, findings = dc.parse_register(f.root)
        self.assertTrue(any(x.code == "register-anchor-missing" for x in findings))

    def test_pending_needs_an_open_issue(self):
        f = Fixture()
        f.write("docs/decisions/DECISIONS.md", REGISTER.replace("- **Implemented by:** #10 (open)\n", ""))
        _, findings = dc.parse_register(f.root)
        self.assertTrue(any(x.code == "pending-without-issue" for x in findings))


class Resolution(unittest.TestCase):
    def test_clean_fixture_has_no_errors(self):
        f = Fixture()
        code, out = f.run("--strict")
        self.assertEqual(code, 0, out)
        self.assertIn("0 error(s)", out)

    def test_unknown_decision_rfc_section_adr_seam_and_path(self):
        f = Fixture()
        f.write("crates/x/src/bad.rs", "// D-99, RFC-001 §9.9, ADR-009, U-77, docs/nope.md\n")
        code, out = f.run("--strict")
        self.assertEqual(code, 1)
        for c in ("unresolved-decision", "unresolved-rfc-section", "unresolved-adr", "unresolved-seam", "unresolved-docpath"):
            self.assertIn(c, out, c)

    def test_upstream_rfc_numbers_are_not_ours(self):
        f = Fixture()
        f.write("crates/x/src/up.rs", "// mirrors rift RFC-712 and bare RFC-712\n")
        code, out = f.run("--strict")
        self.assertEqual(code, 0, out)

    def test_docpath_may_resolve_under_vendor(self):
        f = Fixture()
        f.write("vendor/rift/docs/upstream.md", "x")
        f.write("crates/x/src/v.rs", "// see docs/upstream.md\n")
        code, out = f.run("--strict")
        self.assertEqual(code, 0, out)

    def test_superseded_cited_from_code_is_a_warning_not_from_docs(self):
        f = Fixture()
        f.write("crates/x/src/old.rs", "// still does D-1\n")
        f.write("docs/architecture/01-x.md", "history: D-1 was replaced\n")
        code, out = f.run("--strict")
        self.assertEqual(code, 0)
        self.assertEqual(out.count("superseded-cited"), 1)
        self.assertIn("crates/x/src/old.rs", out)


class Callouts(unittest.TestCase):
    def test_missing_callout_is_an_error(self):
        f = Fixture()
        f.write("docs/rfc/RFC-001-test.md", RFC.replace("> **Amended by D-2** (2026-02-01): see the register.\n\n", ""))
        code, out = f.run("--strict")
        self.assertEqual(code, 1)
        self.assertIn("amendment-callout-missing", out)
        self.assertIn("§2.1", out)

    def test_superseded_by_banner_counts(self):
        f = Fixture()
        f.write("docs/rfc/RFC-001-test.md", RFC.replace("> **Amended by D-2**", "> ⚠️ **Superseded by ADR-001, decision D-2**"))
        code, out = f.run("--strict")
        self.assertEqual(code, 0, out)

    def test_callout_in_the_wrong_section_does_not_count(self):
        f = Fixture()
        moved = RFC.replace("> **Amended by D-2** (2026-02-01): see the register.\n\n", "")
        moved = moved.replace("### 2.2 Untouched\n\n", "### 2.2 Untouched\n\n> **Amended by D-2**\n\n")
        f.write("docs/rfc/RFC-001-test.md", moved)
        code, out = f.run("--strict")
        self.assertEqual(code, 1)
        self.assertIn("amendment-callout-missing", out)


class Coverage(unittest.TestCase):
    def test_pins_and_citations_are_counted(self):
        f = Fixture()
        code, out = f.run("--json")
        import json

        payload = json.loads(out)
        self.assertEqual(payload["decisions"]["D-2"]["pins"], ["crates/x/src/lib.rs::new_way_holds"])
        self.assertEqual(len(payload["decisions"]["D-2"]["code"]), 2)

    def test_uncited_active_decision_warns(self):
        f = Fixture()
        f.write("crates/x/src/new.rs", "// nothing\n")
        f.write("crates/x/src/lib.rs", "// RFC-001 §2.2\n")
        _, out = f.run()
        self.assertIn("decision-uncited", out)
        self.assertIn("D-2", out)


class Index(unittest.TestCase):
    def test_unverified_is_info_and_stale_is_warning(self):
        f = Fixture()
        _, out = f.run()
        self.assertIn("doc-unverified", out)
        # mark verified, then change code the doc describes
        f.run("--mark-verified", "docs/rfc/RFC-001-test.md")
        self.assertIn("verified_sha = ", (f.root / "docs/design-index.toml").read_text())
        _, out = f.run()
        self.assertNotIn("doc-stale", out)
        f.write("crates/x/src/new.rs", "// D-2 changed\n")
        git(f.root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-am", "change")
        _, out = f.run()
        self.assertIn("doc-stale", out)
        self.assertIn("crates/x/src/new.rs", out)


class Diff(unittest.TestCase):
    def test_code_to_design_flags_untouched_docs(self):
        f = Fixture()
        f.write("crates/x/src/new.rs", "// D-2 and RFC-001 §2.2 — edited\n")
        _, out = f.run("--diff")
        self.assertIn("CODE → DESIGN", out)
        self.assertIn("docs/decisions/DECISIONS.md  [NOT changed", out)
        self.assertIn("docs/rfc/RFC-001-test.md  [NOT changed", out)
        self.assertIn("D-2 — New way [active]", out)

    def test_design_to_code_lists_citers_of_changed_section_only(self):
        f = Fixture()
        f.write("docs/rfc/RFC-001-test.md", RFC.replace("### 2.2 Untouched\n\nbody", "### 2.2 Untouched\n\nbody changed"))
        _, out = f.run("--diff")
        self.assertIn("DESIGN → CODE", out)
        self.assertIn("RFC-001 §2.2: 1 citation(s)", out)
        self.assertNotIn("RFC-001 §2.1", out)

    def test_register_edit_maps_to_citing_code(self):
        f = Fixture()
        f.write("docs/decisions/DECISIONS.md", REGISTER.replace("New text.", "New text, clarified."))
        _, out = f.run("--diff")
        self.assertIn("D-2: 2 citation(s)", out)


if __name__ == "__main__":
    unittest.main()
