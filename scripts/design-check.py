#!/usr/bin/env python3
"""Design <-> code coherence check. Deterministic, stdlib only, no LLM.

The repo's design lives in `docs/` and is cited from code, tests and other docs with a small
grammar of tokens (`D-16`, `RFC-002 §4.3`, `ADR-001`, `U-13`, `docs/architecture/06-flow-state.md`).
This script is the mechanical half of keeping the two in sync:

  design-check.py                 full report: unresolved citations, superseded decisions still
                                  cited from code, missing amendment callouts, uncited decisions,
                                  stale documents (index.verified_sha older than the code they
                                  describe)
  design-check.py --diff          the change-time question, in BOTH directions:
                                    code changed  -> which decisions/docs does it cite, and were
                                                     they touched in the same change?
                                    doc changed   -> which code cites the changed sections, so
                                                     the citations get re-verified
  design-check.py --diff <ref>    same, for changes since <ref> (a sha, branch, or a worktree
                                  path -> its merge-base with origin/master)
  design-check.py --strict        exit 1 on errors (CI); warnings never fail
  design-check.py --json          machine-readable report
  design-check.py --mark-verified <doc>...
                                  record HEAD as the sha at which <doc> was re-read against the
                                  code. Records the claim; does not make it true.

Errors (fail --strict)         Warnings (advisory)
  unresolved citation            superseded decision cited from code
  missing amendment callout      active decision never cited from code
  register anchor path missing   active decision with no test pin
  malformed register entry       stale document (per docs/design-index.toml)
                                 pending decision without an open issue

A file may opt out of the citation scan by carrying `design-check: ignore-file` in its first
eight lines (test fixtures, path-filter self-tests with fake paths).

Where things are defined (see docs/decisions/DECISIONS.md "Citation grammar"):
  D-n        docs/decisions/DECISIONS.md          ### D-n — title
  RFC-00N §x docs/rfc/RFC-00N-*.md                ## x. / ### x.y / #### x.y.z headings
  ADR-00N    docs/adr/ADR-00N-*.md
  U-n        docs/architecture/11-upstream-boundary.md
  docs path  the file
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import tomllib
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REGISTER = "docs/decisions/DECISIONS.md"
INDEX = "docs/design-index.toml"
RFC_DIR = "docs/rfc"
ADR_DIR = "docs/adr"
SEAMS_DOC = "docs/architecture/11-upstream-boundary.md"

# Where citations are looked for. vendor/ is upstream and cites nothing of ours.
CODE_ROOTS = ("crates", "tests", "web/src", "scripts", "deploy", ".github")
DOC_ROOTS = ("docs",)
CODE_EXT = {".rs", ".ts", ".tsx", ".py", ".sh", ".yml", ".yaml", ".toml"}
SKIP_DIRS = {"vendor", "target", "node_modules", "graphify-out", ".git", "dist", ".claude"}

DECISION_RE = re.compile(r"(?<![A-Za-z0-9_])D-(\d+)\b")
RFC_RE = re.compile(r"\bRFC-(\d{3})(?:\s+v\d(?:\.\d)?)?(?:\s*§\s*(\d+(?:\.\d+)*))?")
ADR_RE = re.compile(r"\bADR-(\d{3})\b")
SEAM_RE = re.compile(r"(?<![A-Za-z0-9_])U-(\d+)\b")
DOCPATH_RE = re.compile(r"(?<![*?\[{$/])\bdocs/[A-Za-z0-9_./-]+?\.md\b")
# "rift RFC-712" / "upstream RFC-712" are upstream Rift's RFCs, a different numbering space.
UPSTREAM_RFC_RE = re.compile(r"\b(?:rift|upstream)\s+RFC-\d+", re.I)

STATUSES = {"active", "amended", "superseded", "pending"}
TEST_ATTR_RE = re.compile(r"^\s*#\[(?:tokio::)?test(?:\(|\])|^\s*#\[rstest\]|^\s*#\[test_log::test")
TEST_FN_RE = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+([A-Za-z0-9_]+)")
TS_TEST_RE = re.compile(r"^\s*(?:it|test)\(\s*['\"`](.+?)['\"`]")


@dataclass
class Decision:
    id: str
    title: str
    status: str
    line: int
    supersedes: list[str] = field(default_factory=list)
    superseded_by: list[str] = field(default_factory=list)
    amends: list[str] = field(default_factory=list)
    implemented_by: list[str] = field(default_factory=list)
    code: list[str] = field(default_factory=list)
    end_line: int = 0


@dataclass
class Citation:
    kind: str  # decision | rfc | adr | seam | docpath
    key: str  # "D-16" | "RFC-002 §4.3" | "ADR-001" | "U-13" | "docs/..."
    file: str
    line: int
    in_doc: bool
    pin_of: str | None = None  # test name when the citation pins a test


@dataclass
class Finding:
    level: str  # error | warning | info
    code: str
    message: str
    file: str = ""
    line: int = 0

    def render(self) -> str:
        loc = f"{self.file}:{self.line}: " if self.file else ""
        return f"{self.level.upper():7} {self.code:24} {loc}{self.message}"


# --------------------------------------------------------------------------- helpers


def sh(args: list[str], cwd: Path) -> str:
    out = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    return out.stdout


def walk(root: Path, subroots: tuple[str, ...], exts: set[str] | None) -> list[Path]:
    files: list[Path] = []
    for sub in subroots:
        base = root / sub
        if not base.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for name in filenames:
                p = Path(dirpath) / name
                if exts is None or p.suffix in exts:
                    files.append(p)
    return files


def rel(root: Path, p: Path) -> str:
    return p.relative_to(root).as_posix()


# --------------------------------------------------------------------------- register


def parse_register(root: Path) -> tuple[dict[str, Decision], list[Finding]]:
    findings: list[Finding] = []
    path = root / REGISTER
    if not path.exists():
        return {}, [Finding("error", "register-missing", f"{REGISTER} not found")]
    lines = path.read_text(encoding="utf-8").splitlines()
    decisions: dict[str, Decision] = {}
    cur: Decision | None = None
    in_register = False
    for i, line in enumerate(lines, 1):
        if line.strip() == "## Register":
            in_register = True
            continue
        if not in_register:
            continue
        m = re.match(r"^### (~~)?(D-\d+) — (.+?)(~~)?\s*$", line)
        if m:
            if cur:
                cur.end_line = i - 1
            cur = Decision(id=m.group(2), title=m.group(3), status="", line=i)
            if cur.id in decisions:
                findings.append(Finding("error", "register-duplicate", f"{cur.id} defined twice", REGISTER, i))
            decisions[cur.id] = cur
            continue
        if cur is None:
            continue
        fm = re.match(r"^- \*\*([A-Za-z ]+):\*\*\s*(.*)$", line)
        if fm:
            key, val = fm.group(1).strip().lower(), fm.group(2).strip()
            if key == "status":
                cur.status = val.split()[0] if val else ""
            elif key == "supersedes":
                cur.supersedes = DECISION_RE.findall(val) and [f"D-{n}" for n in DECISION_RE.findall(val)]
            elif key == "superseded by":
                cur.superseded_by = [f"D-{n}" for n in DECISION_RE.findall(val)]
            elif key == "amends":
                cur.amends = [f"RFC-{n} §{s}" if s else f"RFC-{n}" for n, s in RFC_RE.findall(val)]
                cur.amends += DOCPATH_RE.findall(val)
            elif key == "implemented by":
                cur.implemented_by = re.findall(r"#\d+(?:\s*\(open[^)]*\))?", val)
            elif key == "code":
                cur.code = [c.strip().strip("`") for c in val.split(",") if c.strip() and c.strip() != "—"]
    if cur:
        cur.end_line = len(lines)

    for d in decisions.values():
        if d.status not in STATUSES:
            findings.append(Finding("error", "register-malformed", f"{d.id} has status '{d.status}' (want one of {sorted(STATUSES)})", REGISTER, d.line))
        struck = lines[d.line - 1].startswith("### ~~")
        if d.status == "superseded" and not struck:
            findings.append(Finding("error", "register-malformed", f"{d.id} is superseded but its title is not struck through", REGISTER, d.line))
        if d.status == "superseded" and not d.superseded_by:
            findings.append(Finding("error", "register-malformed", f"{d.id} is superseded but names no 'Superseded by'", REGISTER, d.line))
        for other in d.supersedes:
            o = decisions.get(other)
            if o is None:
                findings.append(Finding("error", "register-malformed", f"{d.id} supersedes unknown {other}", REGISTER, d.line))
            elif d.id not in o.superseded_by:
                findings.append(Finding("error", "register-malformed", f"{d.id} supersedes {other}, but {other} does not list 'Superseded by: {d.id}'", REGISTER, o.line))
        for other in d.superseded_by:
            if other not in decisions:
                findings.append(Finding("error", "register-malformed", f"{d.id} superseded by unknown {other}", REGISTER, d.line))
        for c in d.code:
            if not (root / c).exists():
                findings.append(Finding("error", "register-anchor-missing", f"{d.id} anchors code at '{c}', which does not exist", REGISTER, d.line))
        if d.status == "pending" and not any("open" in x for x in d.implemented_by):
            findings.append(Finding("warning", "pending-without-issue", f"{d.id} is pending but lists no open issue under 'Implemented by'", REGISTER, d.line))
    return decisions, findings


# --------------------------------------------------------------------------- resolvers


def rfc_sections(root: Path) -> dict[str, tuple[str, dict[str, tuple[int, int]]]]:
    """RFC number -> (path, {section -> (start_line, end_line)})."""
    out: dict[str, tuple[str, dict[str, tuple[int, int]]]] = {}
    d = root / RFC_DIR
    if not d.exists():
        return out
    for p in sorted(d.glob("RFC-*.md")):
        m = re.match(r"RFC-(\d{3})", p.name)
        if not m:
            continue
        lines = p.read_text(encoding="utf-8").splitlines()
        heads: list[tuple[int, int, str]] = []  # (line, level, section)
        for i, line in enumerate(lines, 1):
            hm = re.match(r"^(#{2,5})\s+(\d+(?:\.\d+)*)\.?\s", line)
            if hm:
                heads.append((i, len(hm.group(1)), hm.group(2)))
        sections: dict[str, tuple[int, int]] = {}
        for idx, (ln, lvl, sec) in enumerate(heads):
            end = len(lines)
            for ln2, lvl2, _ in heads[idx + 1 :]:
                if lvl2 <= lvl:
                    end = ln2 - 1
                    break
            sections[sec] = (ln, end)
        out[m.group(1)] = (rel(root, p), sections)
    return out


def adr_files(root: Path) -> dict[str, str]:
    d = root / ADR_DIR
    return {m.group(1): rel(root, p) for p in (d.glob("ADR-*.md") if d.exists() else []) if (m := re.match(r"ADR-(\d{3})", p.name))}


def seam_ids(root: Path) -> set[str]:
    p = root / SEAMS_DOC
    return set(SEAM_RE.findall(p.read_text(encoding="utf-8"))) if p.exists() else set()


# --------------------------------------------------------------------------- citations


def scan_file(root: Path, p: Path, in_doc: bool) -> list[Citation]:
    try:
        text = p.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    f = rel(root, p)
    lines = text.splitlines()
    # A file whose first lines carry `design-check: ignore-file` holds fixtures or fake ids
    # (this script's own tests, a path-filter self-test) and is not a citation source.
    if any("design-check: ignore-file" in ln for ln in lines[:8]):
        return []
    cites: list[Citation] = []
    self_register = f == REGISTER

    # Test pins: a citation in the doc/comment block right above a test attribute pins that test.
    pin_lines: dict[int, str] = {}
    if not in_doc and p.suffix == ".rs":
        for i, line in enumerate(lines):
            if not TEST_ATTR_RE.match(line):
                continue
            name = None
            for j in range(i + 1, min(i + 6, len(lines))):
                fm = TEST_FN_RE.match(lines[j])
                if fm:
                    name = fm.group(1)
                    break
                if TEST_ATTR_RE.match(lines[j]) or lines[j].strip().startswith("#["):
                    continue
            if not name:
                continue
            k = i - 1
            while k >= 0 and (lines[k].strip().startswith("///") or lines[k].strip().startswith("//") or lines[k].strip().startswith("#[")):
                if not lines[k].strip().startswith("#["):
                    pin_lines[k + 1] = name
                k -= 1
    elif not in_doc and p.suffix in {".ts", ".tsx"}:
        for i, line in enumerate(lines):
            tm = TS_TEST_RE.match(line)
            if not tm:
                continue
            k = i - 1
            while k >= 0 and (lines[k].strip().startswith("//") or lines[k].strip().startswith("*") or lines[k].strip().startswith("/*")):
                pin_lines[k + 1] = tm.group(1)
                k -= 1

    for i, line in enumerate(lines, 1):
        if self_register and re.match(r"^### ", line):
            continue  # a definition, not a citation
        # Strip upstream RFC mentions before matching RFC tokens.
        probe = UPSTREAM_RFC_RE.sub("", line)
        pin = pin_lines.get(i)
        for n in DECISION_RE.findall(probe):
            cites.append(Citation("decision", f"D-{n}", f, i, in_doc, pin))
        for n, sec in RFC_RE.findall(probe):
            cites.append(Citation("rfc", f"RFC-{n} §{sec}" if sec else f"RFC-{n}", f, i, in_doc, pin))
        for n in ADR_RE.findall(probe):
            cites.append(Citation("adr", f"ADR-{n}", f, i, in_doc, pin))
        if f != SEAMS_DOC:
            for n in SEAM_RE.findall(probe):
                cites.append(Citation("seam", f"U-{n}", f, i, in_doc, pin))
        for path in DOCPATH_RE.findall(probe):
            cites.append(Citation("docpath", path, f, i, in_doc, pin))
    return cites


def scan_all(root: Path) -> list[Citation]:
    cites: list[Citation] = []
    for p in walk(root, CODE_ROOTS, CODE_EXT):
        cites += scan_file(root, p, in_doc=False)
    for p in walk(root, DOC_ROOTS, {".md"}):
        cites += scan_file(root, p, in_doc=True)
    return cites


# --------------------------------------------------------------------------- checks


def check_resolution(root: Path, cites: list[Citation], decisions: dict[str, Decision]) -> list[Finding]:
    findings: list[Finding] = []
    rfcs = rfc_sections(root)
    adrs = adr_files(root)
    seams = seam_ids(root)
    for c in cites:
        if c.kind == "decision":
            if c.key not in decisions:
                findings.append(Finding("error", "unresolved-decision", f"{c.key} is not in the register", c.file, c.line))
            elif not c.in_doc and decisions[c.key].status == "superseded":
                by = ", ".join(decisions[c.key].superseded_by) or "?"
                findings.append(Finding("warning", "superseded-cited", f"{c.key} is superseded by {by}; code should cite the successor (or say it is history)", c.file, c.line))
        elif c.kind == "rfc":
            m = re.match(r"RFC-(\d{3})(?: §(.+))?$", c.key)
            n, sec = m.group(1), m.group(2)
            if int(n) >= 100:
                continue  # upstream Rift's RFC numbering space (rift RFC-712 …); not ours to resolve
            if n not in rfcs:
                findings.append(Finding("error", "unresolved-rfc", f"{c.key}: no docs/rfc/RFC-{n}-*.md", c.file, c.line))
            elif sec and sec not in rfcs[n][1]:
                findings.append(Finding("error", "unresolved-rfc-section", f"{c.key}: {rfcs[n][0]} has no heading numbered {sec}", c.file, c.line))
        elif c.kind == "adr":
            if c.key.split("-")[1] not in adrs:
                findings.append(Finding("error", "unresolved-adr", f"{c.key}: no {ADR_DIR}/{c.key}-*.md", c.file, c.line))
        elif c.kind == "seam":
            if c.key.split("-")[1] not in seams:
                findings.append(Finding("error", "unresolved-seam", f"{c.key} is not listed in {SEAMS_DOC}", c.file, c.line))
        elif c.kind == "docpath":
            if not (root / c.key).exists() and not (root / "vendor" / "rift" / c.key).exists():
                findings.append(Finding("error", "unresolved-docpath", f"{c.key} does not exist (neither here nor under vendor/rift/)", c.file, c.line))
    return findings


def check_callouts(root: Path, decisions: dict[str, Decision]) -> list[Finding]:
    """Every `Amends: RFC-00N §x.y` needs an `Amended by D-n` callout inside that section."""
    findings: list[Finding] = []
    rfcs = rfc_sections(root)
    for d in decisions.values():
        for target in d.amends:
            m = re.match(r"RFC-(\d{3})(?: §(.+))?$", target)
            if m:
                n, sec = m.group(1), m.group(2)
                if n not in rfcs:
                    findings.append(Finding("error", "amends-unresolved", f"{d.id} amends {target}, which does not exist", REGISTER, d.line))
                    continue
                path, sections = rfcs[n]
                if sec is None:
                    findings.append(Finding("error", "amends-unresolved", f"{d.id} amends {target} without a section — name the section", REGISTER, d.line))
                    continue
                if sec not in sections:
                    findings.append(Finding("error", "amends-unresolved", f"{d.id} amends {target}: no heading numbered {sec} in {path}", REGISTER, d.line))
                    continue
                start, end = sections[sec]
                body = (root / path).read_text(encoding="utf-8").splitlines()[start - 1 : end]
                if not any(re.search(rf"(?:Amended|Superseded|Reversed) by[^\n]*\b{re.escape(d.id)}\b", ln, re.I) for ln in body):
                    findings.append(Finding("error", "amendment-callout-missing", f"{d.id} amends {target}, but {path} §{sec} (lines {start}–{end}) has no '> **Amended by {d.id}**' callout", path, start))
            elif target.startswith("docs/"):
                p = root / target
                if not p.exists():
                    findings.append(Finding("error", "amends-unresolved", f"{d.id} amends {target}, which does not exist", REGISTER, d.line))
                elif not re.search(rf"(?:Amended|Superseded|Reversed) by[^\n]*\b{re.escape(d.id)}\b", p.read_text(encoding="utf-8"), re.I):
                    findings.append(Finding("error", "amendment-callout-missing", f"{d.id} amends {target}, which has no 'Amended by {d.id}' callout", target, 1))
    return findings


def check_coverage(cites: list[Citation], decisions: dict[str, Decision]) -> tuple[list[Finding], dict[str, dict[str, list[str]]]]:
    findings: list[Finding] = []
    cover: dict[str, dict[str, list[str]]] = {d: {"code": [], "pins": []} for d in decisions}
    for c in cites:
        if c.kind != "decision" or c.in_doc or c.key not in cover:
            continue
        cover[c.key]["code"].append(f"{c.file}:{c.line}")
        if c.pin_of:
            cover[c.key]["pins"].append(f"{c.file}::{c.pin_of}")
    for d in decisions.values():
        if d.status in {"active", "amended"}:
            if not cover[d.id]["code"]:
                findings.append(Finding("warning", "decision-uncited", f"{d.id} ({d.title}) is cited from no code — either it is not built, or the code that embodies it does not say so", REGISTER, d.line))
            elif not cover[d.id]["pins"]:
                findings.append(Finding("info", "decision-unpinned", f"{d.id} is cited from code but pinned by no test", REGISTER, d.line))
    return findings, cover


def load_index(root: Path) -> dict[str, dict]:
    p = root / INDEX
    if not p.exists():
        return {}
    with p.open("rb") as fh:
        return tomllib.load(fh)


def changed_since(root: Path, sha: str, paths: list[str]) -> list[str]:
    if not sha:
        return []
    out = sh(["git", "diff", "--name-only", f"{sha}..HEAD", "--"] + paths, root)
    return [ln for ln in out.splitlines() if ln.strip()]


def check_index(root: Path, cites: list[Citation], decisions: dict[str, Decision]) -> list[Finding]:
    findings: list[Finding] = []
    index = load_index(root)
    all_files = [rel(root, p) for p in walk(root, CODE_ROOTS, CODE_EXT)]
    # doc -> files that cite it (by path, by RFC/ADR number, or by a decision that lists it under Amends)
    citing: dict[str, set[str]] = defaultdict(set)
    rfc_by_num = {n: path for n, (path, _) in rfc_sections(root).items()}
    adr_by_num = adr_files(root)
    for c in cites:
        if c.in_doc:
            continue
        if c.kind == "docpath":
            citing[c.key].add(c.file)
        elif c.kind == "rfc":
            n = re.match(r"RFC-(\d{3})", c.key).group(1)
            if n in rfc_by_num:
                citing[rfc_by_num[n]].add(c.file)
        elif c.kind == "adr":
            n = c.key.split("-")[1]
            if n in adr_by_num:
                citing[adr_by_num[n]].add(c.file)
        elif c.kind == "decision":
            citing[REGISTER].add(c.file)
    for doc, meta in index.items():
        if not (root / doc).exists():
            findings.append(Finding("error", "index-doc-missing", f"{doc} is in {INDEX} but does not exist", INDEX, 1))
            continue
        sha = (meta.get("verified_sha") or "").strip()
        if not sha:
            findings.append(Finding("info", "doc-unverified", f"{doc} has never been verified against the code (verified_sha empty)", doc, 1))
            continue
        globs = meta.get("code") or []
        described = [f for f in all_files if any(fnmatch.fnmatch(f, g) or fnmatch.fnmatch(f, g.replace("**", "*")) for g in globs)]
        watch = sorted(set(described) | citing.get(doc, set()) | {doc})
        changed = changed_since(root, sha, watch)
        if not changed:
            continue
        doc_itself = doc in changed
        code_changed = [f for f in changed if f != doc]
        if code_changed and not doc_itself:
            sample = ", ".join(code_changed[:4]) + (f" (+{len(code_changed) - 4})" if len(code_changed) > 4 else "")
            findings.append(Finding("warning", "doc-stale", f"{doc} verified at {sha[:9]}; code it describes changed since: {sample}", doc, 1))
        elif doc_itself and not code_changed:
            findings.append(Finding("info", "doc-edited-since-verify", f"{doc} was edited after its last verification at {sha[:9]} — re-verify or --mark-verified", doc, 1))
        else:
            findings.append(Finding("warning", "doc-stale", f"{doc} and the code it describes both changed since {sha[:9]}; re-verify", doc, 1))
    return findings


# --------------------------------------------------------------------------- --diff


def resolve_diff_base(root: Path, ref: str | None) -> tuple[list[str], str, dict[str, set[int]]]:
    """Return (changed files, description, changed line numbers per file)."""
    if ref and Path(ref).exists() and (Path(ref) / ".git").exists():
        wt = Path(ref).resolve()
        base = sh(["git", "merge-base", "origin/master", "HEAD"], wt).strip() or "origin/master"
        names = sh(["git", "diff", "--name-only", f"{base}...HEAD"], wt)
        hunks = sh(["git", "diff", "-U0", f"{base}...HEAD"], wt)
        desc = f"{wt.name} vs merge-base {base[:9]}"
    elif ref:
        names = sh(["git", "diff", "--name-only", f"{ref}...HEAD"], root)
        hunks = sh(["git", "diff", "-U0", f"{ref}...HEAD"], root)
        if not names.strip():
            names = sh(["git", "diff", "--name-only", ref], root)
            hunks = sh(["git", "diff", "-U0", ref], root)
        desc = f"changes since {ref}"
    else:
        names = sh(["git", "diff", "--name-only", "HEAD"], root)
        hunks = sh(["git", "diff", "-U0", "HEAD"], root)
        desc = "uncommitted changes vs HEAD"
    files = [ln for ln in names.splitlines() if ln.strip()]
    changed_lines: dict[str, set[int]] = defaultdict(set)
    cur = None
    for ln in hunks.splitlines():
        if ln.startswith("+++ b/"):
            cur = ln[6:]
        elif ln.startswith("@@") and cur:
            m = re.search(r"\+(\d+)(?:,(\d+))?", ln)
            if m:
                start, count = int(m.group(1)), int(m.group(2) or 1)
                changed_lines[cur].update(range(start, start + max(count, 1)))
    return files, desc, changed_lines


def diff_report(root: Path, ref: str | None, decisions: dict[str, Decision], cites: list[Citation]) -> tuple[list[str], dict]:
    files, desc, changed_lines = resolve_diff_base(root, ref)
    files_set = set(files)
    out: list[str] = [f"design-check --diff — {desc}", f"  {len(files)} changed file(s)", ""]
    payload: dict = {"changed": files, "code_to_design": {}, "design_to_code": {}}
    if not files:
        out.append("  nothing changed")
        return out, payload

    rfc_by_num = {n: path for n, (path, _) in rfc_sections(root).items()}
    adr_by_num = adr_files(root)

    # Direction 1 — code changed: which design does it cite, and was that design touched too?
    by_doc: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))  # doc -> key -> files
    for c in cites:
        if c.in_doc or c.file not in files_set:
            continue
        if c.kind == "decision":
            by_doc[REGISTER][c.key].add(c.file)
        elif c.kind == "rfc":
            n = re.match(r"RFC-(\d{3})", c.key).group(1)
            if n in rfc_by_num:
                by_doc[rfc_by_num[n]][c.key].add(c.file)
        elif c.kind == "adr":
            n = c.key.split("-")[1]
            if n in adr_by_num:
                by_doc[adr_by_num[n]][c.key].add(c.file)
        elif c.kind == "docpath":
            by_doc[c.key][c.key].add(c.file)
    # Also: changed code that the index says a document describes.
    index = load_index(root)
    for doc, meta in index.items():
        for g in meta.get("code") or []:
            for f in files:
                if f.startswith("docs/"):
                    continue
                if fnmatch.fnmatch(f, g) or fnmatch.fnmatch(f, g.replace("**", "*")):
                    by_doc[doc]["(described by index)"].add(f)

    out.append("CODE → DESIGN: changed code cites or is described by these documents")
    if not by_doc:
        out.append("  (none — the changed code cites no design document)")
    for doc in sorted(by_doc):
        touched = doc in files_set
        mark = "also changed ✓" if touched else "NOT changed — confirm it still holds, or amend it"
        out.append(f"  {doc}  [{mark}]")
        for key in sorted(by_doc[doc]):
            fs = sorted(by_doc[doc][key])
            extra = ""
            if key.startswith("D-") and key in decisions:
                d = decisions[key]
                extra = f" — {d.title} [{d.status}]"
            out.append(f"      {key}{extra}: {', '.join(fs[:3])}{' …' if len(fs) > 3 else ''}")
        payload["code_to_design"][doc] = {"touched": touched, "citations": {k: sorted(v) for k, v in by_doc[doc].items()}}
    out.append("")

    # Direction 2 — design changed: which code cites the changed parts?
    out.append("DESIGN → CODE: changed documents, and the code that cites them")
    changed_docs = [f for f in files if f.startswith("docs/") and f.endswith(".md")]
    if not changed_docs:
        out.append("  (no design document changed)")
    citers: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for c in cites:
        if c.in_doc:
            continue
        if c.kind == "decision" and REGISTER in files_set:
            d = decisions.get(c.key)
            if d and changed_lines.get(REGISTER) and any(d.line <= ln <= (d.end_line or d.line) for ln in changed_lines[REGISTER]):
                citers[REGISTER][c.key].add(f"{c.file}:{c.line}")
        elif c.kind == "rfc":
            n = re.match(r"RFC-(\d{3})", c.key).group(1)
            path = rfc_by_num.get(n)
            if path in files_set:
                # Only sections whose lines changed, when the citation names a section.
                sec = c.key.split("§")[1] if "§" in c.key else None
                sections = rfc_sections(root)[n][1]
                if sec and sec in sections and changed_lines.get(path):
                    s, e = sections[sec]
                    if not any(s <= ln <= e for ln in changed_lines[path]):
                        continue
                citers[path][c.key].add(f"{c.file}:{c.line}")
        elif c.kind == "adr":
            path = adr_by_num.get(c.key.split("-")[1])
            if path in files_set:
                citers[path][c.key].add(f"{c.file}:{c.line}")
        elif c.kind == "docpath" and c.key in files_set:
            citers[c.key][c.key].add(f"{c.file}:{c.line}")
    for doc in changed_docs:
        out.append(f"  {doc}")
        if doc not in citers:
            out.append("      cited by no code (explanatory change, or the code that embodies it does not cite it)")
            continue
        for key in sorted(citers[doc]):
            fs = sorted(citers[doc][key])
            out.append(f"      {key}: {len(fs)} citation(s) — {', '.join(fs[:4])}{' …' if len(fs) > 4 else ''}")
        payload["design_to_code"][doc] = {k: sorted(v) for k, v in citers[doc].items()}
    out.append("")
    out.append("A decision reached while making this change is not made until it is a D-n in " + REGISTER + ".")
    return out, payload


# --------------------------------------------------------------------------- --mark-verified


def mark_verified(root: Path, docs: list[str]) -> list[str]:
    head = sh(["git", "rev-parse", "--short=9", "HEAD"], root).strip()
    today = sh(["git", "log", "-1", "--format=%cs", "HEAD"], root).strip()
    p = root / INDEX
    text = p.read_text(encoding="utf-8")
    msgs = []
    for doc in docs:
        header = f'["{doc}"]'
        if header not in text:
            msgs.append(f"{doc}: not in {INDEX} — add a table for it first")
            continue
        start = text.index(header)
        nxt = text.find("\n[\"", start + 1)
        block = text[start : nxt if nxt != -1 else len(text)]
        new = re.sub(r'verified_sha = "[^"]*"', f'verified_sha = "{head}"', block, count=1)
        new = re.sub(r'verified_on = "[^"]*"', f'verified_on = "{today}"', new, count=1)
        text = text[:start] + new + text[start + len(block) :]
        msgs.append(f"{doc}: verified_sha = {head} ({today})")
    p.write_text(text, encoding="utf-8")
    return msgs


# --------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None, help="repo root (default: git toplevel of cwd)")
    ap.add_argument("--diff", nargs="?", const="", default=None, metavar="REF", help="change-time report (optional ref / worktree path)")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any error")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--mark-verified", nargs="+", metavar="DOC")
    ap.add_argument("--quiet", action="store_true", help="only errors and warnings")
    args = ap.parse_args(argv)

    root = Path(args.root).resolve() if args.root else Path(sh(["git", "rev-parse", "--show-toplevel"], Path.cwd()).strip() or ".").resolve()

    if args.mark_verified:
        for m in mark_verified(root, args.mark_verified):
            print(m)
        return 0

    decisions, findings = parse_register(root)
    cites = scan_all(root)

    if args.diff is not None:
        lines, payload = diff_report(root, args.diff or None, decisions, cites)
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print("\n".join(lines))
        return 0

    findings += check_resolution(root, cites, decisions)
    findings += check_callouts(root, decisions)
    cov_findings, cover = check_coverage(cites, decisions)
    findings += cov_findings
    findings += check_index(root, cites, decisions)

    order = {"error": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: (order[f.level], f.code, f.file, f.line))
    errors = [f for f in findings if f.level == "error"]
    warnings = [f for f in findings if f.level == "warning"]
    infos = [f for f in findings if f.level == "info"]

    if args.json:
        print(json.dumps({
            "errors": [f.__dict__ for f in errors],
            "warnings": [f.__dict__ for f in warnings],
            "info": [f.__dict__ for f in infos],
            "decisions": {d.id: {"status": d.status, "title": d.title, **cover.get(d.id, {})} for d in decisions.values()},
            "citations": len(cites),
        }, indent=2))
    else:
        n_code = sum(1 for c in cites if not c.in_doc)
        print(f"design-check — {len(decisions)} decisions, {len(cites)} citations ({n_code} from code), root {root}")
        print()
        for f in errors + warnings + ([] if args.quiet else infos):
            print(f.render())
        print()
        print(f"{len(errors)} error(s), {len(warnings)} warning(s), {len(infos)} info")
        if not args.quiet:
            print()
            print("decision coverage (code citations / test pins):")
            for d in decisions.values():
                c = cover[d.id]
                print(f"  {d.id:5} {d.status:10} {len(c['code']):3} / {len(c['pins']):2}   {d.title[:70]}")
    return 1 if (args.strict and errors) else 0


if __name__ == "__main__":
    sys.exit(main())
