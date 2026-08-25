# Keeping design and code in sync — the practice

This repository has one normative home for design: **`docs/`**. Everything else that talks about
the design — GitHub issues, the Obsidian vault, agent memory, chat transcripts — is either a work
item, a derivative, or a lesson, and each of those must *point at* `docs/`, never restate it.
This document says what lives where, what "in sync" means mechanically, and what to do at the
three moments where drift is born: before a change, during it, and after.

It is written to be copied. Nothing below is specific to Rift except the file names; the
practice is in [`docs/process/design-code-sync.md`](design-code-sync.md) of this repo and, in a
project-agnostic form, in the `design-sync` skill.

## 1. Where things live (the authority table)

| Artifact | Role | Authoritative for | Cited as |
|---|---|---|---|
| `docs/decisions/DECISIONS.md` | **decision register** | *what was decided*, its status, what it amends/supersedes | `D-n` |
| `docs/rfc/RFC-00N-*.md` | proposal → accepted spec | the mechanism as specified, **until** a `D-n` amends a section (callout required) | `RFC-00N §x.y` |
| `docs/adr/ADR-00N-*.md` | the argument behind a large decision | the reasoning; the decision itself is a `D-n` | `ADR-00N` |
| `docs/architecture/NN-*.md` | explanatory guide | how it fits together; *must not contradict* a decision | `docs/architecture/NN-….md` |
| `docs/architecture/11-upstream-boundary.md` | seam table | the upstream seams | `U-n` |
| `docs/design-index.toml` | verification index | when each doc was last verified against code | — |
| GitHub issues / epics | work items | acceptance criteria *until merged*; then the tests that cite the issue are the record | `#n` |
| Obsidian vault | analyses, drafts, Q&A | nothing normative — every file carries a banner saying so | — |
| Agent auto-memory | process lessons, how-tos, user preferences | nothing about the design — design facts go to the register | — |

**One rule generates the table:** if two artifacts can disagree about the design, exactly one of
them is allowed to be right, and it is the one higher in this list. A decision reached anywhere
lower — in an issue thread, a review, a session — *is not made* until it is a `D-n`.

## 2. What "in sync" means, mechanically

`scripts/design-check.py` (stdlib Python, no network, no LLM) checks what can be checked:

| Check | Level | Meaning |
|---|---|---|
| every `D-n`, `RFC-00N §x.y`, `ADR-00N`, `U-n`, `docs/…md` cited anywhere resolves | error | a citation that resolves to nothing is a claim with no referent |
| every `Amends:` in the register has an `Amended by D-n` callout in the amended section | error | the spec must announce, *at the section a reader lands on*, that it no longer holds |
| register entries are well-formed (status, supersedes ↔ superseded-by, code anchors exist) | error | |
| code cites a `superseded` decision | warning | the code may still do the old thing — look |
| an active decision is cited from no code | warning | either unbuilt, or built without saying so |
| an active decision is pinned by no test | info | nothing would go red if it were broken |
| a doc's code (per the index) changed after `verified_sha` | warning | the doc may now be lying |
| `--diff` | report | see §3 |

CI runs `--strict`: errors fail, warnings are printed. Warnings are the backlog, not noise —
the count should trend down.

**What it does not check, and what covers that instead:** whether a chapter's *prose* is true.
That needs a person (or an agent) re-reading the chapter against the code — after which they run
`--mark-verified <doc>` so the index records the sha at which that was done. The tool records
the claim; it does not make it true.

## 3. The three moments

### Before a change — read the design you are about to touch

```sh
scripts/design-check.py --diff            # or --diff <worktree-path> once you have one
```

Direction **code → design** lists every decision and section the files you are changing cite,
and whether the index says a document describes them. Read those before writing code. If the
issue's premise contradicts a `D-n`, stop: either the issue is wrong (fix the issue text) or the
decision is (amend it — in the PR, not in your head). Issues have been wrong at the intent
level while every fact in them checked out (#359, #366); the register is what you check them
against.

### During — a decision reached is a decision recorded

Any of these means a register edit in the *same PR*:

- you chose between real alternatives, and the next person could reasonably choose otherwise;
- the code now does something a doc says it does not (amendment + callout);
- a reviewer or the user ruled on a should-question;
- you found the design was wrong and built the right thing (supersede, do not silently diverge).

Cite the decision from the code that embodies it, and pin it with a test whose doc comment
states the claim the test discriminates (`/// Pins D-19: …`). A `D-n` with no citation is a
decision nobody can find from the code; a `D-n` with no pin is one nothing enforces.

### After — the PR answers the design question explicitly

The PR body carries one of:

- `Design: unchanged — <docs the diff cites>, confirmed still accurate`
- `Design: amended — D-n (<one line>)`
- `Design: n/a — <why no design doc describes this change>`

and `design-check.py --diff <worktree>` output is what you confirm against, not memory. Silence
is not an option: every PR says which it is. After merge, the doc(s) you re-read get
`--mark-verified`.

## 4. Session and tool boundaries (why this exists)

The repo is worked on by people and agents across sessions, worktrees and tools, and a session
boundary leaves no trace in the artifact being read. The failure this practice targets is a
decision that lived in a context that no longer exists — a chat, a memory file, a vault note, an
issue comment — being re-derived differently next time. Two rules follow:

- **Memory vs. intent.** Before saving anything to agent memory, ask: *would a fresh agent, in
  any tool, next session, need this to build the system right?* If yes, it is intent — it goes
  in the register (or an amendment), and memory keeps at most a pointer. Memory is for how the
  *work* goes (build traps, review habits, user preferences), not what the *system* is.
- **Vault vs. repo.** The vault is where analysis happens; its conclusions are promoted into the
  register or a doc, and the vault file gets a banner naming what it fed. A vault file that
  mirrors a repo file is replaced by a pointer — a mirror is a second source of truth with a
  delay.

## 5. Bootstrapping this in another repository

1. Create `docs/decisions/DECISIONS.md` from this repo's header (the "How to read an entry" and
   "Citation grammar" sections are the contract). Seed it by *moving* every decision log you
   already have — an ADR's "decisions" list, an RFC appendix — into it, leaving pointers behind.
   Then read the issue tracker for closed-as-not-planned and "we chose X" threads; those are
   decisions with no home.
2. Copy `scripts/design-check.py`; adjust the constants at the top (`CODE_ROOTS`, `RFC_DIR`,
   heading regexes) to the repo's layout. Run it. The first run's error list *is* the drift
   inventory — fix the real ones, tune the resolver for the false positives.
3. Create `docs/design-index.toml` with one table per design doc and empty `verified_sha`;
   verify docs as you actually re-read them, never in bulk.
4. Add the CI job (`--strict`), the CLAUDE.md block (authority table + when to run), and the PR
   body line.
5. Wire the `--diff` call into whatever implements issues (`/fix-issue` Phase 2.5 and the ship
   report; `/ship-issues` 1c-docs) — as a conditional step that fires only when the repo declares
   `design-check` in its CLAUDE.md, so the skill stays generic.

## 6. What this deliberately is not

- **Not spec-first.** Code can and does correct the design; the requirement is that the
  correction is *recorded as a decision*, not that the spec is rewritten before the code exists.
- **Not a doc-generation pipeline.** Nothing here writes prose from code. Generated
  descriptions of what the code does are a third source of truth, and drift twice as fast.
- **Not a gate on prose accuracy.** The check gates structure (citations, callouts, register
  shape). Whether a chapter is *true* is a reading, recorded in the index — and the index says
  honestly when nobody has done that reading.
