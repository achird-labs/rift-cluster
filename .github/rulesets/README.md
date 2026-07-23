# Branch rulesets

`master.json` is the ruleset protecting the default branch. It lives here, in
the tree, rather than only in the repo settings UI so that a change to how this
repository can be merged into is reviewed the same way a change to how it builds
is — a diff, in a PR, with a reason attached.

Apply it with:

```sh
scripts/apply-ruleset.sh              # create or update from master.json
scripts/apply-ruleset.sh --dry-run    # show what would change
```

The script is idempotent: it looks the ruleset up by name and POSTs or PUTs
accordingly, so re-running after an edit is the normal way to change the live
configuration. Editing this file without running it changes nothing; editing the
live ruleset in the UI without updating this file makes the two drift silently,
which is exactly what committing it is meant to prevent.

## What it enforces, and why

**Every change reaches `master` through a pull request, and its checks must
finish first.** This is issue #104: PR #101 was merged while its `cluster-smoke`
job was still `in_progress`, so the container chaos tier never validated the
change that landed. Nothing prevented it — the repo had no rulesets and no
branch protection, so `gh pr merge` succeeded regardless of whether checks had
completed. A check that is *running* is not a check that *passed*.

**Zero required approvals.** The gate being bought here is "the checks
finished", not "a human looked". On a solo-maintainer repository a review
requirement would be self-approval theatre, and it is the thing most likely to
push someone toward an admin override — which would take the status-check gate
down with it.

**`strict_required_status_checks_policy: false`** — branches are *not* required
to be up to date with `master` before merging. Requiring that would force a
~25-minute `cluster-smoke` re-run for every position in a merge queue, turning a
correctness gate into a serialization point. The tier is there to catch a broken
cluster, not to prove a linear history.

**`bypass_actors` is empty — deliberately, including for admins.** Unlike legacy
branch protection, a ruleset does not exempt administrators unless they are
listed here. Leaving the list empty is the point: the merge that motivated this
issue was made by an admin, and an automated agent running with admin
credentials is precisely the actor that needs to be told "not yet". A bypass
list containing the only maintainer would restore the status quo it exists to
change.

The cost is real and worth stating: a genuine emergency needs the ruleset
disabled or edited first. That is a deliberate speed bump, not an oversight.

**`non_fast_forward` and `deletion`.** Requiring a PR while leaving `master`
force-pushable is theatre — the rule is only as strong as the cheapest way
around it.

## The required checks

`build`, `public-api`, `cluster-smoke` — the job names in
`.github/workflows/ci.yml`, which are what GitHub reports as check contexts.

`cluster-smoke` is safe to require even though it is the expensive one. The job
runs on **every** pull request; only its heavy steps are gated behind the path
filter (`scripts/cluster-smoke-paths.sh`), so a docs-only PR completes it in
seconds while a cluster-touching one waits for the full tier. Requiring it does
not serialize unrelated work.

One caveat worth knowing: on a **push** to `master`, `cluster-smoke` is *skipped*
(the job carries `if: github.event_name == 'pull_request'`). That is unchanged
and correct — the gate is a PR gate. Required checks are evaluated on the pull
request, where the job really runs.
