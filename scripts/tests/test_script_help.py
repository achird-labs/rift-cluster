"""The human-run scripts answer `-h`/`--help` with their usage, and do nothing else (#379).

Run: python3 -m unittest scripts.tests.test_script_help -v

Each script is run from a copy in an empty temporary directory, with no repository, submodule or
build around it. A script that ran past its help check would reach its first side effect there —
`cd` into a `vendor/rift` that does not exist, a `git` command outside a repository, a missing
binary — and exit non-zero, so "exit 0 and usage on stdout" is only reachable by stopping first.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent

# Script → a line its usage must contain. Each is the invocation line the header already documents,
# so the assertion is that the header was printed, not merely that something was.
HELP = {
    "upstream-pr.sh": 'scripts/upstream-pr.sh <branch> "<PR title>"',
    "sync-upstream.sh": "Sync the vendor/rift submodule to the latest public Rift master",
    "e2e-console.sh": "scripts/e2e-console.sh down   # stop and wipe",
}


def run_isolated(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as tmp:
        scripts = Path(tmp) / "scripts"
        scripts.mkdir()
        copy = scripts / script
        shutil.copy2(SCRIPTS / script, copy)
        return subprocess.run(
            ["bash", str(copy), *args],
            cwd=tmp,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )


class HelpIsAnswered(unittest.TestCase):
    def test_both_spellings_print_usage_and_exit_zero(self) -> None:
        for script, line in HELP.items():
            for flag in ("-h", "--help"):
                with self.subTest(script=script, flag=flag):
                    result = run_isolated(script, flag)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(line, result.stdout)
                    self.assertEqual(result.stderr, "")

    def test_usage_is_the_header_without_comment_markers(self) -> None:
        for script in HELP:
            with self.subTest(script=script):
                out = run_isolated(script, "--help").stdout
                self.assertFalse(out.startswith("#"), out)
                self.assertNotIn("set -euo pipefail", out)
                self.assertNotIn("#!/usr/bin/env", out)

    def test_help_is_honoured_in_any_argument_position(self) -> None:
        # `upstream-pr.sh <branch> --help` used to take `--help` as the PR title and push.
        result = run_isolated("upstream-pr.sh", "some-branch", "--help")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(HELP["upstream-pr.sh"], result.stdout)


class MisuseIsAnError(unittest.TestCase):
    def test_an_unknown_e2e_command_is_usage_on_stderr_and_exit_2(self) -> None:
        result = run_isolated("e2e-console.sh", "bogus")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn(HELP["e2e-console.sh"], result.stderr)

    def test_upstream_pr_without_arguments_still_fails(self) -> None:
        result = run_isolated("upstream-pr.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("usage", result.stderr)

    def test_sync_upstream_rejects_arguments_it_does_not_take(self) -> None:
        result = run_isolated("sync-upstream.sh", "--force")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn(HELP["sync-upstream.sh"], result.stderr)


if __name__ == "__main__":
    unittest.main()
