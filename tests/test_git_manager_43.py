"""Tests for GitManager.commit_paths scoped commit behavior (issue #43)."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.git_manager import GitManager


class TestCommitPathsScoped(unittest.TestCase):
    """Verify that commit_paths only commits the specified paths."""

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.gm = GitManager(self.repo, "test", "test@test.com")
        self.gm.init_repo()
        # Create an initial commit so HEAD exists.
        init_file = self.repo / "init.txt"
        init_file.write_text("init")
        self.gm.commit_file(init_file, "initial commit")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _log_oneline(self) -> list[str]:
        """Return git log messages (one per commit, newest first)."""
        cp = subprocess.run(
            ["git", "log", "--oneline", "--format=%s"],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=True,
        )
        return [line for line in cp.stdout.strip().splitlines() if line]

    def _committed_files(self, ref: str = "HEAD") -> set[str]:
        """Return filenames touched in the given commit."""
        cp = subprocess.run(
            ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", ref],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=True,
        )
        return {line for line in cp.stdout.strip().splitlines() if line}

    def _stage(self, *names: str) -> None:
        """Stage files by name in the test repo."""
        subprocess.run(
            ["git", "add", *names],
            cwd=self.repo,
            check=True,
        )

    def test_commit_scoped_to_specified_paths(self) -> None:
        """A commit_paths call must only include the files it was given."""
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa")
        file_b.write_text("bbb")

        # Stage both files to simulate a concurrent add.
        self._stage("a.txt", "b.txt")

        # Commit only file_a via commit_paths.
        result = self.gm.commit_paths([file_a], "commit a only")
        self.assertTrue(result)

        # The HEAD commit must only contain a.txt.
        self.assertEqual(self._committed_files("HEAD"), {"a.txt"})

        # b.txt should still be staged (not committed yet).
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", "b.txt"],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("A  b.txt", status.stdout)

    def test_pre_staged_files_not_captured_by_other_commit(self) -> None:
        """Simulate the race: both files staged, then scoped commits in sequence.

        Pre-stages both files before either commit runs, reproducing the
        exact scenario from issue #43. Without the fix, the first commit
        would capture both files and the second would return False.
        """
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa")
        file_b.write_text("bbb")

        # Both files staged in the index before any commit runs.
        self._stage("a.txt", "b.txt")

        # First scoped commit: only file_a.
        result_a = self.gm.commit_paths([file_a], "commit a")
        self.assertTrue(result_a)
        self.assertEqual(self._committed_files("HEAD"), {"a.txt"})

        # Second scoped commit: only file_b.
        # Without the fix this would return False (b.txt already committed by A).
        result_b = self.gm.commit_paths([file_b], "commit b")
        self.assertTrue(result_b)
        self.assertEqual(self._committed_files("HEAD"), {"b.txt"})

        log = self._log_oneline()
        self.assertIn("commit a", log)
        self.assertIn("commit b", log)

    def test_four_files_pre_staged_then_committed_individually(self) -> None:
        """All files staged upfront; each scoped commit captures only its own file."""
        files = {}
        for i in range(4):
            f = self.repo / f"file_{i}.txt"
            f.write_text(f"content {i}")
            files[i] = f

        # Stage everything before any commit — worst-case index contention.
        self._stage(*(f"file_{i}.txt" for i in range(4)))

        for i in range(4):
            result = self.gm.commit_paths([files[i]], f"commit file_{i}")
            self.assertTrue(result, f"commit for file_{i} should succeed")
            self.assertEqual(
                self._committed_files("HEAD"),
                {f"file_{i}.txt"},
                f"HEAD after committing file_{i} should only contain file_{i}.txt",
            )

        log = self._log_oneline()
        for i in range(4):
            self.assertIn(f"commit file_{i}", log)

    def test_commit_paths_returns_false_when_no_changes(self) -> None:
        """commit_paths returns False for paths with no pending changes."""
        file_a = self.repo / "a.txt"
        file_a.write_text("aaa")
        self.gm.commit_file(file_a, "commit a")

        # Call again with the same file — nothing changed.
        result = self.gm.commit_paths([file_a], "should be noop")
        self.assertFalse(result)

        # Also verify for a file that exists but is unchanged.
        result2 = self.gm.commit_paths([file_a], "second noop")
        self.assertFalse(result2)

    def test_env_author_overrides_system_env(self) -> None:
        """commit_paths uses the configured author, not ambient GIT_* env vars."""
        file_a = self.repo / "a.txt"
        file_a.write_text("aaa")

        # Even if the environment has different GIT_AUTHOR_NAME, the
        # GitManager's configured author should win.
        import os
        old = os.environ.get("GIT_AUTHOR_NAME")
        try:
            os.environ["GIT_AUTHOR_NAME"] = "wrong_author"
            self.gm.commit_paths([file_a], "author check")
        finally:
            if old is None:
                os.environ.pop("GIT_AUTHOR_NAME", None)
            else:
                os.environ["GIT_AUTHOR_NAME"] = old

        cp = subprocess.run(
            ["git", "log", "-1", "--format=%an"],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertEqual(cp.stdout.strip(), "test")


if __name__ == "__main__":
    unittest.main()
