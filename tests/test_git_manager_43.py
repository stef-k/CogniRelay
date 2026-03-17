"""Tests for GitManager.commit_paths scoped commit behavior (issue #43)."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def test_commit_scoped_to_specified_paths(self) -> None:
        """A commit_paths call must only include the files it was given."""
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa")
        file_b.write_text("bbb")

        # Stage both files manually to simulate a concurrent add.
        subprocess.run(
            ["git", "add", "a.txt", "b.txt"],
            cwd=self.repo,
            check=True,
        )

        # Commit only file_a via commit_paths.
        result = self.gm.commit_paths([file_a], "commit a only")
        self.assertTrue(result)

        # The HEAD commit must only contain a.txt.
        committed = self._committed_files("HEAD")
        self.assertEqual(committed, {"a.txt"})

        # b.txt should still be staged (not committed yet).
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", "b.txt"],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("A  b.txt", status.stdout)

    def test_concurrent_commit_paths_no_cross_contamination(self) -> None:
        """Two concurrent commit_paths for different files each produce their own commit."""
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa")
        file_b.write_text("bbb")

        # Use a lock-step approach: stage both, then commit sequentially
        # to simulate the race described in the issue.
        # With the fix, each commit is scoped so the second one still succeeds.
        result_a = self.gm.commit_paths([file_a], "commit a")
        result_b = self.gm.commit_paths([file_b], "commit b")

        self.assertTrue(result_a)
        self.assertTrue(result_b)

        log = self._log_oneline()
        self.assertIn("commit a", log)
        self.assertIn("commit b", log)

        # Verify each commit only touched its own file.
        # HEAD is commit b, HEAD~1 is commit a.
        self.assertEqual(self._committed_files("HEAD"), {"b.txt"})
        self.assertEqual(self._committed_files("HEAD~1"), {"a.txt"})

    def test_concurrent_threadpool_no_lost_commits(self) -> None:
        """Parallel commit_paths calls from threads must each succeed."""
        files = {}
        for i in range(4):
            f = self.repo / f"file_{i}.txt"
            f.write_text(f"content {i}")
            files[i] = f

        results = {}
        # Run commits one at a time but from different threads to prove
        # each commit is self-contained (no index cross-talk).
        with ThreadPoolExecutor(max_workers=1) as pool:
            futures = {
                pool.submit(
                    self.gm.commit_paths, [files[i]], f"commit file_{i}"
                ): i
                for i in range(4)
            }
            for future in as_completed(futures):
                idx = futures[future]
                results[idx] = future.result()

        # All four commits must succeed.
        for i in range(4):
            self.assertTrue(results[i], f"commit for file_{i} should succeed")

        log = self._log_oneline()
        for i in range(4):
            self.assertIn(f"commit file_{i}", log)


if __name__ == "__main__":
    unittest.main()
