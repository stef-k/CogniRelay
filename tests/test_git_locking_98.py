"""Regression tests for Issue #98 repository-level git mutation locking."""

from __future__ import annotations

import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

import app.git_safety as git_safety
from app.continuity.service import continuity_delete_service
from app.coordination.common import persist_new_artifact
from app.git_manager import GitManager
from app.git_safety import safe_commit_new_file
from app.models import ContinuityDeleteRequest
from tests.helpers import AllowAllAuthStub


class TestRepositoryMutationLocking(unittest.TestCase):
    """Validate repository-level serialization for git-backed mutations."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.gm = GitManager(self.repo, "test", "test@test.com")
        self.gm.init_repo()
        seed = self.repo / "seed.txt"
        seed.write_text("seed", encoding="utf-8")
        self.gm.commit_file(seed, "seed")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run_thread(self, target):
        result: dict[str, object] = {}
        error: dict[str, BaseException] = {}

        def runner() -> None:
            try:
                result["value"] = target()
            except BaseException as exc:  # noqa: BLE001 - test needs raw exception capture
                error["value"] = exc

        thread = threading.Thread(target=runner)
        thread.start()
        return thread, result, error

    def _commit_files(self, ref: str = "HEAD") -> set[str]:
        """Return the file paths recorded by one commit."""
        cp = subprocess.run(
            ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", ref],
            cwd=self.repo,
            check=True,
            text=True,
            capture_output=True,
        )
        return {line for line in cp.stdout.splitlines() if line}

    def _commit_sha(self, message: str) -> str:
        """Return the SHA for the most recent commit with the exact subject."""
        cp = subprocess.run(
            ["git", "log", "--format=%H%x00%s"],
            cwd=self.repo,
            check=True,
            text=True,
            capture_output=True,
        )
        for line in cp.stdout.splitlines():
            sha, subject = line.split("\x00", 1)
            if subject == message:
                return sha
        raise AssertionError(f"Commit not found: {message}")

    def _status(self, *rels: str) -> str:
        """Return porcelain status for the selected paths."""
        cp = subprocess.run(
            ["git", "status", "--porcelain", "--", *rels],
            cwd=self.repo,
            check=True,
            text=True,
            capture_output=True,
        )
        return cp.stdout.strip()

    def test_unrelated_concurrent_commits_do_not_cross_contaminate(self) -> None:
        """One blocked commit must keep another commit out of the shared index."""
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa", encoding="utf-8")
        file_b.write_text("bbb", encoding="utf-8")

        entered_a = threading.Event()
        release_a = threading.Event()
        entered_b = threading.Event()
        real_run = subprocess.run

        def controlled_run(*args, **kwargs):
            cmd = args[0]
            if cmd[:2] == ["git", "commit"]:
                message = cmd[cmd.index("-m") + 1]
                if message == "commit a":
                    entered_a.set()
                    if not release_a.wait(timeout=5):
                        raise RuntimeError("Timed out waiting to release commit a")
                elif message == "commit b":
                    entered_b.set()
            return real_run(*args, **kwargs)

        with patch("app.git_manager.subprocess.run", side_effect=controlled_run):
            thread_a, result_a, error_a = self._run_thread(lambda: self.gm.commit_file(file_a, "commit a"))
            self.assertTrue(entered_a.wait(timeout=5))
            thread_b, result_b, error_b = self._run_thread(lambda: self.gm.commit_file(file_b, "commit b"))
            self.assertFalse(entered_b.wait(timeout=0.2))
            release_a.set()
            thread_a.join(timeout=5)
            thread_b.join(timeout=5)

        self.assertNotIn("value", error_a)
        self.assertNotIn("value", error_b)
        self.assertTrue(result_a["value"])
        self.assertTrue(result_b["value"])
        self.assertEqual(self._commit_files(self._commit_sha("commit a")), {"a.txt"})
        self.assertEqual(self._commit_files(self._commit_sha("commit b")), {"b.txt"})

    def test_failed_commit_rolls_back_before_other_commit_can_start(self) -> None:
        """Rollback after a failed commit must complete before another commit enters."""
        file_a = self.repo / "a.txt"
        file_b = self.repo / "b.txt"
        file_a.write_text("aaa", encoding="utf-8")
        file_b.write_text("bbb", encoding="utf-8")

        entered_failed_commit = threading.Event()
        release_failed_commit = threading.Event()
        entered_unstage = threading.Event()
        release_unstage = threading.Event()
        entered_b_commit = threading.Event()
        real_run = subprocess.run
        real_unstage = git_safety._unstage

        def controlled_run(*args, **kwargs):
            cmd = args[0]
            if cmd[:2] == ["git", "commit"]:
                message = cmd[cmd.index("-m") + 1]
                if message == "fail a":
                    entered_failed_commit.set()
                    if not release_failed_commit.wait(timeout=5):
                        raise RuntimeError("Timed out waiting to release failed commit")
                    raise subprocess.CalledProcessError(1, cmd, stderr="boom")
                if message == "commit b":
                    entered_b_commit.set()
            return real_run(*args, **kwargs)

        def controlled_unstage(gm, paths):
            if paths == [file_a]:
                entered_unstage.set()
                if not release_unstage.wait(timeout=5):
                    raise RuntimeError("Timed out waiting to release rollback")
            return real_unstage(gm, paths)

        with patch("app.git_manager.subprocess.run", side_effect=controlled_run), patch(
            "app.git_safety._unstage",
            side_effect=controlled_unstage,
        ):
            thread_a, _result_a, error_a = self._run_thread(
                lambda: safe_commit_new_file(
                    path=file_a,
                    gm=self.gm,
                    commit_message="fail a",
                    error_detail="expected failure",
                )
            )
            self.assertTrue(entered_failed_commit.wait(timeout=5))
            thread_b, result_b, error_b = self._run_thread(lambda: self.gm.commit_file(file_b, "commit b"))
            release_failed_commit.set()
            self.assertTrue(entered_unstage.wait(timeout=5))
            self.assertFalse(entered_b_commit.wait(timeout=0.2))
            release_unstage.set()
            thread_a.join(timeout=5)
            thread_b.join(timeout=5)

        self.assertIn("value", error_a)
        self.assertIsInstance(error_a["value"], HTTPException)
        self.assertNotIn("value", error_b)
        self.assertTrue(result_b["value"])
        self.assertFalse(file_a.exists())
        self.assertEqual(self._commit_files(self._commit_sha("commit b")), {"b.txt"})
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", "a.txt"],
            cwd=self.repo,
            check=True,
            text=True,
            capture_output=True,
        )
        self.assertEqual(status.stdout.strip(), "")

    def test_continuity_delete_rollback_cleans_index(self) -> None:
        """Failed continuity delete should restore files and leave no staged deletions."""
        active = self.repo / "memory" / "continuity" / "user-stef.json"
        fallback = self.repo / "memory" / "continuity" / "fallback" / "user-stef.json"
        active.parent.mkdir(parents=True, exist_ok=True)
        fallback.parent.mkdir(parents=True, exist_ok=True)
        active.write_text('{"active": true}\n', encoding="utf-8")
        fallback.write_text('{"fallback": true}\n', encoding="utf-8")
        self.gm.commit_paths([active, fallback], "seed continuity")

        real_run = subprocess.run

        def fail_commit(*args, **kwargs):
            cmd = args[0]
            if cmd[:2] == ["git", "commit"]:
                raise subprocess.CalledProcessError(1, cmd, stderr="boom")
            return real_run(*args, **kwargs)

        with patch("app.git_manager.subprocess.run", side_effect=fail_commit):
            with self.assertRaises(HTTPException):
                continuity_delete_service(
                    repo_root=self.repo,
                    gm=self.gm,
                    auth=AllowAllAuthStub(),
                    req=ContinuityDeleteRequest(
                        subject_kind="user",
                        subject_id="stef",
                        delete_active=True,
                        delete_fallback=True,
                        reason="cleanup",
                    ),
                    audit=lambda *_args: None,
                )

        self.assertTrue(active.exists())
        self.assertTrue(fallback.exists())
        self.assertEqual(self._status("memory/continuity/user-stef.json", "memory/continuity/fallback/user-stef.json"), "")

    def test_persist_new_artifact_rollback_cleans_index(self) -> None:
        """Failed coordination artifact persist should remove staged AD entries."""
        artifact_path = self.repo / "memory" / "coordination" / "handoffs" / "handoff_test.json"
        artifact = {"handoff_id": "handoff_test", "created_at": "2026-03-18T00:00:00Z"}
        real_run = subprocess.run

        def fail_commit(*args, **kwargs):
            cmd = args[0]
            if cmd[:2] == ["git", "commit"]:
                raise subprocess.CalledProcessError(1, cmd, stderr="boom")
            return real_run(*args, **kwargs)

        with patch("app.git_manager.subprocess.run", side_effect=fail_commit):
            with self.assertRaises(HTTPException):
                persist_new_artifact(
                    path=artifact_path,
                    rel="memory/coordination/handoffs/handoff_test.json",
                    gm=self.gm,
                    artifact=artifact,
                    commit_message="handoff: create handoff_test",
                    error_detail="expected failure",
                )

        self.assertFalse(artifact_path.exists())
        self.assertEqual(self._status("memory/coordination/handoffs/handoff_test.json"), "")


if __name__ == "__main__":
    unittest.main()
