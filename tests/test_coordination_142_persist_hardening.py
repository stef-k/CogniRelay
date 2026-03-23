"""Regression tests for Issue #142: coordination persist helpers write inside lock.

Verifies that:
  1. persist_new_artifact and persist_updated_artifact write the artifact file
     inside repository_mutation_lock, not before it.
  2. Lock timeout leaves no orphaned file (new) or preserves original (update).
  3. Commit failure triggers correct rollback with atomic writes.
  4. Rollback failure is surfaced in the HTTPException detail.
  5. persist_updated_artifact uses write_bytes_file (atomic) for rollback.
  6. write_text_file failure inside the lock produces a structured HTTPException.
  7. commit_file returning False (no-op) triggers rollback.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.coordination.common import persist_new_artifact, persist_updated_artifact
from app.git_locking import GitLockTimeout
from app.git_manager import GitManager
from app.storage import write_text_file
from tests.helpers import SimpleGitManagerStub


# ------------------------------------------------------------------ #
#  Stubs
# ------------------------------------------------------------------ #

class _FailingGitManagerStub:
    """Git manager stub that raises on commit_file."""

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root

    def commit_file(self, _path: Path, _message: str) -> bool:
        raise RuntimeError("simulated commit failure")


class _NoOpGitManagerStub:
    """Git manager stub that returns False from commit_file (no changes)."""

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root

    def commit_file(self, _path: Path, _message: str) -> bool:
        return False


@contextmanager
def _raise_git_lock_timeout(*_args, **_kwargs):
    """Context manager that raises GitLockTimeout on entry."""
    raise GitLockTimeout("simulated git lock timeout")
    yield  # noqa: RET503


# ------------------------------------------------------------------ #
#  Tests: success paths
# ------------------------------------------------------------------ #


class TestPersistSuccessPaths(unittest.TestCase):
    """Basic success paths for both persist helpers."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_success_path(self) -> None:
        """persist_new_artifact writes the file and returns the rel path."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_aaa.json"
        artifact = {"handoff_id": "handoff_aaa", "data": "test"}
        rel = "memory/coordination/handoffs/handoff_aaa.json"

        result = persist_new_artifact(
            path=path, rel=rel, gm=gm, artifact=artifact,
            commit_message="test", error_detail="fail",
        )

        self.assertEqual(result, rel)
        self.assertTrue(path.exists())
        stored = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(stored["handoff_id"], "handoff_aaa")

    def test_updated_artifact_success_path(self) -> None:
        """persist_updated_artifact overwrites existing content and returns rel."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_bbb.json"
        rel = "memory/coordination/handoffs/handoff_bbb.json"

        # Pre-create file
        write_text_file(path, json.dumps({"version": 1}))
        self.assertTrue(path.exists())

        result = persist_updated_artifact(
            path=path, rel=rel, gm=gm,
            artifact={"version": 2, "handoff_id": "handoff_bbb"},
            commit_message="test", error_detail="fail",
        )

        self.assertEqual(result, rel)
        stored = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(stored["version"], 2)


# ------------------------------------------------------------------ #
#  Tests: commit failure rollback
# ------------------------------------------------------------------ #


class TestCommitFailureRollback(unittest.TestCase):
    """Verify rollback on commit failure for both helpers."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_rollback_on_commit_failure(self) -> None:
        """Failed commit removes the newly written file."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_ccc.json"

        with self.assertRaises(HTTPException) as cm:
            persist_new_artifact(
                path=path,
                rel="memory/coordination/handoffs/handoff_ccc.json",
                gm=gm,
                artifact={"handoff_id": "handoff_ccc"},
                commit_message="test",
                error_detail="expected failure",
            )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertTrue(cm.exception.detail.startswith("expected failure:"))
        self.assertIn("simulated commit failure", cm.exception.detail)
        self.assertFalse(path.exists())

    def test_updated_artifact_rollback_restores_old_bytes(self) -> None:
        """Failed commit restores the original file content."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_ddd.json"

        original = json.dumps({"version": 1, "handoff_id": "handoff_ddd"})
        write_text_file(path, original)
        original_bytes = path.read_bytes()

        with self.assertRaises(HTTPException) as cm:
            persist_updated_artifact(
                path=path,
                rel="memory/coordination/handoffs/handoff_ddd.json",
                gm=gm,
                artifact={"version": 2, "handoff_id": "handoff_ddd"},
                commit_message="test",
                error_detail="expected failure",
            )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), original_bytes)

    def test_updated_artifact_rollback_deletes_when_no_prior_file(self) -> None:
        """If no prior file existed, failed commit removes the newly created file."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_eee.json"

        with self.assertRaises(HTTPException):
            persist_updated_artifact(
                path=path,
                rel="memory/coordination/handoffs/handoff_eee.json",
                gm=gm,
                artifact={"handoff_id": "handoff_eee"},
                commit_message="test",
                error_detail="expected failure",
            )

        self.assertFalse(path.exists())


# ------------------------------------------------------------------ #
#  Tests: commit_file returns False (no-op)
# ------------------------------------------------------------------ #


class TestNoOpCommitRollback(unittest.TestCase):
    """Verify rollback when commit_file returns False (no changes detected)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_noop_commit_triggers_rollback(self) -> None:
        """commit_file returning False raises RuntimeError caught by rollback."""
        gm = _NoOpGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_noop.json"

        with self.assertRaises(HTTPException) as cm:
            persist_new_artifact(
                path=path,
                rel="memory/coordination/handoffs/handoff_noop.json",
                gm=gm,
                artifact={"handoff_id": "handoff_noop"},
                commit_message="test",
                error_detail="expected failure",
            )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertIn("no changes", cm.exception.detail)
        self.assertFalse(path.exists())

    def test_updated_artifact_noop_commit_restores_original(self) -> None:
        """commit_file returning False triggers rollback that restores prior bytes."""
        gm = _NoOpGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_noop2.json"

        original = json.dumps({"version": 1})
        write_text_file(path, original)
        original_bytes = path.read_bytes()

        with self.assertRaises(HTTPException) as cm:
            persist_updated_artifact(
                path=path,
                rel="memory/coordination/handoffs/handoff_noop2.json",
                gm=gm,
                artifact={"version": 2},
                commit_message="test",
                error_detail="expected failure",
            )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertIn("no changes", cm.exception.detail)
        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), original_bytes)


# ------------------------------------------------------------------ #
#  Tests: lock timeout — no orphan / preserve original
# ------------------------------------------------------------------ #


class TestLockTimeoutDurability(unittest.TestCase):
    """Verify that lock timeout does not leave dirty filesystem state."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_lock_timeout_leaves_no_orphan(self) -> None:
        """If the lock times out, no artifact file should exist on disk."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_fff.json"

        with patch(
            "app.coordination.common.repository_mutation_lock",
            _raise_git_lock_timeout,
        ):
            with self.assertRaises(GitLockTimeout):
                persist_new_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_fff.json",
                    gm=gm,
                    artifact={"handoff_id": "handoff_fff"},
                    commit_message="test",
                    error_detail="fail",
                )

        self.assertFalse(path.exists())

    def test_updated_artifact_lock_timeout_preserves_original(self) -> None:
        """If the lock times out, the original file content must survive."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_ggg.json"

        original = json.dumps({"version": 1, "handoff_id": "handoff_ggg"})
        write_text_file(path, original)
        original_bytes = path.read_bytes()

        with patch(
            "app.coordination.common.repository_mutation_lock",
            _raise_git_lock_timeout,
        ):
            with self.assertRaises(GitLockTimeout):
                persist_updated_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_ggg.json",
                    gm=gm,
                    artifact={"version": 2, "handoff_id": "handoff_ggg"},
                    commit_message="test",
                    error_detail="fail",
                )

        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), original_bytes)


# ------------------------------------------------------------------ #
#  Tests: write_text_file failure inside lock
# ------------------------------------------------------------------ #


class TestWriteFailureInsideLock(unittest.TestCase):
    """Verify that write_text_file failure produces structured HTTPException."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_write_failure_returns_structured_500(self) -> None:
        """write_text_file raising inside lock produces HTTPException, not raw OSError."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_wfail.json"

        with patch(
            "app.coordination.common.write_text_file",
            side_effect=OSError("simulated disk full"),
        ):
            with self.assertRaises(HTTPException) as cm:
                persist_new_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_wfail.json",
                    gm=gm,
                    artifact={"handoff_id": "handoff_wfail"},
                    commit_message="test",
                    error_detail="expected failure",
                )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertIn("write failed", cm.exception.detail)
        self.assertIn("simulated disk full", cm.exception.detail)

    def test_updated_artifact_write_failure_preserves_original(self) -> None:
        """write_text_file failure leaves original file untouched."""
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_wfail2.json"

        original = json.dumps({"version": 1})
        write_text_file(path, original)
        original_bytes = path.read_bytes()

        with patch(
            "app.coordination.common.write_text_file",
            side_effect=OSError("simulated disk full"),
        ):
            with self.assertRaises(HTTPException) as cm:
                persist_updated_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_wfail2.json",
                    gm=gm,
                    artifact={"version": 2},
                    commit_message="test",
                    error_detail="expected failure",
                )

        self.assertEqual(cm.exception.status_code, 500)
        self.assertIn("write failed", cm.exception.detail)
        # Original file must be untouched since write_text_file was mocked to fail
        # before any rename could occur.
        self.assertTrue(path.exists())
        self.assertEqual(path.read_bytes(), original_bytes)


# ------------------------------------------------------------------ #
#  Tests: rollback failure reporting
# ------------------------------------------------------------------ #


class TestRollbackFailureReporting(unittest.TestCase):
    """Verify that rollback failures are surfaced in the HTTP error detail."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_new_artifact_rollback_failure_in_detail(self) -> None:
        """When both commit and unlink fail, the error detail includes both."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_hhh.json"

        with patch.object(
            Path, "unlink", side_effect=OSError("simulated unlink failure"),
        ):
            with self.assertRaises(HTTPException) as cm:
                persist_new_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_hhh.json",
                    gm=gm,
                    artifact={"handoff_id": "handoff_hhh"},
                    commit_message="test",
                    error_detail="expected failure",
                )

        detail = str(cm.exception.detail)
        self.assertIn("simulated commit failure", detail)
        self.assertIn("rollback failed", detail)
        self.assertIn("simulated unlink failure", detail)

    def test_updated_artifact_rollback_failure_in_detail(self) -> None:
        """When both commit and restore fail, the error detail includes both."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_iii.json"

        original = json.dumps({"version": 1, "handoff_id": "handoff_iii"})
        write_text_file(path, original)

        with patch(
            "app.coordination.common.write_bytes_file",
            side_effect=OSError("simulated restore failure"),
        ):
            with self.assertRaises(HTTPException) as cm:
                persist_updated_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_iii.json",
                    gm=gm,
                    artifact={"version": 2, "handoff_id": "handoff_iii"},
                    commit_message="test",
                    error_detail="expected failure",
                )

        detail = str(cm.exception.detail)
        self.assertIn("simulated commit failure", detail)
        self.assertIn("rollback failed", detail)
        self.assertIn("simulated restore failure", detail)


# ------------------------------------------------------------------ #
#  Tests: atomic rollback write
# ------------------------------------------------------------------ #


class TestAtomicRollbackWrite(unittest.TestCase):
    """Verify persist_updated_artifact uses write_bytes_file for rollback."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_rollback_calls_write_bytes_file(self) -> None:
        """Rollback must use write_bytes_file, not bare path.write_bytes."""
        gm = _FailingGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_jjj.json"

        original = json.dumps({"version": 1, "handoff_id": "handoff_jjj"})
        write_text_file(path, original)
        original_bytes = path.read_bytes()

        calls: list[tuple[Path, bytes]] = []

        from app.storage import write_bytes_file as _real_wbf

        def recording_write_bytes_file(p: Path, data: bytes) -> None:
            calls.append((p, data))
            _real_wbf(p, data)

        with patch(
            "app.coordination.common.write_bytes_file",
            side_effect=recording_write_bytes_file,
        ):
            with self.assertRaises(HTTPException):
                persist_updated_artifact(
                    path=path,
                    rel="memory/coordination/handoffs/handoff_jjj.json",
                    gm=gm,
                    artifact={"version": 2, "handoff_id": "handoff_jjj"},
                    commit_message="test",
                    error_detail="fail",
                )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], path)
        self.assertEqual(calls[0][1], original_bytes)


# ------------------------------------------------------------------ #
#  Tests: write happens inside lock
# ------------------------------------------------------------------ #


class TestWriteInsideLock(unittest.TestCase):
    """Verify both helpers call write_text_file inside the lock context."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _make_instrumented_lock(self, call_log: list[str]):
        """Return a lock context manager that records enter/exit in call_log."""
        @contextmanager
        def instrumented_lock(*_args, **_kwargs):
            call_log.append("lock_enter")
            yield
            call_log.append("lock_exit")
        return instrumented_lock

    def _make_instrumented_write(self, call_log: list[str]):
        """Return a write function that records the call and delegates."""
        from app.storage import write_text_file as real_write
        def instrumented_write(path, content):
            call_log.append("write")
            return real_write(path, content)
        return instrumented_write

    def test_new_artifact_write_inside_lock(self) -> None:
        """write_text_file must be called between lock_enter and lock_exit."""
        call_log: list[str] = []
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_kkk.json"

        with patch("app.coordination.common.repository_mutation_lock",
                    self._make_instrumented_lock(call_log)), \
             patch("app.coordination.common.write_text_file",
                    self._make_instrumented_write(call_log)):
            persist_new_artifact(
                path=path, rel="test", gm=gm,
                artifact={"handoff_id": "handoff_kkk"},
                commit_message="test", error_detail="fail",
            )

        self.assertEqual(call_log, ["lock_enter", "write", "lock_exit"])

    def test_updated_artifact_write_inside_lock(self) -> None:
        """write_text_file must be called between lock_enter and lock_exit."""
        call_log: list[str] = []
        gm = SimpleGitManagerStub(self.repo)
        path = self.artifact_dir / "handoff_lll.json"
        write_text_file(path, json.dumps({"version": 1}))

        with patch("app.coordination.common.repository_mutation_lock",
                    self._make_instrumented_lock(call_log)), \
             patch("app.coordination.common.write_text_file",
                    self._make_instrumented_write(call_log)):
            persist_updated_artifact(
                path=path, rel="test", gm=gm,
                artifact={"version": 2, "handoff_id": "handoff_lll"},
                commit_message="test", error_detail="fail",
            )

        self.assertEqual(call_log, ["lock_enter", "write", "lock_exit"])


# ------------------------------------------------------------------ #
#  Tests: concurrent access serialization
# ------------------------------------------------------------------ #


class TestConcurrentPersistSerialization(unittest.TestCase):
    """Verify two threads persisting simultaneously are serialized by the lock."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self.gm = GitManager(self.repo, "test", "test@test.com")
        self.gm.init_repo()
        seed = self.repo / "seed.txt"
        seed.write_text("seed", encoding="utf-8")
        self.gm.commit_file(seed, "seed")
        self.artifact_dir = self.repo / "memory" / "coordination" / "handoffs"
        self.artifact_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_concurrent_creates_both_succeed(self) -> None:
        """Two threads creating different artifacts should both succeed."""
        results: dict[str, str] = {}
        errors: dict[str, BaseException] = {}

        def create_artifact(tag: str) -> None:
            try:
                artifact_id = f"handoff_{tag}"
                path = self.artifact_dir / f"{artifact_id}.json"
                rel = f"memory/coordination/handoffs/{artifact_id}.json"
                result = persist_new_artifact(
                    path=path, rel=rel, gm=self.gm,
                    artifact={"handoff_id": artifact_id, "tag": tag},
                    commit_message=f"create {artifact_id}",
                    error_detail="fail",
                )
                results[tag] = result
            except BaseException as exc:  # noqa: BLE001
                errors[tag] = exc

        t1 = threading.Thread(target=create_artifact, args=("thread1",))
        t2 = threading.Thread(target=create_artifact, args=("thread2",))
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        self.assertEqual(errors, {}, f"Unexpected errors: {errors}")
        self.assertEqual(len(results), 2)
        # Both files must exist with correct content
        for tag in ("thread1", "thread2"):
            path = self.artifact_dir / f"handoff_{tag}.json"
            self.assertTrue(path.exists(), f"Missing {path}")
            stored = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(stored["tag"], tag)


# ------------------------------------------------------------------ #
#  Tests: real git rollback (integration)
# ------------------------------------------------------------------ #


class TestRealGitRollback(unittest.TestCase):
    """Integration test using a real git repo to verify index stays clean."""

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

    def _status(self, *rels: str) -> str:
        cp = subprocess.run(
            ["git", "status", "--porcelain", "--", *rels],
            cwd=self.repo, check=True, text=True, capture_output=True,
        )
        return cp.stdout.strip()

    def test_new_artifact_rollback_cleans_index(self) -> None:
        """Failed commit leaves no staged entries and no file on disk."""
        artifact_path = self.repo / "memory" / "coordination" / "handoffs" / "handoff_test.json"
        artifact = {"handoff_id": "handoff_test", "created_at": "2026-03-23T00:00:00Z"}
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

    def test_updated_artifact_rollback_restores_and_cleans_index(self) -> None:
        """Failed commit restores original content and leaves index clean."""
        artifact_path = self.repo / "memory" / "coordination" / "handoffs" / "handoff_upd.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

        original = {"handoff_id": "handoff_upd", "version": 1}
        write_text_file(artifact_path, json.dumps(original))
        self.gm.commit_file(artifact_path, "initial")
        original_bytes = artifact_path.read_bytes()

        real_run = subprocess.run

        def fail_commit(*args, **kwargs):
            cmd = args[0]
            if cmd[:2] == ["git", "commit"]:
                raise subprocess.CalledProcessError(1, cmd, stderr="boom")
            return real_run(*args, **kwargs)

        with patch("app.git_manager.subprocess.run", side_effect=fail_commit):
            with self.assertRaises(HTTPException):
                persist_updated_artifact(
                    path=artifact_path,
                    rel="memory/coordination/handoffs/handoff_upd.json",
                    gm=self.gm,
                    artifact={"handoff_id": "handoff_upd", "version": 2},
                    commit_message="handoff: update",
                    error_detail="expected failure",
                )

        self.assertTrue(artifact_path.exists())
        self.assertEqual(artifact_path.read_bytes(), original_bytes)
        self.assertEqual(self._status("memory/coordination/handoffs/handoff_upd.json"), "")


if __name__ == "__main__":
    unittest.main()
