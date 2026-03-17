"""Tests for Issue #42: per-artifact file locking for coordination mutations.

Validates that concurrent mutations to the same artifact are serialized
(at most one succeeds), while mutations to different artifacts proceed
independently.
"""

from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from app.coordination.locking import _lock_dir_ready, artifact_lock, purge_stale_lockfiles
from app.coordination.shared_service import shared_update_service
from app.models import CoordinationSharedArtifact, CoordinationSharedUpdateRequest
from app.storage import canonical_json, write_text_file
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _SlowGitManagerStub(SimpleGitManagerStub):
    """Git stub that introduces a delay to widen the race window."""

    def __init__(self, delay: float = 0.1) -> None:
        self._delay = delay

    def commit_file(self, _path: Path, _message: str) -> bool:
        time.sleep(self._delay)
        return True


class TestArtifactLockUnit(unittest.TestCase):
    """Validate the artifact_lock context manager in isolation."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.lock_dir = Path(self._tmpdir.name) / ".locks"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        _lock_dir_ready.discard(str(self.lock_dir))

    def test_lock_serializes_same_artifact(self) -> None:
        """Two threads acquiring a lock on the same id run sequentially."""
        order: list[str] = []
        barrier = threading.Barrier(2, timeout=5)

        def worker(tag: str) -> None:
            barrier.wait()
            with artifact_lock("same_id", lock_dir=self.lock_dir):
                order.append(f"{tag}-enter")
                time.sleep(0.05)
                order.append(f"{tag}-exit")

        t1 = threading.Thread(target=worker, args=("A",))
        t2 = threading.Thread(target=worker, args=("B",))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        # One must fully complete before the other enters.
        self.assertEqual(len(order), 4)
        self.assertEqual(order[0][-5:], "enter")
        self.assertEqual(order[1][-4:], "exit")
        self.assertEqual(order[1][:1], order[0][:1], "Same thread should enter and exit before the other enters")

    def test_lock_allows_different_artifacts_in_parallel(self) -> None:
        """Two threads with different artifact ids can overlap."""
        entered: dict[str, float] = {}
        barrier = threading.Barrier(2, timeout=5)

        def worker(artifact_id: str) -> None:
            barrier.wait()
            with artifact_lock(artifact_id, lock_dir=self.lock_dir):
                entered[artifact_id] = time.monotonic()
                time.sleep(0.05)

        t1 = threading.Thread(target=worker, args=("id_a",))
        t2 = threading.Thread(target=worker, args=("id_b",))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        self.assertIn("id_a", entered)
        self.assertIn("id_b", entered)

    def test_lock_released_on_exception(self) -> None:
        """Lock is released even when the body raises."""
        with self.assertRaises(RuntimeError):
            with artifact_lock("err_id", lock_dir=self.lock_dir):
                raise RuntimeError("boom")

        # Must be able to re-acquire immediately.
        with artifact_lock("err_id", lock_dir=self.lock_dir):
            pass  # Would deadlock if the first lock was not released.

    def test_rejects_path_traversal_id(self) -> None:
        """Artifact IDs containing path traversal characters are rejected."""
        for bad_id in ["../../etc/passwd", "foo/bar", "foo\\bar", "ok\x00evil", "has-dash", "has.dot"]:
            with self.assertRaises(HTTPException) as ctx:
                with artifact_lock(bad_id, lock_dir=self.lock_dir):
                    pass
            self.assertEqual(ctx.exception.status_code, 400)

    def test_accepts_valid_artifact_ids(self) -> None:
        """Well-formed artifact IDs with lowercase hex and underscores are accepted."""
        for good_id in ["shared_abc123", "handoff_deadbeef", "recon_0123456789abcdef"]:
            with artifact_lock(good_id, lock_dir=self.lock_dir):
                pass  # Should not raise.

    def test_raises_503_when_lock_dir_not_writable(self) -> None:
        """Lock infrastructure failure returns HTTP 503."""
        # Use a path under a non-existent read-only parent.
        bad_dir = Path("/proc/nonexistent/.locks")
        with self.assertRaises(HTTPException) as ctx:
            with artifact_lock("some_id", lock_dir=bad_dir):
                pass
        self.assertEqual(ctx.exception.status_code, 503)

    def test_lock_timeout_returns_503(self) -> None:
        """A held lock that exceeds the timeout returns HTTP 503."""
        held = threading.Event()
        release = threading.Event()

        def hold_lock() -> None:
            with artifact_lock("timeout_id", lock_dir=self.lock_dir):
                held.set()
                release.wait(timeout=5)

        holder = threading.Thread(target=hold_lock)
        holder.start()
        held.wait(timeout=5)

        with self.assertRaises(HTTPException) as ctx:
            with artifact_lock("timeout_id", lock_dir=self.lock_dir, timeout=0.15):
                pass
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertIn("timed out", ctx.exception.detail)

        release.set()
        holder.join(timeout=5)


class TestPurgeStaleFiles(unittest.TestCase):
    """Validate startup lockfile purge."""

    def test_purge_removes_lockfiles(self) -> None:
        """purge_stale_lockfiles removes .lock files and returns the count."""
        with tempfile.TemporaryDirectory() as tmp:
            lock_dir = Path(tmp) / ".locks"
            lock_dir.mkdir()
            (lock_dir / "shared_abc.lock").touch()
            (lock_dir / "handoff_def.lock").touch()
            (lock_dir / "not_a_lock.txt").touch()

            removed = purge_stale_lockfiles(lock_dir)
            self.assertEqual(removed, 2)
            self.assertTrue((lock_dir / "not_a_lock.txt").exists())
            self.assertFalse((lock_dir / "shared_abc.lock").exists())

    def test_purge_noop_when_dir_missing(self) -> None:
        """purge_stale_lockfiles returns 0 if the directory doesn't exist."""
        removed = purge_stale_lockfiles(Path("/nonexistent/.locks"))
        self.assertEqual(removed, 0)


class TestSharedUpdateConcurrency(unittest.TestCase):
    """Validate that concurrent shared_update_service calls are serialized."""

    def _seed_shared_artifact(self, repo_root: Path) -> str:
        """Create a minimal shared artifact on disk and return its id."""
        shared_id = f"shared_{uuid4().hex}"
        artifact = CoordinationSharedArtifact(
            shared_id=shared_id,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T00:00:00Z",
            created_by="peer-test",
            owner_peer="peer-test",
            participant_peers=["peer-other"],
            task_id="task-1",
            thread_id=None,
            title="test shared",
            summary=None,
            shared_state={"constraints": ["c1"], "drift_signals": [], "coordination_alerts": []},
            version=1,
            last_updated_by="peer-test",
        ).model_dump(mode="json")
        directory = Path(repo_root) / "memory" / "coordination" / "shared"
        directory.mkdir(parents=True, exist_ok=True)
        write_text_file(directory / f"{shared_id}.json", canonical_json(artifact))
        return shared_id

    def _make_update_request(self, *, expected_version: int, tag: str) -> CoordinationSharedUpdateRequest:
        return CoordinationSharedUpdateRequest(
            title=f"updated-{tag}",
            summary=None,
            constraints=[f"constraint-{tag}"],
            drift_signals=[],
            coordination_alerts=[],
            expected_version=expected_version,
            commit_message=None,
        )

    def test_concurrent_updates_one_wins_one_gets_409(self) -> None:
        """Two concurrent updates to the same shared artifact: at most one succeeds."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            shared_id = self._seed_shared_artifact(repo_root)

            # Peer registry stub.
            registry = {"peers": {"peer-test": {"trust_level": "trusted"}, "peer-other": {"trust_level": "trusted"}}}
            registry_path = repo_root / "memory" / "peers" / "registry.json"
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            write_text_file(registry_path, canonical_json(registry))

            results: dict[str, Any] = {}
            errors: dict[str, Any] = {}
            barrier = threading.Barrier(2, timeout=5)

            def do_update(tag: str) -> None:
                barrier.wait()
                try:
                    result = shared_update_service(
                        repo_root=repo_root,
                        gm=_SlowGitManagerStub(delay=0.1),
                        auth=AllowAllAuthStub(peer_id="peer-test"),
                        shared_id=shared_id,
                        req=self._make_update_request(expected_version=1, tag=tag),
                        enforce_rate_limit=lambda *a: None,
                        enforce_payload_limit=lambda *a: None,
                        settings=None,
                        audit=lambda *a, **kw: None,
                    )
                    results[tag] = result
                except Exception as exc:
                    errors[tag] = exc

            t1 = threading.Thread(target=do_update, args=("A",))
            t2 = threading.Thread(target=do_update, args=("B",))
            t1.start()
            t2.start()
            t1.join(timeout=10)
            t2.join(timeout=10)

            # Exactly one should succeed and one should get a 409 version conflict.
            self.assertEqual(
                len(results), 1, f"Expected exactly one success, got {len(results)} successes and {len(errors)} errors"
            )
            self.assertEqual(len(errors), 1)
            failing_exc = list(errors.values())[0]
            self.assertEqual(getattr(failing_exc, "status_code", None), 409)


if __name__ == "__main__":
    unittest.main()
