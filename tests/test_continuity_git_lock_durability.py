"""Regression tests for write-before-lock durability gap fixes.

Verifies that:
  1. _persist_active_capsule does not overwrite the file when the git lock
     cannot be acquired (GitLockTimeout / GitLockInfrastructureError).
  2. continuity_archive_service does not leave orphaned archive files when
     the git lock cannot be acquired.
  3. Global FastAPI exception handlers return structured 409/503 responses
     for uncaught git lock exceptions.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.git_locking import GitLockInfrastructureError, GitLockTimeout
from app.main import app
from app.storage import write_text_file
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


# ------------------------------------------------------------------ #
#  Stubs
# ------------------------------------------------------------------ #

class _NoopAudit:
    """Audit callable that does nothing."""

    def __call__(self, _auth: object, _event: str, _detail: dict) -> None:
        pass


@contextmanager
def _raise_git_lock_timeout(*_args, **_kwargs):
    """Context manager that raises GitLockTimeout on entry."""
    raise GitLockTimeout("simulated git lock timeout")
    yield  # noqa: RET503


@contextmanager
def _raise_git_lock_infra(*_args, **_kwargs):
    """Context manager that raises GitLockInfrastructureError on entry."""
    raise GitLockInfrastructureError("simulated git lock infrastructure failure")
    yield  # noqa: RET503


class _Req:
    """Minimal request stub with arbitrary attributes."""

    def __init__(self, **kwargs: object) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


def _make_active_capsule(subject_kind: str = "user", subject_id: str = "u1") -> dict:
    """Return a minimal valid continuity capsule dict."""
    return {
        "schema_type": "continuity_capsule",
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": "2026-03-22T00:00:00Z",
        "verified_at": "2026-03-22T00:00:00Z",
        "source": {
            "producer": "test",
            "update_reason": "manual",
            "inputs": [],
        },
        "continuity": {
            "top_priorities": [],
            "active_concerns": [],
            "active_constraints": [],
            "open_loops": [],
            "drift_signals": [],
            "stance_summary": "test",
        },
        "confidence": {
            "continuity": 0.9,
            "relationship_model": 0.9,
        },
    }


# ------------------------------------------------------------------ #
#  Tests: _persist_active_capsule preserves file on lock failure
# ------------------------------------------------------------------ #


class TestPersistActiveCapsuleLockDurability(unittest.TestCase):
    """_persist_active_capsule must not leave the file overwritten when the
    git lock cannot be acquired."""

    def _run_persist_with_lock_failure(self, lock_mock):
        """Helper: write an original capsule, attempt persist with a failing lock,
        and assert the original content survives."""
        from app.continuity.service import _persist_active_capsule

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            capsule_dir = repo / "memory" / "continuity"
            capsule_dir.mkdir(parents=True)
            capsule_path = capsule_dir / "user-u1.json"

            # Write original durable content
            original = json.dumps({"original": True}, indent=2)
            write_text_file(capsule_path, original)
            original_bytes = capsule_path.read_bytes()

            # Attempt persist with lock that will fail
            new_content = json.dumps(_make_active_capsule(), indent=2)
            with patch("app.continuity.persistence.repository_mutation_lock", lock_mock):
                with self.assertRaises((GitLockTimeout, GitLockInfrastructureError)):
                    _persist_active_capsule(
                        repo_root=repo,
                        gm=SimpleGitManagerStub(repo),
                        path=capsule_path,
                        canonical=new_content,
                        commit_message="test",
                    )

            # Original content must be preserved
            self.assertTrue(capsule_path.exists(), "Capsule file must still exist")
            self.assertEqual(capsule_path.read_bytes(), original_bytes,
                             "Original capsule content must not be overwritten")

    def test_preserves_file_on_git_lock_timeout(self) -> None:
        """GitLockTimeout must not leave the capsule overwritten."""
        self._run_persist_with_lock_failure(_raise_git_lock_timeout)

    def test_preserves_file_on_git_lock_infra_error(self) -> None:
        """GitLockInfrastructureError must not leave the capsule overwritten."""
        self._run_persist_with_lock_failure(_raise_git_lock_infra)


# ------------------------------------------------------------------ #
#  Tests: _persist_active_capsule commit-failure rollback
# ------------------------------------------------------------------ #


class _FailingCommitGM(SimpleGitManagerStub):
    """Git manager stub whose commit_file always raises."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        raise RuntimeError("simulated commit failure")


class _NoChangesGM(SimpleGitManagerStub):
    """Git manager stub whose commit_file returns False (no diff)."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        return False


@contextmanager
def _passthrough_lock(*_args, **_kwargs):
    """Context manager that yields immediately (bypass real locking)."""
    yield


class TestPersistActiveCapsuleCommitFailure(unittest.TestCase):
    """Commit failures in _persist_active_capsule must produce structured
    errors and restore the original file."""

    def test_commit_failure_returns_structured_error(self) -> None:
        """Commit exception → 500 with continuity_persist_commit_failed, file restored."""
        from app.continuity.service import _persist_active_capsule

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            capsule_dir = repo / "memory" / "continuity"
            capsule_dir.mkdir(parents=True)
            capsule_path = capsule_dir / "user-u1.json"

            original = json.dumps({"original": True}, indent=2)
            write_text_file(capsule_path, original)
            original_bytes = capsule_path.read_bytes()

            new_content = json.dumps(_make_active_capsule(), indent=2)
            with patch("app.continuity.persistence.repository_mutation_lock", _passthrough_lock):
                with self.assertRaises(HTTPException) as ctx:
                    _persist_active_capsule(
                        repo_root=repo,
                        gm=_FailingCommitGM(repo),
                        path=capsule_path,
                        canonical=new_content,
                        commit_message="test",
                    )

            self.assertEqual(ctx.exception.status_code, 500)
            detail = ctx.exception.detail
            self.assertEqual(detail["error"]["code"], "continuity_persist_commit_failed")
            # Original content must be restored
            self.assertEqual(capsule_path.read_bytes(), original_bytes)

    def test_commit_and_rollback_failure_returns_rollback_error(self) -> None:
        """Commit + rollback failure → 500 with continuity_persist_rollback_failed."""
        from app.continuity.service import _persist_active_capsule

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            capsule_dir = repo / "memory" / "continuity"
            capsule_dir.mkdir(parents=True)
            capsule_path = capsule_dir / "user-u1.json"

            original = json.dumps({"original": True}, indent=2)
            write_text_file(capsule_path, original)

            new_content = json.dumps(_make_active_capsule(), indent=2)
            with (
                patch("app.continuity.persistence.repository_mutation_lock", _passthrough_lock),
                patch("app.continuity.persistence.write_bytes_file", side_effect=OSError("disk full")),
            ):
                with self.assertRaises(HTTPException) as ctx:
                    _persist_active_capsule(
                        repo_root=repo,
                        gm=_FailingCommitGM(repo),
                        path=capsule_path,
                        canonical=new_content,
                        commit_message="test",
                    )

            self.assertEqual(ctx.exception.status_code, 500)
            detail = ctx.exception.detail
            self.assertEqual(detail["error"]["code"], "continuity_persist_rollback_failed")
            self.assertIn("disk full", detail["error"]["detail"])

    def test_no_changes_is_success(self) -> None:
        """commit_file returning False (no diff) must not trigger rollback."""
        from app.continuity.service import _persist_active_capsule

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            capsule_dir = repo / "memory" / "continuity"
            capsule_dir.mkdir(parents=True)
            capsule_path = capsule_dir / "user-u1.json"

            original = json.dumps({"original": True}, indent=2)
            write_text_file(capsule_path, original)

            new_content = json.dumps(_make_active_capsule(), indent=2)
            with patch("app.continuity.persistence.repository_mutation_lock", _passthrough_lock):
                # Should NOT raise — content is already durable in git
                _persist_active_capsule(
                    repo_root=repo,
                    gm=_NoChangesGM(repo),
                    path=capsule_path,
                    canonical=new_content,
                    commit_message="test",
                )

            # File must have the NEW content, not rolled back to original
            self.assertEqual(capsule_path.read_text(encoding="utf-8"), new_content)


# ------------------------------------------------------------------ #
#  Tests: continuity_archive_service — no orphaned archive on lock failure
# ------------------------------------------------------------------ #


class TestArchiveNoOrphanOnLockFailure(unittest.TestCase):
    """continuity_archive_service must not leave an orphaned archive file
    when the git lock cannot be acquired."""

    def _run_archive_with_lock_failure(self, lock_mock):
        """Helper: set up an active capsule, attempt archive with a failing lock,
        and assert no archive file is created."""
        from app.continuity.service import continuity_archive_service

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Write an active capsule
            capsule = _make_active_capsule()
            rel = "memory/continuity/user-u1.json"
            capsule_path = repo / rel
            capsule_path.parent.mkdir(parents=True, exist_ok=True)
            write_text_file(capsule_path, json.dumps(capsule, indent=2))
            original_bytes = capsule_path.read_bytes()

            archive_dir = repo / "memory" / "continuity" / "archive"

            req = _Req(subject_kind="user", subject_id="u1", reason="test")
            now = datetime.now(timezone.utc).replace(microsecond=0)

            with patch("app.continuity.service.repository_mutation_lock", lock_mock):
                with self.assertRaises((GitLockTimeout, GitLockInfrastructureError)):
                    continuity_archive_service(
                        repo_root=repo,
                        gm=SimpleGitManagerStub(repo),
                        auth=AllowAllAuthStub(),
                        req=req,
                        now=now,
                        audit=_NoopAudit(),
                    )

            # Active capsule must be untouched
            self.assertTrue(capsule_path.exists(), "Active capsule must still exist")
            self.assertEqual(capsule_path.read_bytes(), original_bytes,
                             "Active capsule content must be preserved")

            # No archive file should exist
            if archive_dir.exists():
                archives = list(archive_dir.iterdir())
                self.assertEqual(archives, [],
                                 f"No archive file should exist, but found: {archives}")

    def test_no_orphan_on_git_lock_timeout(self) -> None:
        """GitLockTimeout must not leave an orphaned archive."""
        self._run_archive_with_lock_failure(_raise_git_lock_timeout)

    def test_no_orphan_on_git_lock_infra_error(self) -> None:
        """GitLockInfrastructureError must not leave an orphaned archive."""
        self._run_archive_with_lock_failure(_raise_git_lock_infra)


# ------------------------------------------------------------------ #
#  Tests: global exception handlers return structured responses
# ------------------------------------------------------------------ #


class TestGlobalGitLockExceptionHandlers(unittest.TestCase):
    """Global FastAPI exception handlers must convert uncaught git lock
    exceptions to structured 409/503 responses."""

    def test_git_lock_timeout_returns_409(self) -> None:
        """GitLockTimeout → 409 with structured error body."""
        client = TestClient(app, raise_server_exceptions=False)
        with patch("app.main._services", side_effect=GitLockTimeout("test timeout")):
            response = client.get("/health")

        self.assertEqual(response.status_code, 409)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["operation"], "git_lock")
        self.assertEqual(body["error"]["code"], "git_lock_timeout")

    def test_git_lock_infra_returns_503(self) -> None:
        """GitLockInfrastructureError → 503 with structured error body."""
        client = TestClient(app, raise_server_exceptions=False)
        with patch("app.main._services", side_effect=GitLockInfrastructureError("test infra")):
            response = client.get("/health")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["operation"], "git_lock")
        self.assertEqual(body["error"]["code"], "git_lock_infrastructure_unavailable")


if __name__ == "__main__":
    unittest.main()
