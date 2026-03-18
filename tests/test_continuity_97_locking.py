"""Regression tests for Issue #97 subject-level continuity mutation locking."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.continuity.service import (
    continuity_archive_service,
    continuity_delete_service,
    continuity_revalidate_service,
    continuity_upsert_service,
)
from app.models import (
    ContinuityArchiveRequest,
    ContinuityDeleteRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
)
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _ControlledGitManager(SimpleGitManagerStub):
    """Git stub that can block or fail selected commit messages deterministically."""

    def __init__(self) -> None:
        super().__init__()
        self.file_calls: list[tuple[str, str]] = []
        self.path_calls: list[tuple[list[str], str]] = []
        self._file_controls: dict[str, dict[str, Any]] = {}
        self._path_controls: dict[str, dict[str, Any]] = {}

    def control_file_commit(
        self, message: str, *, fail_with: Exception | None = None
    ) -> tuple[threading.Event, threading.Event]:
        """Return entered/release events for a controlled single-file commit."""
        entered = threading.Event()
        release = threading.Event()
        self._file_controls[message] = {"entered": entered, "release": release, "fail_with": fail_with}
        return entered, release

    def control_path_commit(
        self, message: str, *, fail_with: Exception | None = None
    ) -> tuple[threading.Event, threading.Event]:
        """Return entered/release events for a controlled multi-path commit."""
        entered = threading.Event()
        release = threading.Event()
        self._path_controls[message] = {"entered": entered, "release": release, "fail_with": fail_with}
        return entered, release

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the file commit and optionally block or fail it."""
        self.file_calls.append((str(path), message))
        control = self._file_controls.get(message)
        if control is not None:
            control["entered"].set()
            if not control["release"].wait(timeout=5):
                raise RuntimeError(f"Timed out waiting to release file commit: {message}")
            if control["fail_with"] is not None:
                raise control["fail_with"]
        return True

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record the path commit and optionally block or fail it."""
        self.path_calls.append(([str(path) for path in paths], message))
        control = self._path_controls.get(message)
        if control is not None:
            control["entered"].set()
            if not control["release"].wait(timeout=5):
                raise RuntimeError(f"Timed out waiting to release path commit: {message}")
            if control["fail_with"] is not None:
                raise control["fail_with"]
        return True


class TestContinuitySubjectLocking(unittest.TestCase):
    """Validate same-subject serialization across continuity mutation endpoints."""

    def _capsule_payload(self, *, subject_id: str = "stef", updated_at: str) -> dict[str, Any]:
        """Return a minimal valid continuity capsule payload."""
        return {
            "schema_version": "1.0",
            "subject_kind": "user",
            "subject_id": subject_id,
            "updated_at": updated_at,
            "verified_at": updated_at,
            "verification_kind": "self_review",
            "source": {
                "producer": "handoff-hook",
                "update_reason": "pre_compaction",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": [f"priority at {updated_at}"],
                "active_concerns": [f"concern at {updated_at}"],
                "active_constraints": [f"constraint at {updated_at}"],
                "open_loops": [f"loop at {updated_at}"],
                "stance_summary": f"stance at {updated_at}",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _signals(self, *, observed_at: str) -> list[dict[str, str]]:
        """Return one strong verification signal list for revalidate tests."""
        return [
            {
                "kind": "system_check",
                "source_ref": "memory/logs/system-check.json",
                "observed_at": observed_at,
                "summary": "System check passed.",
            }
        ]

    def _write_active(self, repo_root: Path, payload: dict[str, Any]) -> Path:
        """Write one active capsule directly to disk."""
        path = repo_root / "memory" / "continuity" / f"user-{payload['subject_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _read_active(self, repo_root: Path, *, subject_id: str = "stef") -> dict[str, Any]:
        """Load one active capsule payload from disk."""
        path = repo_root / "memory" / "continuity" / f"user-{subject_id}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def _run_thread(self, target: Any) -> tuple[threading.Thread, dict[str, Any], dict[str, BaseException]]:
        """Start one worker thread and capture exactly one result or exception."""
        result: dict[str, Any] = {}
        error: dict[str, BaseException] = {}

        def runner() -> None:
            try:
                result["value"] = target()
            except BaseException as exc:  # noqa: BLE001 - tests capture service failures directly
                error["value"] = exc

        thread = threading.Thread(target=runner)
        thread.start()
        return thread, result, error

    def test_failed_upsert_cannot_rollback_over_later_successful_upsert(self) -> None:
        """A failed upsert must not restore stale bytes after a newer upsert commits."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _ControlledGitManager()
            auth = AllowAllAuthStub()
            base = self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:00:00Z")
            self._write_active(repo_root, base)
            entered_a, release_a = gm.control_file_commit("continuity: concurrent A", fail_with=RuntimeError("boom A"))
            entered_b, release_b = gm.control_file_commit("continuity: concurrent B")

            upsert_a = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:01:00Z"),
                commit_message="continuity: concurrent A",
            )
            upsert_b = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:02:00Z"),
                commit_message="continuity: concurrent B",
            )

            thread_a, _result_a, error_a = self._run_thread(
                lambda: continuity_upsert_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=upsert_a,
                    audit=lambda *_args: None,
                )
            )
            self.assertTrue(entered_a.wait(timeout=5))
            thread_b, result_b, error_b = self._run_thread(
                lambda: continuity_upsert_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=upsert_b,
                    audit=lambda *_args: None,
                )
            )
            self.assertFalse(entered_b.wait(timeout=0.2))
            release_a.set()
            self.assertTrue(entered_b.wait(timeout=5))
            release_b.set()
            thread_a.join(timeout=5)
            thread_b.join(timeout=5)

            self.assertIn("value", error_a)
            self.assertNotIn("value", error_b)
            self.assertTrue(result_b["value"]["ok"])
            active = self._read_active(repo_root)
            self.assertEqual(active["updated_at"], "2026-03-18T10:02:00Z")
            self.assertEqual(active["continuity"]["top_priorities"], ["priority at 2026-03-18T10:02:00Z"])

    def test_failed_upsert_cannot_rollback_over_later_revalidate(self) -> None:
        """A failed upsert must not erase a later same-subject revalidate result."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _ControlledGitManager()
            auth = AllowAllAuthStub()
            base = self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:00:00Z")
            self._write_active(repo_root, base)
            entered_upsert, release_upsert = gm.control_file_commit(
                "continuity: concurrent upsert", fail_with=RuntimeError("boom upsert")
            )
            entered_revalidate, release_revalidate = gm.control_file_commit("continuity: revalidate user stef")

            upsert_req = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:01:00Z"),
                commit_message="continuity: concurrent upsert",
            )
            revalidate_req = ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="confirm",
                signals=self._signals(observed_at="2026-03-18T10:02:00Z"),
            )

            thread_upsert, _upsert_result, upsert_error = self._run_thread(
                lambda: continuity_upsert_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=upsert_req,
                    audit=lambda *_args: None,
                )
            )
            self.assertTrue(entered_upsert.wait(timeout=5))
            thread_revalidate, revalidate_result, revalidate_error = self._run_thread(
                lambda: continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=revalidate_req,
                    audit=lambda *_args: None,
                )
            )
            self.assertFalse(entered_revalidate.wait(timeout=0.2))
            release_upsert.set()
            self.assertTrue(entered_revalidate.wait(timeout=5))
            release_revalidate.set()
            thread_upsert.join(timeout=5)
            thread_revalidate.join(timeout=5)

            self.assertIn("value", upsert_error)
            self.assertNotIn("value", revalidate_error)
            self.assertEqual(revalidate_result["value"]["outcome"], "confirm")
            active = self._read_active(repo_root)
            self.assertEqual(active["verification_state"]["status"], "system_confirmed")
            self.assertEqual(active["capsule_health"]["status"], "healthy")

    def test_failed_archive_cannot_rollback_over_later_upsert(self) -> None:
        """A failed archive must not restore stale bytes after a later upsert succeeds."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _ControlledGitManager()
            auth = AllowAllAuthStub()
            base = self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:00:00Z")
            self._write_active(repo_root, base)
            entered_archive, release_archive = gm.control_path_commit(
                "continuity: archive user stef", fail_with=RuntimeError("boom archive")
            )
            entered_upsert, release_upsert = gm.control_file_commit("continuity: after archive")

            archive_req = ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="cleanup")
            upsert_req = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:03:00Z"),
                commit_message="continuity: after archive",
            )

            thread_archive, _archive_result, archive_error = self._run_thread(
                lambda: continuity_archive_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=archive_req,
                    now=datetime(2026, 3, 18, 10, 1, tzinfo=timezone.utc),
                    audit=lambda *_args: None,
                )
            )
            self.assertTrue(entered_archive.wait(timeout=5))
            thread_upsert, upsert_result, upsert_error = self._run_thread(
                lambda: continuity_upsert_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=upsert_req,
                    audit=lambda *_args: None,
                )
            )
            self.assertFalse(entered_upsert.wait(timeout=0.2))
            release_archive.set()
            self.assertTrue(entered_upsert.wait(timeout=5))
            release_upsert.set()
            thread_archive.join(timeout=5)
            thread_upsert.join(timeout=5)

            self.assertIn("value", archive_error)
            self.assertNotIn("value", upsert_error)
            self.assertTrue(upsert_result["value"]["ok"])
            active = self._read_active(repo_root)
            self.assertEqual(active["updated_at"], "2026-03-18T10:03:00Z")
            archive_dir = repo_root / "memory" / "continuity" / "archive"
            archived = list(archive_dir.glob("user-stef-*.json")) if archive_dir.exists() else []
            self.assertEqual(archived, [])

    def test_failed_delete_cannot_rollback_over_later_upsert(self) -> None:
        """A failed delete must not restore stale bytes after a later upsert succeeds."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _ControlledGitManager()
            auth = AllowAllAuthStub()
            base = self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:00:00Z")
            self._write_active(repo_root, base)
            entered_delete, release_delete = gm.control_path_commit(
                "continuity: delete user stef - cleanup", fail_with=RuntimeError("boom delete")
            )
            entered_upsert, release_upsert = gm.control_file_commit("continuity: after delete")

            delete_req = ContinuityDeleteRequest(
                subject_kind="user",
                subject_id="stef",
                delete_active=True,
                reason="cleanup",
            )
            upsert_req = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_id="stef", updated_at="2026-03-18T10:04:00Z"),
                commit_message="continuity: after delete",
            )

            thread_delete, _delete_result, delete_error = self._run_thread(
                lambda: continuity_delete_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=delete_req,
                    audit=lambda *_args: None,
                )
            )
            self.assertTrue(entered_delete.wait(timeout=5))
            thread_upsert, upsert_result, upsert_error = self._run_thread(
                lambda: continuity_upsert_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=auth,
                    req=upsert_req,
                    audit=lambda *_args: None,
                )
            )
            self.assertFalse(entered_upsert.wait(timeout=0.2))
            release_delete.set()
            self.assertTrue(entered_upsert.wait(timeout=5))
            release_upsert.set()
            thread_delete.join(timeout=5)
            thread_upsert.join(timeout=5)

            self.assertIn("value", delete_error)
            self.assertNotIn("value", upsert_error)
            self.assertTrue(upsert_result["value"]["ok"])
            active = self._read_active(repo_root)
            self.assertEqual(active["updated_at"], "2026-03-18T10:04:00Z")


if __name__ == "__main__":
    unittest.main()
