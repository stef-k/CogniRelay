"""Tests for Phase 4 retention classes and delete lifecycle behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.continuity.service import (
    CONTINUITY_ARCHIVE_SCHEMA_TYPE,
    CONTINUITY_ARCHIVE_SCHEMA_VERSION,
    continuity_delete_service,
)
from app.main import continuity_delete, continuity_list
from app.models import ContinuityDeleteRequest, ContinuityListRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by Phase 4 delete tests."""


class _SelectiveDeleteAuth(_AuthStub):
    """Auth stub that denies writes for selected suffixes."""

    def __init__(self, denied_suffixes: set[str]) -> None:
        """Store denied suffixes for delete authorization tests."""
        super().__init__()
        self.denied_suffixes = denied_suffixes

    def require_write_path(self, path: str) -> None:
        """Reject writes for configured path suffixes."""
        for suffix in self.denied_suffixes:
            if path.endswith(suffix):
                raise HTTPException(status_code=403, detail="forbidden")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records delete multi-path commits."""

    def __init__(self) -> None:
        """Initialize the commit record list."""
        self.commit_paths_calls: list[tuple[list[str], str]] = []

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record the delete commit request and report success."""
        self.commit_paths_calls.append(([str(path) for path in paths], message))
        return True


class _UnlinkFailurePath(type(Path())):
    """Concrete path subclass used to inject delete unlink failures."""


class TestContinuityPhase4Phase3(unittest.TestCase):
    """Validate Phase 4 retention and delete behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build settings rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self, *, subject_kind: str, subject_id: str, verified_at: str | None = None) -> dict:
        """Return a continuity capsule payload."""
        now = verified_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": now,
            "verified_at": now,
            "verification_kind": "self_review",
            "source": {
                "producer": "handoff-hook",
                "update_reason": "pre_compaction",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": [f"priority for {subject_id}"],
                "active_concerns": [f"concern for {subject_id}"],
                "active_constraints": [f"constraint for {subject_id}"],
                "open_loops": [f"loop for {subject_id}"],
                "stance_summary": f"stance for {subject_id}",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _write_active(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict | None = None) -> Path:
        """Write one active continuity capsule."""
        path = repo_root / "memory" / "continuity" / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload or self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)), encoding="utf-8")
        return path

    def _write_fallback(self, repo_root: Path, *, subject_kind: str, subject_id: str, capsule: dict) -> Path:
        """Write one fallback snapshot envelope."""
        path = repo_root / "memory" / "continuity" / "fallback" / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_type": "continuity_fallback_snapshot",
                    "schema_version": "1.0",
                    "captured_at": capsule["updated_at"],
                    "source_path": f"memory/continuity/{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json",
                    "verification_status": "system_confirmed",
                    "health_status": "healthy",
                    "capsule": capsule,
                }
            ),
            encoding="utf-8",
        )
        return path

    def _write_archive(self, repo_root: Path, *, subject_kind: str, subject_id: str, capsule: dict, archived_at: datetime) -> Path:
        """Write one archive envelope with a deterministic timestamp."""
        ts = archived_at.strftime("%Y%m%dT%H%M%SZ")
        path = repo_root / "memory" / "continuity" / "archive" / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}-{ts}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_type": CONTINUITY_ARCHIVE_SCHEMA_TYPE,
                    "schema_version": CONTINUITY_ARCHIVE_SCHEMA_VERSION,
                    "archived_at": archived_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "archived_by": "peer-test",
                    "reason": "cleanup",
                    "active_path": f"memory/continuity/{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json",
                    "capsule": capsule,
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_list_includes_active_fallback_and_archived_with_retention_classes(self) -> None:
        """Extended list mode should surface all artifact states with deterministic retention classes."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            active_capsule = self._capsule_payload(subject_kind="user", subject_id="alpha")
            fallback_capsule = self._capsule_payload(subject_kind="user", subject_id="beta")
            archive_capsule = self._capsule_payload(subject_kind="user", subject_id="gamma")
            self._write_active(repo_root, subject_kind="user", subject_id="alpha", payload=active_capsule)
            self._write_fallback(repo_root, subject_kind="user", subject_id="beta", capsule=fallback_capsule)
            self._write_archive(repo_root, subject_kind="user", subject_id="gamma", capsule=archive_capsule, archived_at=now - timedelta(days=10))
            self._write_archive(repo_root, subject_kind="user", subject_id="delta", capsule=self._capsule_payload(subject_kind="user", subject_id="delta"), archived_at=now - timedelta(days=120))

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                out = continuity_list(
                    req=ContinuityListRequest(limit=10, include_fallback=True, include_archived=True),
                    auth=_AuthStub(),
                )

            self.assertEqual(
                [(row["subject_id"], row["artifact_state"], row["retention_class"]) for row in out["capsules"]],
                [
                    ("alpha", "active", "active"),
                    ("beta", "fallback", "fallback"),
                    ("delta", "archived", "archive_stale"),
                    ("gamma", "archived", "archive_recent"),
                ],
            )

    def test_delete_validation_rejects_requests_without_any_delete_flags(self) -> None:
        """Delete should reject empty flag selections."""
        with self.assertRaises(ValidationError) as cm:
            ContinuityDeleteRequest(subject_kind="user", subject_id="stef", reason="cleanup")

        self.assertIn("at least one", str(cm.exception))

    def test_delete_removes_active_fallback_and_all_matching_archives_in_one_commit(self) -> None:
        """Delete should remove the selected active, fallback, and matching archive paths together."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            active = self._write_active(repo_root, subject_kind="user", subject_id="stef", payload=capsule)
            fallback = self._write_fallback(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)
            archive_one = self._write_archive(
                repo_root,
                subject_kind="user",
                subject_id="stef",
                capsule=capsule,
                archived_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            )
            archive_two = self._write_archive(
                repo_root,
                subject_kind="user",
                subject_id="stef",
                capsule=capsule,
                archived_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
            )
            other_capsule = self._capsule_payload(subject_kind="user", subject_id="stef-v2")
            archive_other = self._write_archive(
                repo_root,
                subject_kind="user",
                subject_id="stef-v2",
                capsule=other_capsule,
                archived_at=datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc),
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_delete(
                    req=ContinuityDeleteRequest(
                        subject_kind="user",
                        subject_id="stef",
                        delete_active=True,
                        delete_fallback=True,
                        delete_archive=True,
                        reason="cleanup",
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            self.assertEqual(sorted(out["deleted_paths"]), sorted([
                "memory/continuity/user-stef.json",
                "memory/continuity/fallback/user-stef.json",
                "memory/continuity/archive/user-stef-20260315T120000Z.json",
                "memory/continuity/archive/user-stef-20260314T120000Z.json",
            ]))
            self.assertEqual(out["missing_paths"], [])
            self.assertFalse(active.exists())
            self.assertFalse(fallback.exists())
            self.assertFalse(archive_one.exists())
            self.assertFalse(archive_two.exists())
            self.assertTrue(archive_other.exists())
            self.assertEqual(len(gm.commit_paths_calls), 1)
            staged_paths, commit_message = gm.commit_paths_calls[0]
            self.assertEqual(sorted(staged_paths), sorted([str(active), str(fallback), str(archive_one), str(archive_two)]))
            self.assertEqual(commit_message, "continuity: delete user stef - cleanup")

    def test_delete_archive_match_requires_full_timestamp_suffix(self) -> None:
        """Archive deletion should not capture similarly prefixed subjects that start with digits."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            archive_target = self._write_archive(
                repo_root,
                subject_kind="user",
                subject_id="a",
                capsule=self._capsule_payload(subject_kind="user", subject_id="a"),
                archived_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            )
            archive_other = self._write_archive(
                repo_root,
                subject_kind="user",
                subject_id="a-1abc",
                capsule=self._capsule_payload(subject_kind="user", subject_id="a-1abc"),
                archived_at=datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc),
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_delete(
                    req=ContinuityDeleteRequest(
                        subject_kind="user",
                        subject_id="a",
                        delete_archive=True,
                        reason="cleanup",
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            self.assertEqual(out["deleted_paths"], ["memory/continuity/archive/user-a-20260315T120000Z.json"])
            self.assertFalse(archive_target.exists())
            self.assertTrue(archive_other.exists())

    def test_delete_returns_noop_success_when_only_missing_paths_remain(self) -> None:
        """Delete should succeed without a git commit when nothing exists to remove."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()

            out = continuity_delete_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityDeleteRequest(
                    subject_kind="user",
                    subject_id="stef",
                    delete_active=True,
                    delete_fallback=True,
                    delete_archive=True,
                    reason="cleanup",
                ),
                audit=lambda *_args: None,
            )

            self.assertTrue(out["ok"])
            self.assertEqual(out["deleted_paths"], [])
            self.assertEqual(
                sorted(out["missing_paths"]),
                sorted([
                    "memory/continuity/user-stef.json",
                    "memory/continuity/fallback/user-stef.json",
                ]),
            )
            self.assertEqual(gm.commit_paths_calls, [])

    def test_delete_fails_entirely_when_any_requested_path_is_unauthorized(self) -> None:
        """Delete should fail hard instead of partially deleting authorized subsets."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_active(repo_root, subject_kind="user", subject_id="stef", payload=capsule)
            self._write_fallback(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)

            with self.assertRaises(HTTPException) as cm:
                continuity_delete_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=_SelectiveDeleteAuth({"fallback/user-stef.json"}),
                    req=ContinuityDeleteRequest(
                        subject_kind="user",
                        subject_id="stef",
                        delete_active=True,
                        delete_fallback=True,
                        reason="cleanup",
                    ),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 403)
            self.assertEqual(gm.commit_paths_calls, [])

    def test_delete_unlink_failure_restores_prior_paths_and_returns_structured_500(self) -> None:
        """Delete should restore already removed files when a later unlink fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            active = self._write_active(repo_root, subject_kind="user", subject_id="stef", payload=capsule)
            fallback = self._write_fallback(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)

            from app.continuity import service as continuity_service

            original_safe_path = continuity_service.safe_path

            def failing_safe_path(root: Path, rel: str) -> Path:
                path = original_safe_path(root, rel)
                if rel == "memory/continuity/fallback/user-stef.json":
                    class _FailingFallbackPath(_UnlinkFailurePath):
                        def unlink(self, missing_ok: bool = False) -> None:
                            raise PermissionError("cannot unlink fallback")

                    return _FailingFallbackPath(path)
                return path

            with patch("app.continuity.service.safe_path", side_effect=failing_safe_path):
                with self.assertRaises(HTTPException) as cm:
                    continuity_delete_service(
                        repo_root=repo_root,
                        gm=gm,
                        auth=_AuthStub(),
                        req=ContinuityDeleteRequest(
                            subject_kind="user",
                            subject_id="stef",
                            delete_active=True,
                            delete_fallback=True,
                            reason="cleanup",
                        ),
                        audit=lambda *_args: None,
                    )

            self.assertEqual(cm.exception.status_code, 500)
            self.assertTrue(active.exists())
            self.assertTrue(fallback.exists())
            self.assertEqual(gm.commit_paths_calls, [])
