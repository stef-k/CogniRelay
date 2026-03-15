"""Tests for continuity-state V2 Phase 4 archive lifecycle behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_archive_service, continuity_list_service, continuity_read_service
from app.main import continuity_archive
from app.models import ContinuityArchiveRequest, ContinuityListRequest, ContinuityReadRequest


class _AuthStub:
    """Auth stub that permits all scopes used by continuity tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None

    def require_write_path(self, _path: str) -> None:
        """Accept any requested write path for test purposes."""
        return None


class _GitManagerStub:
    """Git manager stub that records archive commit-path calls."""

    def __init__(self) -> None:
        """Initialize the stubbed commit call log."""
        self.commit_calls: list[tuple[list[str], str]] = []

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record the multi-path commit request and report success."""
        self.commit_calls.append(([str(path) for path in paths], message))
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class _FailingGitManagerStub(_GitManagerStub):
    """Git manager stub that fails while archiving staged paths."""

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record the attempted commit and then simulate git failure."""
        super().commit_paths(paths, message)
        raise RuntimeError("git commit failed")


class TestContinuityV2Phase4(unittest.TestCase):
    """Validate the Phase 4 archive lifecycle contract."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build a settings object rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self, *, subject_kind: str, subject_id: str) -> dict:
        """Return a valid capsule payload ready for archiving."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str) -> None:
        """Write one active continuity capsule to the expected repository path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        normalized = subject_id.strip().lower().replace(" ", "-")
        payload = self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
        (continuity_dir / f"{subject_kind}-{normalized}.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_archive_route_writes_envelope_and_removes_active_file(self) -> None:
        """Archive should write the envelope, remove the active file, and return both paths."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef")
            archive_now = datetime(2026, 3, 15, 14, 30, 22, tzinfo=timezone.utc)

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = archive_now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                out = continuity_archive(
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="superseded by new handoff"),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            self.assertEqual(out["removed_active_path"], "memory/continuity/user-stef.json")
            self.assertEqual(out["archived_path"], "memory/continuity/archive/user-stef-20260315T143022Z.json")

            active_path = repo_root / "memory" / "continuity" / "user-stef.json"
            archive_path = repo_root / "memory" / "continuity" / "archive" / "user-stef-20260315T143022Z.json"
            self.assertFalse(active_path.exists())
            self.assertTrue(archive_path.exists())

            envelope = json.loads(archive_path.read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema_type"], "continuity_archive_envelope")
            self.assertEqual(envelope["schema_version"], "1.0")
            self.assertEqual(envelope["archived_by"], "peer-test")
            self.assertEqual(envelope["active_path"], "memory/continuity/user-stef.json")
            self.assertEqual(envelope["capsule"]["subject_id"], "stef")

            self.assertEqual(
                gm.commit_calls,
                [
                    (
                        [
                            str(archive_path),
                            str(active_path),
                        ],
                        "continuity: archive user stef",
                    )
                ],
            )

    def test_archive_is_not_idempotent_after_active_file_is_removed(self) -> None:
        """Archiving the same selector twice should return 404 on the second call."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef")
            archive_now = datetime(2026, 3, 15, 14, 30, 22, tzinfo=timezone.utc)

            continuity_archive_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="superseded by new handoff"),
                now=archive_now,
                audit=lambda *_args: None,
            )

            with self.assertRaises(HTTPException) as cm:
                continuity_archive_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=_AuthStub(),
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="try again"),
                    now=archive_now,
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 404)

    def test_archive_removes_selector_from_read_and_list_views(self) -> None:
        """Archived capsules should disappear from active read and list surfaces."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef")
            continuity_archive_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="rotated out"),
                now=datetime(2026, 3, 15, 14, 30, 22, tzinfo=timezone.utc),
                audit=lambda *_args: None,
            )

            with self.assertRaises(HTTPException) as read_cm:
                continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    audit=lambda *_args: None,
                )

            self.assertEqual(read_cm.exception.status_code, 404)

            listed = continuity_list_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityListRequest(limit=10),
                now=datetime.now(timezone.utc),
                audit=lambda *_args: None,
            )
            self.assertEqual(listed["count"], 0)
            self.assertEqual(listed["capsules"], [])

    def test_archive_failure_restores_active_capsule_without_data_loss(self) -> None:
        """Archive failures should preserve the active capsule and discard the archive file."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _FailingGitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef")

            with self.assertRaises(RuntimeError):
                continuity_archive_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=_AuthStub(),
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="superseded"),
                    now=datetime(2026, 3, 15, 14, 30, 22, tzinfo=timezone.utc),
                    audit=lambda *_args: None,
                )

            active_path = repo_root / "memory" / "continuity" / "user-stef.json"
            archive_path = repo_root / "memory" / "continuity" / "archive" / "user-stef-20260315T143022Z.json"
            self.assertTrue(active_path.exists())
            self.assertFalse(archive_path.exists())
