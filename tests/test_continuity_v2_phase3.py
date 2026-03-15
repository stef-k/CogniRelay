"""Tests for continuity-state V2 Phase 3 read/list lifecycle behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_list_service
from app.main import continuity_list, continuity_read
from app.models import ContinuityListRequest, ContinuityReadRequest


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


class _SelectiveReadAuth(_AuthStub):
    """Auth stub that denies reads for configured path suffixes."""

    def __init__(self, denied_suffixes: set[str]) -> None:
        """Store denied path suffixes for list-filter tests."""
        self.denied_suffixes = denied_suffixes

    def require_read_path(self, path: str) -> None:
        """Reject reads for denied suffixes and allow the rest."""
        for suffix in self.denied_suffixes:
            if path.endswith(suffix):
                raise HTTPException(status_code=403, detail="forbidden")


class _GitManagerStub:
    """Git manager stub used to satisfy the service bundle patch."""

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestContinuityV2Phase3(unittest.TestCase):
    """Validate the Phase 3 continuity read/list contract."""

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

    def _capsule_payload(
        self,
        *,
        subject_kind: str,
        subject_id: str,
        verified_at: str | None = None,
        include_freshness: bool = True,
    ) -> dict:
        """Return a valid capsule payload with optional freshness metadata."""
        now = verified_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        payload = {
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
        }
        if include_freshness:
            payload["freshness"] = {"freshness_class": "situational"}
        return payload

    def _normalized(self, subject_id: str) -> str:
        """Return the expected normalized file key for simple test IDs."""
        return subject_id.strip().lower().replace(" ", "-")

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict | None = None) -> None:
        """Write one active continuity capsule to the expected repository path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        capsule = payload or self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
        (continuity_dir / f"{subject_kind}-{self._normalized(subject_id)}.json").write_text(json.dumps(capsule), encoding="utf-8")

    def test_continuity_read_returns_active_capsule(self) -> None:
        """Read should return the exact active capsule payload and path."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef")
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(req=ContinuityReadRequest(subject_kind="user", subject_id="stef"), auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["path"], "memory/continuity/user-stef.json")
            self.assertFalse(out["archived"])
            self.assertEqual(out["capsule"]["subject_id"], "stef")

    def test_continuity_read_missing_capsule_raises_404(self) -> None:
        """Read should return 404 when the active capsule does not exist."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_read(req=ContinuityReadRequest(subject_kind="user", subject_id="missing"), auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 404)

    def test_continuity_list_sorts_and_counts_post_limit(self) -> None:
        """List should sort by raw subject tuple and report the post-limit count."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="zeta")
            self._write_capsule(repo_root, subject_kind="thread", subject_id="alpha")
            self._write_capsule(repo_root, subject_kind="user", subject_id="beta")
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(req=ContinuityListRequest(limit=2), auth=_AuthStub())

            self.assertEqual(out["count"], 2)
            self.assertEqual(
                [(item["subject_kind"], item["subject_id"]) for item in out["capsules"]],
                [("thread", "alpha"), ("user", "beta")],
            )

    def test_continuity_list_skips_invalid_archive_and_unauthorized_entries(self) -> None:
        """List should skip invalid files, archive entries, and unreadable active paths."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_capsule(repo_root, subject_kind="user", subject_id="allowed")
            self._write_capsule(repo_root, subject_kind="user", subject_id="blocked")
            continuity_dir = repo_root / "memory" / "continuity"
            (continuity_dir / "user-invalid.json").write_text("{not-json", encoding="utf-8")
            archive_dir = continuity_dir / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            (archive_dir / "user-archived-20260315T143022Z.json").write_text("{}", encoding="utf-8")

            events: list[tuple[str, dict]] = []
            out = continuity_list_service(
                repo_root=repo_root,
                auth=_SelectiveReadAuth({"user-blocked.json"}),
                req=ContinuityListRequest(limit=10),
                now=datetime.now(timezone.utc),
                audit=lambda _auth, event, detail: events.append((event, detail)),
            )

            self.assertEqual(out["count"], 1)
            self.assertEqual(out["capsules"][0]["subject_id"], "allowed")
            self.assertEqual(events[0][0], "continuity_list")

    def test_continuity_list_reports_phase_and_null_freshness_class(self) -> None:
        """List summaries should compute phase and allow null freshness_class."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            old_verified = (datetime.now(timezone.utc) - timedelta(days=40)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="stale-user",
                payload=self._capsule_payload(subject_kind="user", subject_id="stale-user", verified_at=old_verified),
            )
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="no-freshness",
                payload=self._capsule_payload(subject_kind="user", subject_id="no-freshness", include_freshness=False),
            )

            out = continuity_list_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityListRequest(limit=10),
                now=datetime.now(timezone.utc),
                audit=lambda *_args: None,
            )

            by_id = {item["subject_id"]: item for item in out["capsules"]}
            self.assertEqual(by_id["stale-user"]["phase"], "stale_soft")
            self.assertEqual(by_id["stale-user"]["freshness_class"], "situational")
            self.assertIsNone(by_id["no-freshness"]["freshness_class"])
