"""Tests for Issue #33 Phase 1 continuity schema and validation behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.main import continuity_read, continuity_upsert
from app.models import ContinuityReadRequest, ContinuityUpsertRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that records committed paths for continuity tests."""

    def __init__(self) -> None:
        """Initialize the fake commit log."""
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the committed file path and message."""
        self.commits.append((str(path), message))
        return True


class TestContinuity33Phase1(unittest.TestCase):
    """Validate the Issue #33 schema and write-time rules."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build repository-rooted settings for continuity tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self) -> dict:
        """Return a valid capsule payload including the Issue #33 fields."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "schema_version": "1.0",
            "subject_kind": "user",
            "subject_id": "stef",
            "updated_at": now,
            "verified_at": now,
            "verification_kind": "self_review",
            "source": {
                "producer": "handoff-hook",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["preserve continuity accuracy"],
                "active_concerns": ["schema drift during additive evolution"],
                "active_constraints": ["do not regress backward compatibility"],
                "open_loops": ["implement issue 33 cleanly"],
                "stance_summary": "Keep the continuity body additive and deterministic.",
                "drift_signals": [],
                "working_hypotheses": ["negative decisions should be structured"],
                "trailing_notes": ["There may be a later need for richer revisit triggers."],
                "curiosity_queue": ["Should negative decisions later grow revisit hints?"],
                "negative_decisions": [
                    {
                        "decision": "Do not broaden #33 into a decision-log subsystem.",
                        "rationale": "That would add workflow complexity beyond an additive continuity-field slice.",
                    }
                ],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _legacy_capsule_payload(self) -> dict:
        """Return a pre-Issue-33 capsule payload without the new fields."""
        capsule = self._capsule_payload()
        capsule["continuity"].pop("trailing_notes")
        capsule["continuity"].pop("curiosity_queue")
        capsule["continuity"].pop("negative_decisions")
        return capsule

    def _upsert(self, repo_root: Path, capsule: dict) -> dict:
        """Upsert one capsule through the route wrapper."""
        settings = self._settings(repo_root)
        gm = _GitManagerStub()
        req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_upsert(req=req, auth=AllowAllAuthStub())

    def test_upsert_persists_issue_33_fields(self) -> None:
        """Upsert should persist the new additive continuity fields unchanged."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            out = self._upsert(repo_root, self._capsule_payload())

            self.assertTrue(out["ok"])
            payload = json.loads((repo_root / "memory" / "continuity" / "user-stef.json").read_text(encoding="utf-8"))
            self.assertEqual(
                payload["continuity"]["trailing_notes"],
                ["There may be a later need for richer revisit triggers."],
            )
            self.assertEqual(
                payload["continuity"]["curiosity_queue"],
                ["Should negative decisions later grow revisit hints?"],
            )
            self.assertEqual(
                payload["continuity"]["negative_decisions"][0]["decision"],
                "Do not broaden #33 into a decision-log subsystem.",
            )

    def test_upsert_rejects_empty_trailing_notes_items(self) -> None:
        """Empty trailing-notes items should fail in the continuity service with HTTP 400."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["trailing_notes"] = [""]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too short in continuity.trailing_notes")

    def test_upsert_rejects_empty_curiosity_queue_items(self) -> None:
        """Empty curiosity items should fail in the continuity service with HTTP 400."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["curiosity_queue"] = [""]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too short in continuity.curiosity_queue")

    def test_upsert_rejects_empty_negative_decision_decision(self) -> None:
        """Empty negative-decision decisions should surface as HTTP 400, not ValidationError."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["negative_decisions"][0]["decision"] = ""
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too short in continuity.negative_decisions.decision")

    def test_upsert_rejects_empty_negative_decision_rationale(self) -> None:
        """Empty negative-decision rationales should surface as HTTP 400, not ValidationError."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["negative_decisions"][0]["rationale"] = ""
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too short in continuity.negative_decisions.rationale")

    def test_upsert_rejects_overlong_trailing_notes_items(self) -> None:
        """Trailing notes longer than 160 chars should fail with the exact detail string."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["trailing_notes"] = ["x" * 161]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too long in continuity.trailing_notes")

    def test_upsert_rejects_overlong_curiosity_queue_items(self) -> None:
        """Curiosity items longer than 120 chars should fail with the exact detail string."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["curiosity_queue"] = ["x" * 121]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too long in continuity.curiosity_queue")

    def test_upsert_rejects_overlong_negative_decision_decision(self) -> None:
        """Overlong negative-decision decisions should fail with the exact detail string."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["negative_decisions"][0]["decision"] = "x" * 161
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too long in continuity.negative_decisions.decision")

    def test_upsert_rejects_overlong_negative_decision_rationale(self) -> None:
        """Overlong negative-decision rationales should fail with the exact detail string."""
        with tempfile.TemporaryDirectory() as td:
            capsule = self._capsule_payload()
            capsule["continuity"]["negative_decisions"][0]["rationale"] = "x" * 241
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=AllowAllAuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too long in continuity.negative_decisions.rationale")

    def test_more_than_max_issue_33_items_fail_model_validation(self) -> None:
        """List-count bounds should still fail at model validation time."""
        capsule = self._capsule_payload()
        capsule["continuity"]["trailing_notes"] = ["a", "b", "c", "d"]
        with self.assertRaises(ValidationError):
            ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]

    def test_legacy_capsules_normalize_issue_33_fields_on_read(self) -> None:
        """Older capsules should remain readable and normalize the new fields to empty lists."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(
                json.dumps(self._legacy_capsule_payload()),
                encoding="utf-8",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=AllowAllAuthStub(),
                )

            self.assertEqual(out["capsule"]["continuity"]["trailing_notes"], [])
            self.assertEqual(out["capsule"]["continuity"]["curiosity_queue"], [])
            self.assertEqual(out["capsule"]["continuity"]["negative_decisions"], [])
