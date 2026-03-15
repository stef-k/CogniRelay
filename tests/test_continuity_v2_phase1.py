"""Tests for continuity-state V2 Phase 1 schema and validation behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.main import continuity_upsert
from app.models import ContinuityUpsertRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that records committed files for continuity tests."""

    def __init__(self) -> None:
        """Initialize the fake commit ledger."""
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record a committed file path and report success."""
        self.commits.append((str(path), message))
        return True


class TestContinuityV2Phase1(unittest.TestCase):
    """Validate the V2 schema and write-time validation rules."""

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

    def _capsule_payload(self, *, update_reason: str = "pre_compaction") -> dict:
        """Return a valid V2-ready capsule payload with optional overrides."""
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
                "update_reason": update_reason,
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["continuity across compaction"],
                "active_concerns": ["loss of nuance during summarization"],
                "active_constraints": ["do not regress current workflows"],
                "open_loops": ["finish continuity-state spec"],
                "stance_summary": "Preserve continuity quality while staying backward compatible.",
                "drift_signals": [],
                "session_trajectory": ["started with review, moved into implementation"],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def test_upsert_persists_session_trajectory(self) -> None:
        """Upsert should accept and persist the optional V2 session trajectory field."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=self._capsule_payload())  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            payload = json.loads((repo_root / "memory" / "continuity" / "user-stef.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["continuity"]["session_trajectory"], ["started with review, moved into implementation"])

    def test_session_trajectory_item_over_80_chars_is_rejected(self) -> None:
        """Write validation should reject session trajectory items longer than 80 chars."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            capsule = self._capsule_payload()
            capsule["continuity"]["session_trajectory"] = ["x" * 81]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Value too long in continuity.session_trajectory")

    def test_session_trajectory_more_than_five_items_fails_model_validation(self) -> None:
        """The V2 schema should reject session trajectory lists longer than five items."""
        capsule = self._capsule_payload()
        capsule["continuity"]["session_trajectory"] = ["a", "b", "c", "d", "e", "f"]
        with self.assertRaises(ValidationError):
            ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]

    def test_interaction_boundary_requires_metadata_kind(self) -> None:
        """Interaction-boundary updates should require the boundary-kind metadata key."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            capsule = self._capsule_payload(update_reason="interaction_boundary")
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "metadata.interaction_boundary_kind is required when source.update_reason=interaction_boundary")

    def test_interaction_boundary_rejects_invalid_kind(self) -> None:
        """Interaction-boundary updates should reject unknown boundary-kind values."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            capsule = self._capsule_payload(update_reason="interaction_boundary")
            capsule["metadata"] = {"interaction_boundary_kind": "invalid"}
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Invalid metadata.interaction_boundary_kind")

    def test_interaction_boundary_kind_rejected_for_other_update_reasons(self) -> None:
        """Boundary-kind metadata should be rejected when update_reason is not interaction_boundary."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            capsule = self._capsule_payload(update_reason="manual")
            capsule["metadata"] = {"interaction_boundary_kind": "manual_checkpoint"}
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(
                cm.exception.detail,
                "metadata.interaction_boundary_kind requires source.update_reason=interaction_boundary",
            )

    def test_context_retrieve_accepts_v2_selector_fields(self) -> None:
        """The V2 retrieval model should accept selectors and continuity_max_capsules."""
        req = ContextRetrieveRequest(
            task="resume",
            continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            continuity_max_capsules=4,
        )
        self.assertEqual(req.continuity_selectors[0].subject_kind, "user")
        self.assertEqual(req.continuity_max_capsules, 4)

    def test_context_retrieve_rejects_more_than_four_selectors(self) -> None:
        """The V2 retrieval model should reject selector lists above the bounded limit."""
        with self.assertRaises(ValidationError):
            ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                    {"subject_kind": "user", "subject_id": "c"},
                    {"subject_kind": "user", "subject_id": "d"},
                    {"subject_kind": "user", "subject_id": "e"},
                ],
            )
