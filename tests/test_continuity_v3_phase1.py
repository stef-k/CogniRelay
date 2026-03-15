"""Tests for continuity-state V3 Phase 1 schema and validation behavior."""

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
    _strongest_signal_kind,
    _validate_candidate_selector_match,
    _validate_verification_signals,
)
from app.main import continuity_upsert
from app.models import (
    ContinuityCompareRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
)
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


class _FailingGitManagerStub(_GitManagerStub):
    """Git manager stub that fails commits after the file write step."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the attempted commit and then fail."""
        super().commit_file(path, message)
        raise RuntimeError("git commit failed")


class TestContinuityV3Phase1(unittest.TestCase):
    """Validate the V3 schema and write-time validation rules."""

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

    def _capsule_payload(self) -> dict:
        """Return a valid continuity capsule payload with optional V3 fields."""
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
                "update_reason": "pre_compaction",
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

    def _signals_payload(self) -> list[dict]:
        """Return a valid ordered V3 verification signal list."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return [
            {
                "kind": "self_review",
                "source_ref": "memory/logs/self-check-1.json",
                "observed_at": now,
                "summary": "Self review completed.",
            },
            {
                "kind": "system_check",
                "source_ref": "memory/logs/system-check-1.json",
                "observed_at": now,
                "summary": "System check passed.",
            },
        ]

    def test_upsert_strips_v3_verification_fields_before_persisting(self) -> None:
        """Upsert should remove V3 verification-only fields before validation and write."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            capsule = self._capsule_payload()
            capsule["verification_state"] = {
                "status": "system_confirmed",
                "last_revalidated_at": capsule["verified_at"],
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/logs/system-check-1.json"],
            }
            capsule["capsule_health"] = {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": capsule["verified_at"],
            }
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            persisted = json.loads((repo_root / "memory" / "continuity" / "user-stef.json").read_text(encoding="utf-8"))
            self.assertNotIn("verification_state", persisted)
            self.assertNotIn("capsule_health", persisted)

    def test_compare_request_accepts_candidate_and_signals(self) -> None:
        """The V3 compare request model should accept a candidate capsule and signals."""
        req = ContinuityCompareRequest(
            subject_kind="user",
            subject_id="stef",
            candidate_capsule=self._capsule_payload(),
            signals=self._signals_payload(),
        )
        self.assertEqual(req.candidate_capsule.subject_id, "stef")
        self.assertEqual(req.signals[1].kind, "system_check")

    def test_revalidate_request_requires_non_empty_signals(self) -> None:
        """The V3 revalidate request model should reject empty signal arrays."""
        with self.assertRaises(ValidationError):
            ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="confirm",
                signals=[],
            )

    def test_verification_signals_require_utc_timestamps(self) -> None:
        """Signal validation should reject non-UTC or invalid timestamps."""
        req = ContinuityCompareRequest(
            subject_kind="user",
            subject_id="stef",
            candidate_capsule=self._capsule_payload(),
            signals=[
                {
                    "kind": "self_review",
                    "source_ref": "memory/logs/self-check-1.json",
                    "observed_at": "2026-03-15T12:00:00+02:00",
                    "summary": "Self review completed.",
                }
            ],
        )
        with self.assertRaises(HTTPException) as cm:
            _validate_verification_signals(req.signals)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.detail, "Timestamp must be UTC for signals.observed_at")

    def test_candidate_selector_match_uses_normalized_subject_identity(self) -> None:
        """Candidate identity checks should use the same normalized selector rules as V2."""
        candidate = self._capsule_payload()
        candidate["subject_id"] = "My-Task"
        req = ContinuityCompareRequest(
            subject_kind="user",
            subject_id="my task",
            candidate_capsule={**candidate, "subject_kind": "user"},
            signals=self._signals_payload(),
        )
        _validate_candidate_selector_match("user", "my task", req.candidate_capsule)

    def test_candidate_selector_match_rejects_wrong_subject(self) -> None:
        """Candidate identity checks should reject mismatched subjects."""
        req = ContinuityCompareRequest(
            subject_kind="user",
            subject_id="stef",
            candidate_capsule={**self._capsule_payload(), "subject_kind": "peer", "subject_id": "other"},
            signals=self._signals_payload(),
        )
        with self.assertRaises(HTTPException) as cm:
            _validate_candidate_selector_match("user", "stef", req.candidate_capsule)
        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.detail, "Candidate capsule subject does not match request subject")

    def test_strongest_signal_prefers_highest_rank(self) -> None:
        """Trust ranking should prefer the strongest signal regardless of order."""
        req = ContinuityCompareRequest(
            subject_kind="user",
            subject_id="stef",
            candidate_capsule=self._capsule_payload(),
            signals=self._signals_payload(),
        )
        self.assertEqual(_strongest_signal_kind(req.signals), "system_check")

    def test_request_models_enforce_v3_validation_bounds(self) -> None:
        """V3 request models should enforce bounded signals and reason lengths."""
        with self.assertRaises(ValidationError):
            ContinuityCompareRequest(
                subject_kind="user",
                subject_id="stef",
                candidate_capsule=self._capsule_payload(),
                signals=self._signals_payload() * 5,
            )
        with self.assertRaises(ValidationError):
            ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="conflict",
                signals=self._signals_payload(),
                reason="x" * 121,
            )

    def test_candidate_models_enforce_evidence_refs_and_conflict_summary_bounds(self) -> None:
        """Candidate capsule models should reject oversized V3 verification fields."""
        capsule = self._capsule_payload()
        capsule["verification_state"] = {
            "status": "conflicted",
            "last_revalidated_at": capsule["verified_at"],
            "strongest_signal": "system_check",
            "evidence_refs": ["x" * 201],
            "conflict_summary": "x" * 241,
        }
        with self.assertRaises(ValidationError):
            ContinuityCompareRequest(
                subject_kind="user",
                subject_id="stef",
                candidate_capsule=capsule,
                signals=self._signals_payload(),
            )

        capsule["verification_state"]["evidence_refs"] = [f"ref-{idx}" for idx in range(5)]
        capsule["verification_state"]["conflict_summary"] = "ok"
        with self.assertRaises(ValidationError):
            ContinuityCompareRequest(
                subject_kind="user",
                subject_id="stef",
                candidate_capsule=capsule,
                signals=self._signals_payload(),
            )

    def test_upsert_rollback_restores_prior_capsule_on_commit_failure(self) -> None:
        """Upsert should preserve the previously durable capsule on commit failure."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            initial = self._capsule_payload()
            updated = self._capsule_payload()
            updated["updated_at"] = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            updated["continuity"]["stance_summary"] = "updated but should roll back"
            path = repo_root / "memory" / "continuity" / "user-stef.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(initial), encoding="utf-8")
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=updated)  # type: ignore[arg-type]

            with patch("app.main._services", return_value=(settings, _FailingGitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 500)
            restored = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(restored["continuity"]["stance_summary"], initial["continuity"]["stance_summary"])
