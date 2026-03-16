"""Tests for continuity-state V3 Phase 3 revalidate workflow behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_revalidate_service
from app.main import continuity_revalidate
from app.models import ContinuityRevalidateRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that records revalidate commits."""

    def __init__(self) -> None:
        """Initialize commit recording for revalidate tests."""
        self.file_commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record a single-file commit request."""
        self.file_commits.append((str(path), message))
        return True


class _FailingGitManagerStub(_GitManagerStub):
    """Git manager stub that fails the revalidate commit."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the attempted commit and then fail."""
        super().commit_file(path, message)
        raise RuntimeError("git commit failed")


class _FailingFallbackGitManagerStub(_GitManagerStub):
    """Git manager stub that fails only the fallback snapshot commit."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Raise on fallback snapshot commits while allowing the active revalidate commit."""
        super().commit_file(path, message)
        if "fallback" in path.parts:
            raise RuntimeError("fallback git failure")
        return True


class TestContinuityV3Phase3(unittest.TestCase):
    """Validate the V3 revalidate endpoint and service contract."""

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

    def _capsule_payload(self, *, subject_id: str = "stef", stance_summary: str | None = None) -> dict:
        """Return a valid capsule payload for revalidate tests."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "schema_version": "1.0",
            "subject_kind": "user",
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
                "top_priorities": ["continuity across compaction"],
                "active_concerns": ["loss of nuance during summarization"],
                "active_constraints": ["do not regress current workflows"],
                "open_loops": ["finish continuity-state spec"],
                "stance_summary": stance_summary or "Preserve continuity quality while staying backward compatible.",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _signals(self, *, strong: bool = True) -> list[dict]:
        """Return a verification signal list with optional strong evidence."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        signals = [
            {
                "kind": "self_review",
                "source_ref": "memory/logs/self-check-1.json",
                "observed_at": now,
                "summary": "Self review completed.",
            }
        ]
        if strong:
            signals.append(
                {
                    "kind": "system_check",
                    "source_ref": "memory/logs/system-check-1.json",
                    "observed_at": now,
                    "summary": "System check passed.",
                }
            )
        return signals

    def _write_capsule(self, repo_root: Path, payload: dict) -> Path:
        """Write one active continuity capsule to the expected repository path."""
        path = repo_root / "memory" / "continuity" / f"user-{payload['subject_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_revalidate_confirm_sets_healthy_verification_state(self) -> None:
        """Confirm should preserve body content and write healthy verification metadata."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            req = ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="confirm",
                signals=self._signals(),
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_revalidate(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["outcome"], "confirm")
            self.assertFalse(out["updated"])
            self.assertEqual(out["verification_state"]["status"], "system_confirmed")
            self.assertEqual(out["capsule_health"]["status"], "healthy")
            persisted = json.loads((repo_root / "memory" / "continuity" / "user-stef.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted["continuity"]["stance_summary"], active["continuity"]["stance_summary"])
            self.assertEqual(persisted["verification_state"]["status"], "system_confirmed")

    def test_revalidate_correct_replaces_body_and_sets_updated_true(self) -> None:
        """Correct should replace the capsule body and report an updated body diff."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["stance_summary"] = "Updated stance after stronger verification."

            out = continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="correct",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["outcome"], "correct")
            self.assertTrue(out["updated"])
            persisted = json.loads((repo_root / "memory" / "continuity" / "user-stef.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted["continuity"]["stance_summary"], "Updated stance after stronger verification.")

    def test_revalidate_correct_without_content_change_falls_back_to_confirm(self) -> None:
        """Correct with no body diff should behave like confirm and report updated=false."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)

            out = continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="correct",
                    candidate_capsule=self._capsule_payload(),
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["outcome"], "confirm")
            self.assertFalse(out["updated"])
            self.assertEqual(out["capsule_health"]["status"], "healthy")

    def test_revalidate_degrade_marks_capsule_degraded(self) -> None:
        """Degrade should preserve the body and mark the capsule degraded."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)

            out = continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="degrade",
                    reason="source went stale",
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["outcome"], "degrade")
            self.assertFalse(out["updated"])
            self.assertEqual(out["capsule_health"]["status"], "degraded")
            self.assertEqual(out["capsule_health"]["reasons"], ["source went stale"])

    def test_revalidate_conflict_marks_capsule_conflicted(self) -> None:
        """Conflict should preserve the body and set conflicted verification and health state."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)

            out = continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="conflict",
                    reason="current evidence disagrees",
                    signals=self._signals(strong=False),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["outcome"], "conflict")
            self.assertFalse(out["updated"])
            self.assertEqual(out["verification_state"]["status"], "conflicted")
            self.assertEqual(out["capsule_health"]["status"], "conflicted")

    def test_revalidate_commit_failure_restores_prior_capsule(self) -> None:
        """Commit failure should preserve the previously durable active capsule."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            active_path = self._write_capsule(repo_root, active)
            with self.assertRaises(HTTPException) as cm:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_FailingGitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="stef",
                        outcome="confirm",
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 500)
            restored = json.loads(active_path.read_text(encoding="utf-8"))
            self.assertNotIn("verification_state", restored)
            self.assertEqual(restored["continuity"]["stance_summary"], active["continuity"]["stance_summary"])

    def test_revalidate_emits_audit_event(self) -> None:
        """Revalidate should emit an audit event with updated metadata."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            events: list[tuple[str, dict]] = []

            continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="confirm",
                    signals=self._signals(),
                ),
                audit=lambda _auth, event, detail: events.append((event, detail)),
            )

            self.assertEqual(events[0][0], "continuity_revalidate")
            self.assertEqual(events[0][1]["strongest_signal"], "system_check")
            self.assertFalse(events[0][1]["updated"])

    def test_revalidate_derives_evidence_refs_in_request_order_capped_at_four(self) -> None:
        """Revalidate should persist the first four signal source refs in request order."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            signals = [
                {"kind": "self_review", "source_ref": f"ref-{idx}", "observed_at": now, "summary": f"signal {idx}"}
                for idx in range(5)
            ]

            out = continuity_revalidate_service(
                repo_root=repo_root,
                gm=_GitManagerStub(),
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user",
                    subject_id="stef",
                    outcome="confirm",
                    signals=signals,
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["verification_state"]["evidence_refs"], ["ref-0", "ref-1", "ref-2", "ref-3"])

    def test_revalidate_missing_active_capsule_returns_404(self) -> None:
        """Revalidate should return 404 when the selected active capsule does not exist."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            with self.assertRaises(HTTPException) as cm:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="missing",
                        outcome="confirm",
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 404)

    def test_revalidate_surfaces_fallback_write_warning_in_response(self) -> None:
        """Revalidate should degrade success when only the fallback snapshot write fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _FailingFallbackGitManagerStub()
            payload = self._capsule_payload()
            self._write_capsule(repo_root, payload)
            req = ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="confirm",
                signals=self._signals(),
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_revalidate(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["recovery_warnings"], ["continuity_fallback_write_failed"])

    def test_revalidate_enforces_outcome_specific_validation(self) -> None:
        """Revalidate should reject forbidden candidate and reason combinations."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)

            with self.assertRaises(HTTPException) as confirm_reason:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="stef",
                        outcome="confirm",
                        reason="not allowed",
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )
            self.assertEqual(confirm_reason.exception.status_code, 400)

            with self.assertRaises(HTTPException) as degrade_candidate:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="stef",
                        outcome="degrade",
                        candidate_capsule=self._capsule_payload(),
                        reason="stale",
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )
            self.assertEqual(degrade_candidate.exception.status_code, 400)

            with self.assertRaises(HTTPException) as missing_reason:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="stef",
                        outcome="conflict",
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )
            self.assertEqual(missing_reason.exception.status_code, 400)

    def test_revalidate_rejects_oversized_post_injection_capsule(self) -> None:
        """Revalidate should enforce the final assembled 12KB size limit."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["stance_summary"] = "x" * 240
            candidate["continuity"]["top_priorities"] = ["x" * 160] * 5
            candidate["continuity"]["active_concerns"] = ["x" * 160] * 5
            candidate["continuity"]["active_constraints"] = ["x" * 160] * 5
            candidate["continuity"]["open_loops"] = ["x" * 160] * 5
            candidate["continuity"]["working_hypotheses"] = ["x" * 160] * 5
            candidate["continuity"]["long_horizon_commitments"] = ["x" * 160] * 5
            candidate["metadata"] = {f"m{idx}": "x" * 320 for idx in range(12)}
            candidate["canonical_sources"] = [f"memory/core/source-{idx}.md" for idx in range(8)]
            candidate["source"]["inputs"] = [f"memory/core/source-input-{idx}-{'x' * 150}.md"[:200] for idx in range(12)]

            with self.assertRaises(HTTPException) as cm:
                continuity_revalidate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(),
                    auth=_AuthStub(),
                    req=ContinuityRevalidateRequest(
                        subject_kind="user",
                        subject_id="stef",
                        outcome="correct",
                        candidate_capsule=candidate,
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 400)
            self.assertIn("12 KB", str(cm.exception.detail))
