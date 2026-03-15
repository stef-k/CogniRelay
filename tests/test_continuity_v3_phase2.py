"""Tests for continuity-state V3 Phase 2 compare workflow behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_compare_service
from app.main import continuity_compare
from app.models import ContinuityCompareRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that records whether compare incorrectly mutates git."""

    def __init__(self) -> None:
        """Initialize the commit ledger used by read-only assertions."""
        self.file_commits: list[tuple[str, str]] = []
        self.path_commits: list[tuple[list[str], str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record a single-file commit request."""
        self.file_commits.append((str(path), message))
        return True

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record a multi-path commit request."""
        self.path_commits.append(([str(path) for path in paths], message))
        return True


class TestContinuityV3Phase2(unittest.TestCase):
    """Validate the V3 compare endpoint and service contract."""

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

    def _capsule_payload(self, *, subject_id: str = "stef") -> dict:
        """Return a valid capsule payload for compare tests."""
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
                "stance_summary": "Preserve continuity quality while staying backward compatible.",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _signals(self, *, strong: bool = True) -> list[dict]:
        """Return an ordered verification signal list with optional strong evidence."""
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

    def _write_capsule(self, repo_root: Path, payload: dict) -> None:
        """Write one active continuity capsule to the expected repository path."""
        path = repo_root / "memory" / "continuity" / f"user-{payload['subject_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_compare_identical_capsule_returns_confirm(self) -> None:
        """Compare should report identical capsules without mutations."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            req = ContinuityCompareRequest(
                subject_kind="user",
                subject_id="stef",
                candidate_capsule=active,
                signals=self._signals(),
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_compare(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertTrue(out["identical"])
            self.assertEqual(out["changed_fields"], [])
            self.assertEqual(out["strongest_signal"], "system_check")
            self.assertEqual(out["recommended_outcome"], "confirm")
            self.assertEqual(gm.file_commits, [])
            self.assertEqual(gm.path_commits, [])

    def test_compare_changed_capsule_with_strong_signal_recommends_correct(self) -> None:
        """Compare should recommend correct when non-self-review signals support a changed candidate."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["top_priorities"] = ["new priority"]

            out = continuity_compare_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertFalse(out["identical"])
            self.assertEqual(out["changed_fields"], ["continuity.top_priorities"])
            self.assertEqual(out["recommended_outcome"], "correct")

    def test_compare_changed_capsule_with_only_self_review_recommends_conflict(self) -> None:
        """Compare should recommend conflict when only self-review supports a changed candidate."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["metadata"] = {"interaction_boundary_kind": "manual_checkpoint"}
            candidate["source"]["update_reason"] = "interaction_boundary"

            out = continuity_compare_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(strong=False),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["strongest_signal"], "self_review")
            self.assertEqual(out["recommended_outcome"], "conflict")
            self.assertEqual(out["changed_fields"], ["source.update_reason", "metadata.interaction_boundary_kind"])

    def test_compare_missing_active_capsule_returns_404(self) -> None:
        """Compare should return 404 when the selected active capsule does not exist."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            req = ContinuityCompareRequest(
                subject_kind="user",
                subject_id="missing",
                candidate_capsule=self._capsule_payload(subject_id="missing"),
                signals=self._signals(),
            )
            with self.assertRaises(HTTPException) as cm:
                continuity_compare_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=req,
                    audit=lambda *_args: None,
                )
        self.assertEqual(cm.exception.status_code, 404)

    def test_compare_emits_audit_event(self) -> None:
        """Compare should emit an audit event with identical and recommendation metadata."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["top_priorities"] = ["new priority"]
            events: list[tuple[str, dict]] = []

            continuity_compare_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda _auth, event, detail: events.append((event, detail)),
            )

            self.assertEqual(events[0][0], "continuity_compare")
            self.assertEqual(events[0][1]["recommended_outcome"], "correct")
            self.assertFalse(events[0][1]["identical"])

    def test_compare_treats_explicit_none_and_missing_optional_fields_as_identical(self) -> None:
        """Compare should normalize explicit null and missing optional fields equivalently."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = dict(active)
            candidate["attention_policy"] = None

            out = continuity_compare_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertTrue(out["identical"])
            self.assertEqual(out["changed_fields"], [])

    def test_compare_reports_array_order_changes_at_the_shallowest_path(self) -> None:
        """Compare should treat arrays as ordered and report the container path."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            active["continuity"]["top_priorities"] = ["a", "b"]
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["top_priorities"] = ["b", "a"]

            out = continuity_compare_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(out["changed_fields"], ["continuity.top_priorities"])

    def test_compare_rejects_candidate_schema_validation_failures(self) -> None:
        """Compare should reject candidate capsules that fail schema validation."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["canonical_sources"] = ["/etc/passwd"]

            with self.assertRaises(HTTPException) as cm:
                continuity_compare_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityCompareRequest(
                        subject_kind="user",
                        subject_id="stef",
                        candidate_capsule=candidate,
                        signals=self._signals(),
                    ),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 400)
