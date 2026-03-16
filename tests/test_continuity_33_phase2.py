"""Tests for Issue #33 Phase 2 read/retrieve, compare, and trim behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.continuity.service import _estimated_tokens, _render_value, _trim_capsule, continuity_compare_service
from app.main import context_retrieve, continuity_read
from app.models import ContinuityCompareRequest, ContinuityReadRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub used for route-wrapper continuity tests."""


class TestContinuity33Phase2(unittest.TestCase):
    """Validate Issue #33 passthrough, compare, and trim integration."""

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

    def _capsule_payload(self, *, subject_kind: str = "user", subject_id: str = "stef") -> dict:
        """Return a valid capsule payload including the Issue #33 fields."""
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
                "working_hypotheses": [f"hypothesis for {subject_id}"],
                "trailing_notes": [f"trailing note for {subject_id}"],
                "curiosity_queue": [f"curiosity for {subject_id}"],
                "negative_decisions": [
                    {
                        "decision": f"Do not broaden {subject_id} scope.",
                        "rationale": f"Keep {subject_id} focused on the bounded continuity slice.",
                    }
                ],
                "session_trajectory": [f"trajectory for {subject_id}"],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _write_capsule(self, repo_root: Path, payload: dict) -> None:
        """Write one active continuity capsule to the expected repository path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        normalized = payload["subject_id"].strip().lower().replace(" ", "-")
        (continuity_dir / f"{payload['subject_kind']}-{normalized}.json").write_text(json.dumps(payload), encoding="utf-8")

    def _signals(self) -> list[dict]:
        """Return a strong ordered verification signal list for compare tests."""
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

    def test_continuity_read_returns_issue_33_fields_unchanged(self) -> None:
        """Read should return the new continuity fields unchanged when they are stored."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()
            self._write_capsule(repo_root, payload)

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=AllowAllAuthStub(),
                )

            continuity = out["capsule"]["continuity"]
            self.assertEqual(continuity["trailing_notes"], ["trailing note for stef"])
            self.assertEqual(continuity["curiosity_queue"], ["curiosity for stef"])
            self.assertEqual(continuity["negative_decisions"][0]["decision"], "Do not broaden stef scope.")

    def test_context_retrieve_returns_issue_33_fields_when_not_trimmed(self) -> None:
        """Retrieve should pass the new fields through unchanged when budget permits."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(subject_kind="task", subject_id="issue-33")
            self._write_capsule(repo_root, payload)

            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "task", "subject_id": "issue-33"}],
                continuity_max_capsules=1,
                max_tokens_estimate=4000,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=AllowAllAuthStub())

            continuity = out["bundle"]["continuity_state"]["capsules"][0]["continuity"]
            self.assertEqual(continuity["trailing_notes"], ["trailing note for issue-33"])
            self.assertEqual(continuity["curiosity_queue"], ["curiosity for issue-33"])
            self.assertEqual(
                continuity["negative_decisions"][0]["rationale"],
                "Keep issue-33 focused on the bounded continuity slice.",
            )

    def test_compare_reports_shallow_changed_paths_for_issue_33_fields(self) -> None:
        """Compare should treat the new fields as shallow ordered list paths."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            active = self._capsule_payload()
            self._write_capsule(repo_root, active)
            candidate = self._capsule_payload()
            candidate["continuity"]["negative_decisions"][0]["rationale"] = "Changed rationale."
            candidate["continuity"]["trailing_notes"] = ["changed note"]
            candidate["continuity"]["curiosity_queue"] = ["changed curiosity"]

            out = continuity_compare_service(
                repo_root=repo_root,
                auth=AllowAllAuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user",
                    subject_id="stef",
                    candidate_capsule=candidate,
                    signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )

            self.assertEqual(
                out["changed_fields"],
                [
                    "continuity.negative_decisions",
                    "continuity.trailing_notes",
                    "continuity.curiosity_queue",
                ],
            )

    def test_trim_drops_issue_33_fields_before_working_hypotheses(self) -> None:
        """Trimming should drop the new lower-commitment fields before working hypotheses."""
        payload = self._capsule_payload()
        payload["metadata"] = {"note": "x" * 80}
        payload["canonical_sources"] = ["memory/core/a.md", "memory/core/b.md"]
        payload["continuity"]["relationship_model"] = {
            "trust_level": 0.5,
            "preferred_style": ["s" * 80] * 2,
            "sensitivity_notes": ["n" * 120] * 2,
        }
        payload["continuity"]["retrieval_hints"] = {
            "must_include": ["m" * 160] * 2,
            "avoid": ["a" * 160] * 2,
            "load_next": ["memory/core/identity.md", "memory/core/constraints.md"],
        }
        payload["continuity"]["trailing_notes"] = ["t" * 160] * 3
        payload["continuity"]["curiosity_queue"] = ["c" * 120] * 5
        payload["continuity"]["negative_decisions"] = [
            {"decision": "d" * 160, "rationale": "r" * 240} for _ in range(4)
        ]
        payload["continuity"]["working_hypotheses"] = ["h" * 160] * 5

        boundary = json.loads(json.dumps(payload))
        boundary["continuity"].pop("trailing_notes", None)
        boundary["continuity"].pop("curiosity_queue", None)
        boundary["continuity"].pop("negative_decisions", None)
        max_tokens = _estimated_tokens(_render_value(boundary))

        trimmed = _trim_capsule(payload, max_tokens)

        self.assertIsNotNone(trimmed)
        assert trimmed is not None
        continuity = trimmed["continuity"]
        self.assertNotIn("trailing_notes", continuity)
        self.assertNotIn("curiosity_queue", continuity)
        self.assertNotIn("negative_decisions", continuity)
        self.assertIn("working_hypotheses", continuity)
        self.assertEqual(continuity["active_constraints"], ["constraint for stef"])

    def test_trim_drops_trailing_notes_before_curiosity_queue(self) -> None:
        """Targeted budget pressure should drop trailing notes before curiosity queue."""
        payload = self._capsule_payload()
        payload.pop("freshness", None)
        boundary = json.loads(json.dumps(payload))
        boundary["continuity"].pop("trailing_notes", None)
        max_tokens = _estimated_tokens(_render_value(boundary))

        trimmed = _trim_capsule(payload, max_tokens)

        self.assertIsNotNone(trimmed)
        assert trimmed is not None
        continuity = trimmed["continuity"]
        self.assertNotIn("trailing_notes", continuity)
        self.assertIn("curiosity_queue", continuity)
        self.assertIn("negative_decisions", continuity)

    def test_trim_drops_curiosity_queue_before_negative_decisions(self) -> None:
        """Targeted budget pressure should drop curiosity queue before negative decisions."""
        payload = self._capsule_payload()
        payload.pop("freshness", None)
        boundary = json.loads(json.dumps(payload))
        boundary["continuity"].pop("trailing_notes", None)
        boundary["continuity"].pop("curiosity_queue", None)
        max_tokens = _estimated_tokens(_render_value(boundary))

        trimmed = _trim_capsule(payload, max_tokens)

        self.assertIsNotNone(trimmed)
        assert trimmed is not None
        continuity = trimmed["continuity"]
        self.assertNotIn("trailing_notes", continuity)
        self.assertNotIn("curiosity_queue", continuity)
        self.assertIn("negative_decisions", continuity)
