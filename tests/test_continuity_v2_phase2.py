"""Tests for continuity-state V2 Phase 2 multi-capsule retrieval behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import build_continuity_state
from app.main import context_retrieve
from app.models import ContextRetrieveRequest


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
    """Git manager stub used to satisfy the service bundle patch."""

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestContinuityV2Phase2(unittest.TestCase):
    """Validate the Phase 2 multi-capsule retrieval contract."""

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
    ) -> dict:
        """Return a valid capsule payload with V2-compatible fields."""
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
                "session_trajectory": [f"trajectory for {subject_id}"],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
        }

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict | None = None) -> None:
        """Write one active continuity capsule to the expected repository path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        capsule = payload or self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
        normalized = subject_id.strip().lower().replace(" ", "-")
        (continuity_dir / f"{subject_kind}-{normalized}.json").write_text(json.dumps(capsule), encoding="utf-8")

    def test_multi_capsule_retrieval_returns_deterministic_order(self) -> None:
        """Primary and secondary selectors should load in deterministic explicit order."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="thread", subject_id="guestbook-1")
            self._write_capsule(repo_root, subject_kind="user", subject_id="curious")
            self._write_capsule(repo_root, subject_kind="task", subject_id="guestbook-maintenance")
            req = ContextRetrieveRequest(
                task="resume",
                subject_kind="thread",
                subject_id="guestbook-1",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "curious"},
                    {"subject_kind": "task", "subject_id": "guestbook-maintenance"},
                ],
                continuity_max_capsules=3,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertEqual(
                state["requested_selectors"],
                ["thread:guestbook-1", "user:curious", "task:guestbook-maintenance"],
            )
            self.assertEqual(
                state["selection_order"],
                [
                    "explicit:thread:guestbook-1",
                    "explicit:user:curious",
                    "explicit:task:guestbook-maintenance",
                ],
            )
            self.assertEqual([item["subject_id"] for item in state["capsules"]], ["guestbook-1", "curious", "guestbook-maintenance"])

    def test_deduplication_uses_normalized_selector_identity(self) -> None:
        """Selectors that normalize to the same key should collapse to one load."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="task", subject_id="my-task")
            req = ContextRetrieveRequest(
                task="resume",
                subject_kind="task",
                subject_id="My Task",
                continuity_selectors=[{"subject_kind": "task", "subject_id": "my-task"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual(state["requested_selectors"], ["task:My Task"])
            self.assertEqual(state["selection_order"], ["explicit:task:My Task"])
            self.assertEqual(len(state["capsules"]), 1)

    def test_selector_limit_omits_excess_selectors(self) -> None:
        """Selectors beyond continuity_max_capsules should be omitted deterministically."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="a")
            self._write_capsule(repo_root, subject_kind="user", subject_id="b")
            self._write_capsule(repo_root, subject_kind="user", subject_id="c")
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                    {"subject_kind": "user", "subject_id": "c"},
                ],
                continuity_max_capsules=2,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual(state["requested_selectors"], ["user:a", "user:b"])
            self.assertEqual(state["omitted_selectors"], ["user:c"])

    def test_budget_sharing_uses_even_split_without_redistribution(self) -> None:
        """Loaded capsules should share the V1 reserve evenly in selector order."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_capsule(repo_root, subject_kind="user", subject_id="a")
            self._write_capsule(repo_root, subject_kind="user", subject_id="b")
            allocations: list[int] = []

            def _record_trim(capsule: dict, max_tokens: int) -> dict:
                allocations.append(max_tokens)
                return capsule

            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                ],
                continuity_max_capsules=2,
                max_tokens_estimate=4000,
            )
            with patch("app.continuity.service._trim_capsule", side_effect=_record_trim):
                state = build_continuity_state(repo_root=repo_root, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc))

            self.assertEqual(allocations, [400, 400])
            self.assertTrue(state["present"])

    def test_required_mode_succeeds_when_one_selector_loads(self) -> None:
        """Required mode should succeed when at least one requested capsule survives."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="user", subject_id="loaded")
            req = ContextRetrieveRequest(
                task="resume",
                continuity_mode="required",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "loaded"},
                    {"subject_kind": "user", "subject_id": "missing"},
                ],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertEqual(state["selection_order"], ["explicit:user:loaded"])
            self.assertEqual(state["omitted_selectors"], ["user:missing"])

    def test_required_mode_raises_when_zero_capsules_load(self) -> None:
        """Required mode should fail when no selected capsule survives loading."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = ContextRetrieveRequest(
                task="resume",
                continuity_mode="required",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "missing"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    context_retrieve(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 404)

    def test_empty_selector_array_behaves_like_v1_inference(self) -> None:
        """An explicit empty selector array should fall back to V1 inference unchanged."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(repo_root, subject_kind="task", subject_id="build-v2")
            req = ContextRetrieveRequest(task="task:build-v2", continuity_selectors=[])
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual(state["requested_selectors"], ["task:build-v2"])
            self.assertEqual(state["selection_order"], ["inferred:task:build-v2"])
            self.assertEqual(state["warnings"], [])

    def test_warning_mode_uses_qualified_strings_when_v2_selector_field_present(self) -> None:
        """Multi-capsule warning mode should qualify stale warnings even with one surviving capsule."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            verified_at = (datetime.now(timezone.utc) - timedelta(days=40)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="stef",
                payload=self._capsule_payload(subject_kind="user", subject_id="stef", verified_at=verified_at),
            )
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            self.assertIn("continuity_stale_soft:user:stef", out["bundle"]["continuity_state"]["warnings"])

    def test_invalid_selected_capsule_halts_with_400(self) -> None:
        """Invalid continuity capsule files should fail retrieval consistent with V1."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-bad.json").write_text("{not-json", encoding="utf-8")
            req = ContextRetrieveRequest(task="resume", continuity_selectors=[{"subject_kind": "user", "subject_id": "bad"}])
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    context_retrieve(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 400)
