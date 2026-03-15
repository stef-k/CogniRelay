import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.continuity.service import _trim_capsule
from app.main import continuity_upsert, context_retrieve
from app.models import ContinuityUpsertRequest, ContextRetrieveRequest


class _AuthStub:
    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def __init__(self) -> None:
        self.commits: list[tuple[str, str]] = []

    def latest_commit(self) -> str:
        return "test-sha"

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


class TestContinuityV1(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self, *, subject_kind: str = "user", subject_id: str = "stef", verified_at: str | None = None) -> dict:
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

    def test_continuity_upsert_creates_expected_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=self._capsule_payload())  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["path"], "memory/continuity/user-stef.json")
            self.assertTrue(out["created"])
            written = repo_root / "memory" / "continuity" / "user-stef.json"
            self.assertTrue(written.exists())
            payload = json.loads(written.read_text(encoding="utf-8"))
            self.assertEqual(payload["subject_id"], "stef")

    def test_context_retrieve_continuity_mode_off_skips_capsule(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="resume", continuity_mode="off"),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            self.assertFalse(out["bundle"]["continuity_state"]["present"])

    def test_context_retrieve_explicit_subject_returns_capsule(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            capsule = self._capsule_payload()
            (continuity_dir / "user-stef.json").write_text(json.dumps(capsule), encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["bundle"]["continuity_state"]["present"])
            state = out["bundle"]["continuity_state"]
            self.assertEqual(state["selection_order"], ["explicit:user:stef"])
            self.assertEqual(state["capsules"][0]["subject_id"], "stef")

    def test_context_retrieve_required_missing_capsule_raises_404(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    context_retrieve(
                        req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="missing", continuity_mode="required"),
                        auth=_AuthStub(),
                    )
            self.assertEqual(cm.exception.status_code, 404)

    def test_context_retrieve_partial_subject_selector_raises_400(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    context_retrieve(
                        req=ContextRetrieveRequest(task="resume", subject_kind="user"),
                        auth=_AuthStub(),
                    )
            self.assertEqual(cm.exception.status_code, 400)

    def test_context_retrieve_stale_soft_adds_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            verified_at = (datetime.now(timezone.utc) - timedelta(days=40)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            capsule = self._capsule_payload(verified_at=verified_at)
            (continuity_dir / "user-stef.json").write_text(json.dumps(capsule), encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertIn("continuity_stale_soft", out["bundle"]["continuity_state"]["warnings"])

    def test_context_retrieve_uses_verified_at_not_updated_at_for_staleness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            old_verified = (datetime.now(timezone.utc) - timedelta(days=40)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            capsule = self._capsule_payload(verified_at=old_verified)
            capsule["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            (continuity_dir / "user-stef.json").write_text(json.dumps(capsule), encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertIn("continuity_stale_soft", out["bundle"]["continuity_state"]["warnings"])

    def test_context_retrieve_infers_task_selector(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            capsule = self._capsule_payload(subject_kind="task", subject_id="build-v1")
            (continuity_dir / "task-build-v1.json").write_text(json.dumps(capsule), encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="task:build-v1"),
                    auth=_AuthStub(),
                )
            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertEqual(state["selection_order"], ["inferred:task:build-v1"])

    def test_continuity_upsert_same_bytes_reports_update_false(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=self._capsule_payload())  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                first = continuity_upsert(req=req, auth=_AuthStub())
                second = continuity_upsert(req=req, auth=_AuthStub())

            self.assertTrue(first["created"])
            self.assertFalse(second["created"])
            self.assertFalse(second["updated"])
            self.assertEqual(len(gm.commits), 1)

    def test_continuity_upsert_missing_drift_signals_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            payload = self._capsule_payload()
            payload["continuity"].pop("drift_signals")
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(ValidationError):
                    ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=payload)  # type: ignore[arg-type]

    def test_continuity_upsert_invalid_canonical_source_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            payload = self._capsule_payload()
            payload["canonical_sources"] = ["../escape.json"]
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=payload)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())
            self.assertEqual(cm.exception.status_code, 400)

    def test_continuity_upsert_invalid_load_next_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            payload = self._capsule_payload()
            payload["continuity"]["retrieval_hints"] = {"load_next": ["../escape.json"]}
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=payload)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())
            self.assertEqual(cm.exception.status_code, 400)

    def test_continuity_upsert_oversized_capsule_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            payload = self._capsule_payload()
            payload["metadata"] = {"x": "y" * (13 * 1024)}
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=payload)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())
            self.assertEqual(cm.exception.status_code, 400)

    def test_context_retrieve_expired_capsule_not_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            verified_at = (datetime.now(timezone.utc) - timedelta(days=90)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            capsule = self._capsule_payload(verified_at=verified_at)
            (continuity_dir / "user-stef.json").write_text(json.dumps(capsule), encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            state = out["bundle"]["continuity_state"]
            self.assertFalse(state["present"])
            self.assertIn("continuity_expired", state["warnings"])

    def test_continuity_upsert_metadata_scalar_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            payload = self._capsule_payload()
            payload["metadata"] = {"nested": {"bad": True}}
            req = ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=payload)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    continuity_upsert(req=req, auth=_AuthStub())
            self.assertEqual(cm.exception.status_code, 400)

    def test_trim_capsule_drops_lower_priority_optional_fields_before_constraints(self) -> None:
        capsule = self._capsule_payload()
        capsule["continuity"]["working_hypotheses"] = [
            "x" * 160,
            "y" * 160,
        ]
        capsule["continuity"]["relationship_model"] = {
            "trust_level": "high",
            "preferred_style": ["direct", "technical", "low-fluff"],
            "sensitivity_notes": ["keep replies concise"],
        }
        capsule["continuity"]["retrieval_hints"] = {
            "must_include": ["recent commitments", "current blockers"],
            "avoid": ["raw logs", "broad recap"],
            "load_next": ["memory/core/identity.md"],
        }
        capsule["metadata"] = {"trace": "z" * 400}
        capsule["canonical_sources"] = ["memory/core/identity.md"]
        capsule["attention_policy"] = {"presence_bias_overrides": ["long-horizon work first"]}
        trimmed = _trim_capsule(capsule, 180)
        self.assertIsNotNone(trimmed)
        assert trimmed is not None
        self.assertEqual(trimmed["continuity"]["active_constraints"], ["do not regress current workflows"])
        self.assertNotIn("working_hypotheses", trimmed["continuity"])
        self.assertNotIn("relationship_model", trimmed["continuity"])
        self.assertNotIn("canonical_sources", trimmed)
        self.assertNotIn("metadata", trimmed)
        self.assertNotIn("freshness", trimmed)


if __name__ == "__main__":
    unittest.main()
