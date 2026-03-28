"""Tests for Phase 4 fallback snapshot and degraded continuity behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_read_service
from app.main import continuity_revalidate, continuity_upsert, context_retrieve
from app.models import (
    ContinuityReadRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
    ContextRetrieveRequest,
)
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by Phase 4 continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records per-file commit requests."""

    def __init__(self) -> None:
        """Initialize the stubbed commit log."""
        self.commit_file_calls: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the file commit request and report success."""
        self.commit_file_calls.append((str(path), message))
        return True


class _FailingFallbackGitManagerStub(_GitManagerStub):
    """Git stub that fails only the fallback snapshot commit."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Fail the fallback commit while allowing the active write to succeed."""
        self.commit_file_calls.append((str(path), message))
        return "fallback" not in path.parts


class _ExplodingFallbackGitManagerStub(_GitManagerStub):
    """Git stub that raises only for fallback snapshot commits."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Raise on fallback commits while allowing active writes through."""
        self.commit_file_calls.append((str(path), message))
        if "fallback" in path.parts:
            raise RuntimeError("fallback git failure")
        return True


class _RejectingReadAuthStub(_AuthStub):
    """Auth stub that denies reads for fallback paths only."""

    def require_read_path(self, path: str) -> None:
        """Reject fallback-path reads while permitting everything else."""
        if "memory/continuity/fallback/" in path:
            raise HTTPException(status_code=403, detail="denied")


class TestContinuityPhase4Phase1(unittest.TestCase):
    """Validate fallback snapshot persistence and degraded read/retrieve behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build settings rooted at the temporary repository."""
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
        freshness_class: str = "situational",
        verification_status: str | None = None,
        health_status: str | None = None,
        health_reasons: list[str] | None = None,
    ) -> dict:
        """Return a valid continuity capsule payload."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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
            "freshness": {"freshness_class": freshness_class},
        }
        if verification_status is not None:
            payload["verification_state"] = {
                "status": verification_status,
                "last_revalidated_at": now,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            }
        if health_status is not None:
            payload["capsule_health"] = {
                "status": health_status,
                "reasons": list(health_reasons or []),
                "last_checked_at": now,
            }
        return payload

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict | None = None) -> Path:
        """Write one active continuity capsule and return its path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        path = continuity_dir / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json"
        path.write_text(json.dumps(payload or self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)), encoding="utf-8")
        return path

    def _write_fallback_snapshot(self, repo_root: Path, *, subject_kind: str, subject_id: str, capsule: dict) -> Path:
        """Write a fallback snapshot envelope and return its path."""
        fallback_dir = repo_root / "memory" / "continuity" / "fallback"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        path = fallback_dir / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json"
        path.write_text(
            json.dumps(
                {
                    "schema_type": "continuity_fallback_snapshot",
                    "schema_version": "1.0",
                    "captured_at": capsule["updated_at"],
                    "source_path": f"memory/continuity/{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json",
                    "verification_status": "system_confirmed",
                    "health_status": "healthy",
                    "capsule": capsule,
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_upsert_writes_matching_fallback_snapshot_after_active_commit(self) -> None:
        """Successful upserts should also refresh the matching fallback snapshot."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            fallback_path = repo_root / "memory" / "continuity" / "fallback" / "user-stef.json"
            self.assertTrue(fallback_path.exists())
            snapshot = json.loads(fallback_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["schema_type"], "continuity_fallback_snapshot")
            self.assertEqual(snapshot["source_path"], "memory/continuity/user-stef.json")
            self.assertEqual(snapshot["capsule"]["subject_id"], "stef")
            self.assertEqual(
                gm.commit_file_calls,
                [
                    (str(repo_root / "memory" / "continuity" / "user-stef.json"), "continuity: upsert user stef"),
                    (str(fallback_path), "continuity: update fallback user stef"),
                ],
            )

    def test_revalidate_writes_matching_fallback_snapshot_after_active_commit(self) -> None:
        """Successful revalidation should also refresh the fallback snapshot."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(
                subject_kind="user",
                subject_id="stef",
                verification_status="self_attested",
                health_status="healthy",
            )
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef", payload=capsule)
            req = ContinuityRevalidateRequest(
                subject_kind="user",
                subject_id="stef",
                outcome="confirm",
                signals=[
                    {
                        "kind": "system_check",
                        "source_ref": "memory/core/identity.md",
                        "observed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                        "summary": "checked",
                    }
                ],
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_revalidate(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            fallback_path = repo_root / "memory" / "continuity" / "fallback" / "user-stef.json"
            self.assertTrue(fallback_path.exists())
            snapshot = json.loads(fallback_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["capsule"]["subject_id"], "stef")
            self.assertEqual(snapshot["verification_status"], "system_confirmed")
            self.assertEqual(snapshot["health_status"], "healthy")

    def test_upsert_idempotent_write_does_not_fail_or_rewrite_fallback_snapshot(self) -> None:
        """Identical upserts should not report fallback failure or rewrite the fallback snapshot."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")

            with patch("app.main._services", return_value=(settings, gm)):
                first = continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule),
                    auth=_AuthStub(),
                )
                second = continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule),
                    auth=_AuthStub(),
                )

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertFalse(second["updated"])
            fallback_path = repo_root / "memory" / "continuity" / "fallback" / "user-stef.json"
            self.assertTrue(fallback_path.exists())
            self.assertEqual(
                gm.commit_file_calls,
                [
                    (str(repo_root / "memory" / "continuity" / "user-stef.json"), "continuity: upsert user stef"),
                    (str(fallback_path), "continuity: update fallback user stef"),
                ],
            )

    def test_upsert_preserves_previous_fallback_snapshot_on_fallback_commit_failure(self) -> None:
        """Fallback commit failure should restore the prior fallback bytes and keep the active write successful."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _FailingFallbackGitManagerStub()
            old_capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=old_capsule)
            old_snapshot = (repo_root / "memory" / "continuity" / "fallback" / "user-stef.json").read_text(encoding="utf-8")

            new_capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            new_capsule["continuity"]["stance_summary"] = "new stance"

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=new_capsule),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            active_path = repo_root / "memory" / "continuity" / "user-stef.json"
            active = json.loads(active_path.read_text(encoding="utf-8"))
            self.assertEqual(active["continuity"]["stance_summary"], "new stance")
            fallback_path = repo_root / "memory" / "continuity" / "fallback" / "user-stef.json"
            self.assertEqual(fallback_path.read_text(encoding="utf-8"), old_snapshot)

    def test_read_uses_fallback_when_active_is_missing(self) -> None:
        """Read should return a fallback snapshot when the active file is missing."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)

            out = continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
                now=datetime.now(timezone.utc),
                audit=lambda *_args: None,
            )

            self.assertTrue(out["ok"])
            self.assertEqual(out["source_state"], "fallback")
            self.assertEqual(out["capsule"]["subject_id"], "stef")
            self.assertEqual(out["recovery_warnings"], ["continuity_active_missing", "continuity_fallback_used"])
            self.assertFalse(out["archived"])

    def test_read_returns_degraded_success_when_active_and_fallback_are_missing(self) -> None:
        """Read should degrade to a parseable missing response when nothing is usable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)

            out = continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
                now=datetime.now(timezone.utc),
                audit=lambda *_args: None,
            )

            self.assertTrue(out["ok"])
            self.assertIsNone(out["capsule"])
            self.assertEqual(out["source_state"], "missing")
            self.assertEqual(out["recovery_warnings"], ["continuity_active_missing", "continuity_fallback_missing"])
            self.assertFalse(out["archived"])

    def test_read_preserves_strict_behavior_when_allow_fallback_is_false(self) -> None:
        """Read should preserve the prior exact-active 404 path when fallback is disabled."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)

            with self.assertRaises(HTTPException) as cm:
                continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=False),
                    now=datetime.now(timezone.utc),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 404)

    def test_read_rejects_subject_mismatch_in_active_capsule_even_when_fallback_exists(self) -> None:
        """Read should fail hard on active selector mismatch instead of masking it with fallback data."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            mismatched = self._capsule_payload(subject_kind="user", subject_id="other")
            self._write_capsule(repo_root, subject_kind="user", subject_id="stef", payload=mismatched)
            fallback = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=fallback)

            with self.assertRaises(HTTPException) as cm:
                continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
                    now=datetime.now(timezone.utc),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 400)
            self.assertEqual(cm.exception.detail, "Continuity capsule subject does not match requested subject")

    def test_read_requires_auth_for_the_matching_fallback_path(self) -> None:
        """Fallback reads should enforce auth on the fallback path before loading it."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)

            with self.assertRaises(HTTPException) as cm:
                continuity_read_service(
                    repo_root=repo_root,
                    auth=_RejectingReadAuthStub(),
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
                    now=datetime.now(timezone.utc),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 403)

    def test_context_retrieve_uses_fallback_when_policy_allows_it(self) -> None:
        """Retrieve should load fallback continuity when the active file is unavailable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)
            req = ContextRetrieveRequest(
                task="resume",
                subject_kind="user",
                subject_id="stef",
                continuity_resilience_policy="allow_fallback",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertTrue(state["fallback_used"])
            self.assertEqual(
                state["recovery_warnings"],
                ["continuity_active_missing", "continuity_fallback_used", "continuity_index_missing"],
            )
            self.assertEqual(state["capsules"][0]["subject_id"], "stef")
            self.assertEqual(state["capsules"][0]["source_state"], "fallback")

    def test_context_retrieve_require_active_omits_fallback_only_capsules(self) -> None:
        """Require-active should omit fallback-only continuity before budgeting."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)
            req = ContextRetrieveRequest(
                task="resume",
                subject_kind="user",
                subject_id="stef",
                continuity_mode="auto",
                continuity_resilience_policy="require_active",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertFalse(state["present"])
            self.assertFalse(state["fallback_used"])
            self.assertEqual(state["capsules"], [])
            self.assertEqual(state["omitted_selectors"], ["user:stef"])

    def test_context_retrieve_prefer_active_uses_same_active_first_fallback_path(self) -> None:
        """Prefer-active should still load fallback continuity when active continuity is unavailable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_kind="user", subject_id="stef")
            self._write_fallback_snapshot(repo_root, subject_kind="user", subject_id="stef", capsule=capsule)
            req = ContextRetrieveRequest(
                task="resume",
                subject_kind="user",
                subject_id="stef",
                continuity_resilience_policy="prefer_active",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertTrue(state["fallback_used"])
            self.assertEqual(state["capsules"][0]["source_state"], "fallback")

    def test_upsert_degrades_success_when_fallback_snapshot_write_raises(self) -> None:
        """Fallback write exceptions should not fail the already-committed active upsert."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _ExplodingFallbackGitManagerStub()
            req = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="stef",
                capsule=self._capsule_payload(subject_kind="user", subject_id="stef"),
            )

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(req=req, auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["path"], "memory/continuity/user-stef.json")
            self.assertEqual(out["recovery_warnings"], ["continuity_fallback_write_failed"])
            self.assertIn("Failed to persist continuity fallback snapshot", out["fallback_warning_detail"])
