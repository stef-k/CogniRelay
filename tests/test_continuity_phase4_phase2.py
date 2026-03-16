"""Tests for Phase 4 refresh planning behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_refresh_plan_service
from app.context.service import context_retrieve_service
from app.main import continuity_refresh_plan
from app.models import ContinuityRefreshPlanRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by Phase 4 refresh tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records refresh-state commits."""

    def __init__(self) -> None:
        """Initialize the commit record list."""
        self.commit_file_calls: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record a single-file commit request and report success."""
        self.commit_file_calls.append((str(path), message))
        return True


class _FailingRefreshGitManagerStub(_GitManagerStub):
    """Git stub that fails refresh-state commits after the file is written."""

    def commit_file(self, path: Path, message: str) -> bool:
        """Raise when refresh planning tries to commit refresh_state.json."""
        self.commit_file_calls.append((str(path), message))
        raise RuntimeError("refresh git failure")


class TestContinuityPhase4Phase2(unittest.TestCase):
    """Validate refresh-plan derivation and refresh-state persistence."""

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
        updated_at: str,
        verified_at: str,
        verification_status: str | None = None,
        health_status: str | None = None,
        health_reasons: list[str] | None = None,
    ) -> dict:
        """Return a continuity capsule payload with optional V3 fields."""
        payload = {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": updated_at,
            "verified_at": verified_at,
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
            "freshness": {"freshness_class": "situational"},
        }
        if verification_status is not None:
            payload["verification_state"] = {
                "status": verification_status,
                "last_revalidated_at": verified_at,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            }
        if health_status is not None:
            payload["capsule_health"] = {
                "status": health_status,
                "reasons": list(health_reasons or []),
                "last_checked_at": verified_at,
            }
        return payload

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict) -> Path:
        """Write one active continuity capsule and return its path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        path = continuity_dir / f"{subject_kind}-{subject_id.strip().lower().replace(' ', '-')}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _write_fallback_snapshot(self, repo_root: Path, *, subject_kind: str, subject_id: str, capsule: dict) -> Path:
        """Write one fallback snapshot envelope and return its path."""
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
                    "verification_status": capsule.get("verification_state", {}).get("status", "unverified"),
                    "health_status": capsule.get("capsule_health", {}).get("status", "unknown"),
                    "capsule": capsule,
                }
            ),
            encoding="utf-8",
        )
        return path

    def _write_audit_log(self, repo_root: Path, rows: list[dict]) -> None:
        """Write the persisted audit rows used by refresh planning."""
        logs_dir = repo_root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        (logs_dir / "api_audit.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    def test_refresh_plan_request_accepts_phase4_fields(self) -> None:
        """The request model should accept the Phase 4 refresh-plan fields."""
        req = ContinuityRefreshPlanRequest(subject_kind="user", limit=5, include_healthy=True)

        self.assertEqual(req.subject_kind, "user")
        self.assertEqual(req.limit, 5)
        self.assertTrue(req.include_healthy)

    def test_refresh_plan_derives_reason_codes_priorities_and_order(self) -> None:
        """Refresh planning should derive deterministic candidates and ordering."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            stale_iso = (now - timedelta(days=45)).isoformat().replace("+00:00", "Z")

            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="degraded",
                    health_reasons=["stale source"],
                ),
            )
            self._write_capsule(
                repo_root,
                subject_kind="thread",
                subject_id="alpha",
                payload=self._capsule_payload(
                    subject_kind="thread",
                    subject_id="alpha",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="unverified",
                    health_status="healthy",
                ),
            )
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="beta",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="beta",
                    updated_at=stale_iso,
                    verified_at=stale_iso,
                    verification_status="system_confirmed",
                    health_status="healthy",
                ),
            )
            fallback_capsule = self._capsule_payload(
                subject_kind="task",
                subject_id="gamma",
                updated_at=recent_iso,
                verified_at=recent_iso,
                verification_status="peer_confirmed",
                health_status="healthy",
            )
            self._write_fallback_snapshot(repo_root, subject_kind="task", subject_id="gamma", capsule=fallback_capsule)
            self._write_audit_log(
                repo_root,
                [
                    {
                        "ts": recent_iso,
                        "event": "continuity_read",
                        "detail": {
                            "subject_kind": "user",
                            "subject_id": "beta",
                            "path": "memory/continuity/user-beta.json",
                            "source_state": "active",
                        },
                    }
                ],
            )

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                out = continuity_refresh_plan(req=ContinuityRefreshPlanRequest(limit=10), auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(
                [(row["subject_kind"], row["subject_id"], row["recommended_priority"]) for row in out["candidates"]],
                [
                    ("task", "gamma", "high"),
                    ("user", "degraded", "high"),
                    ("thread", "alpha", "medium"),
                    ("user", "beta", "medium"),
                ],
            )
            by_selector = {(row["subject_kind"], row["subject_id"]): row for row in out["candidates"]}
            self.assertEqual(by_selector[("task", "gamma")]["reason_codes"], ["fallback_only"])
            self.assertEqual(by_selector[("task", "gamma")]["path"], "memory/continuity/task-gamma.json")
            self.assertEqual(by_selector[("user", "degraded")]["reason_codes"], ["health_degraded"])
            self.assertEqual(by_selector[("thread", "alpha")]["reason_codes"], ["verification_unverified"])
            self.assertEqual(by_selector[("user", "beta")]["reason_codes"], ["stale_verified_at", "recently_used"])

    def test_refresh_plan_skips_recently_used_only_candidates_by_default(self) -> None:
        """Healthy recently-used-only candidates should be omitted unless requested."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="quiet",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="quiet",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="healthy",
                ),
            )
            self._write_audit_log(
                repo_root,
                [
                    {
                        "ts": recent_iso,
                        "event": "continuity_read",
                        "detail": {
                            "subject_kind": "user",
                            "subject_id": "quiet",
                            "path": "memory/continuity/user-quiet.json",
                            "source_state": "active",
                        },
                    }
                ],
            )

            out = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10),
                now=now,
                audit=lambda *_args: None,
            )
            self.assertEqual(out["candidates"], [])

            with_healthy = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10, include_healthy=True),
                now=now,
                audit=lambda *_args: None,
            )
            self.assertEqual(len(with_healthy["candidates"]), 1)
            self.assertEqual(with_healthy["candidates"][0]["reason_codes"], ["recently_used"])

    def test_refresh_plan_skips_fully_healthy_candidates_by_default(self) -> None:
        """Healthy candidates with no reason codes should be omitted unless include_healthy is enabled."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="quiet",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="quiet",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="healthy",
                ),
            )

            out = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10),
                now=now,
                audit=lambda *_args: None,
            )
            self.assertEqual(out["candidates"], [])

            with_healthy = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10, include_healthy=True),
                now=now,
                audit=lambda *_args: None,
            )
            self.assertEqual(len(with_healthy["candidates"]), 1)
            self.assertEqual(with_healthy["candidates"][0]["reason_codes"], [])

    def test_refresh_state_is_persisted_and_only_committed_when_bytes_change(self) -> None:
        """Refresh planning should rewrite refresh_state.json and skip no-op commits."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="degraded",
                    health_reasons=["stale source"],
                ),
            )

            first = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10),
                now=now,
                audit=lambda *_args: None,
            )
            second = continuity_refresh_plan_service(
                repo_root=repo_root,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRefreshPlanRequest(limit=10),
                now=now,
                audit=lambda *_args: None,
            )

            refresh_state_path = repo_root / "memory" / "continuity" / "refresh_state.json"
            self.assertTrue(refresh_state_path.exists())
            payload = json.loads(refresh_state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "1.0")
            self.assertEqual(payload["last_planned_at"], now.isoformat().replace("+00:00", "Z"))
            self.assertEqual(len(gm.commit_file_calls), 1)
            self.assertEqual(first["latest_commit"], "test-sha")
            self.assertEqual(second["latest_commit"], "test-sha")

    def test_refresh_state_commit_failure_restores_prior_bytes(self) -> None:
        """Refresh planning should restore the prior durable refresh state on commit failure."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _FailingRefreshGitManagerStub()
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="degraded",
                    health_reasons=["stale source"],
                ),
            )
            refresh_path = repo_root / "memory" / "continuity" / "refresh_state.json"
            refresh_path.parent.mkdir(parents=True, exist_ok=True)
            refresh_path.write_text('{"schema_version":"1.0","sentinel":"old"}', encoding="utf-8")
            old_bytes = refresh_path.read_bytes()

            with self.assertRaises(HTTPException) as cm:
                continuity_refresh_plan_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=_AuthStub(),
                    req=ContinuityRefreshPlanRequest(limit=10),
                    now=now,
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 500)
            self.assertEqual(refresh_path.read_bytes(), old_bytes)

    def test_context_retrieve_audit_includes_loaded_continuity_selectors(self) -> None:
        """Context retrieval audit detail should include the loaded continuity selector list."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
            recent_iso = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="stef",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="stef",
                    updated_at=recent_iso,
                    verified_at=recent_iso,
                    verification_status="system_confirmed",
                    health_status="healthy",
                ),
            )
            audit_rows: list[tuple[str, dict]] = []

            out = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContextRetrieveRequest(task="resume", subject_kind="user", subject_id="stef"),
                now=now,
                audit=lambda _auth, event, detail: audit_rows.append((event, detail)),
            )

            self.assertTrue(out["ok"])
            self.assertEqual(audit_rows[-1][0], "context_retrieve")
            self.assertEqual(
                audit_rows[-1][1]["continuity_selectors"],
                [{"subject_kind": "user", "subject_id": "stef", "source_state": "active"}],
            )
