"""Tests for continuity V3 Phase 4 retrieval and list behavior."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import _trim_capsule, continuity_list_service
from app.main import context_retrieve
from app.models import ContextRetrieveRequest, ContinuityListRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub used to satisfy the service bundle patch."""


class TestContinuityV3Phase4(unittest.TestCase):
    """Validate Phase 4 verification-aware retrieval and list behavior."""

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
        health_status: str | None = None,
        health_reasons: list[str] | None = None,
        verification_status: str | None = None,
        extra_metadata_keys: int = 0,
        long_horizon: list[str] | None = None,
        canonical_sources: list[str] | None = None,
    ) -> dict:
        """Return a valid capsule payload with optional V3 fields."""
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
                "long_horizon_commitments": list(long_horizon or []),
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
            "freshness": {"freshness_class": "situational"},
            "metadata": {f"k{idx}": f"value-{idx}" for idx in range(extra_metadata_keys)},
            "canonical_sources": list(canonical_sources or []),
        }
        if verification_status is not None:
            payload["verification_state"] = {
                "status": verification_status,
                "last_revalidated_at": now,
                "strongest_signal": "self_review",
                "evidence_refs": ["memory/core/identity.md"],
            }
        if health_status is not None:
            payload["capsule_health"] = {
                "status": health_status,
                "reasons": list(health_reasons or []),
                "last_checked_at": now,
            }
        return payload

    def _write_capsule(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict | None = None) -> None:
        """Write one active continuity capsule to the expected repository path."""
        continuity_dir = repo_root / "memory" / "continuity"
        continuity_dir.mkdir(parents=True, exist_ok=True)
        capsule = payload or self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
        normalized = subject_id.strip().lower().replace(" ", "-")
        (continuity_dir / f"{subject_kind}-{normalized}.json").write_text(json.dumps(capsule), encoding="utf-8")

    def test_context_retrieve_accepts_verification_policy(self) -> None:
        """The request model should accept the V3 verification policy field."""
        req = ContextRetrieveRequest(task="resume", continuity_verification_policy="prefer_healthy")

        self.assertEqual(req.continuity_verification_policy, "prefer_healthy")

    def test_allow_degraded_loads_unhealthy_capsules_with_warnings(self) -> None:
        """Allow-degraded retrieval should keep unhealthy capsules and surface warnings."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="healthy",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="healthy",
                    verification_status="system_confirmed",
                    health_status="healthy",
                ),
            )
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    verification_status="externally_supported",
                    health_status="degraded",
                    health_reasons=["stale source"],
                ),
            )
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "healthy"},
                    {"subject_kind": "user", "subject_id": "degraded"},
                ],
                continuity_verification_policy="allow_degraded",
                continuity_max_capsules=2,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual([item["subject_id"] for item in state["capsules"]], ["healthy", "degraded"])
            self.assertIn("continuity_degraded:user:degraded", state["warnings"])

    def test_prefer_healthy_orders_capsules_by_health_preserving_group_order(self) -> None:
        """Prefer-healthy retrieval should stable-partition by health status."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            for subject_id, health_status in (
                ("degraded-a", "degraded"),
                ("healthy-a", "healthy"),
                ("conflicted-a", "conflicted"),
                ("healthy-b", "healthy"),
            ):
                self._write_capsule(
                    repo_root,
                    subject_kind="user",
                    subject_id=subject_id,
                    payload=self._capsule_payload(
                        subject_kind="user",
                        subject_id=subject_id,
                        verification_status="system_confirmed" if health_status == "healthy" else "conflicted",
                        health_status=health_status,
                        health_reasons=["needs review"] if health_status != "healthy" else [],
                    ),
                )
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "degraded-a"},
                    {"subject_kind": "user", "subject_id": "healthy-a"},
                    {"subject_kind": "user", "subject_id": "conflicted-a"},
                    {"subject_kind": "user", "subject_id": "healthy-b"},
                ],
                continuity_verification_policy="prefer_healthy",
                continuity_max_capsules=4,
            )
            with patch("app.main._services", return_value=(settings, gm)), patch("app.continuity.service._trim_capsule", side_effect=lambda capsule, _max_tokens: capsule):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual(
                [item["subject_id"] for item in state["capsules"]],
                ["healthy-a", "healthy-b", "degraded-a", "conflicted-a"],
            )
            self.assertEqual(
                state["selection_order"],
                [
                    "explicit:user:healthy-a",
                    "explicit:user:healthy-b",
                    "explicit:user:degraded-a",
                    "explicit:user:conflicted-a",
                ],
            )

    def test_require_healthy_omits_unhealthy_capsules(self) -> None:
        """Require-healthy should omit degraded/conflicted capsules before budgeting."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="healthy",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="healthy",
                    verification_status="peer_confirmed",
                    health_status="healthy",
                ),
            )
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    verification_status="self_attested",
                    health_status="degraded",
                    health_reasons=["evidence stale"],
                ),
            )
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "healthy"},
                    {"subject_kind": "user", "subject_id": "degraded"},
                ],
                continuity_verification_policy="require_healthy",
                continuity_max_capsules=2,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())

            state = out["bundle"]["continuity_state"]
            self.assertEqual([item["subject_id"] for item in state["capsules"]], ["healthy"])
            self.assertEqual(state["omitted_selectors"], ["user:degraded"])

    def test_require_healthy_required_mode_raises_when_only_unhealthy_capsules_exist(self) -> None:
        """Strict required mode should 404 when no healthy capsule remains after filtering."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="degraded",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="degraded",
                    verification_status="self_attested",
                    health_status="degraded",
                    health_reasons=["needs operator review"],
                ),
            )
            req = ContextRetrieveRequest(
                task="resume",
                continuity_mode="required",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "degraded"}],
                continuity_verification_policy="require_healthy",
            )
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as cm:
                    context_retrieve(req=req, auth=_AuthStub())

            self.assertEqual(cm.exception.status_code, 404)

    def test_trim_preserves_verification_and_health_fields(self) -> None:
        """Retrieval trimming should keep V3 verification-derived fields intact."""
        payload = self._capsule_payload(
            subject_kind="user",
            subject_id="trim-me",
            verification_status="peer_confirmed",
            health_status="degraded",
            health_reasons=["manual review pending"],
            extra_metadata_keys=6,
            long_horizon=["long horizon commitment that should be dropped first"],
            canonical_sources=[
                "memory/core/identity.md",
                "memory/tasks/current.md",
            ],
        )

        trimmed = _trim_capsule(payload, 240)

        self.assertIsNotNone(trimmed)
        assert trimmed is not None
        self.assertEqual(trimmed["verification_state"]["status"], "peer_confirmed")
        self.assertEqual(trimmed["capsule_health"]["status"], "degraded")
        self.assertEqual(trimmed["capsule_health"]["reasons"], ["manual review pending"])
        self.assertNotIn("canonical_sources", trimmed)

    def test_list_includes_verification_and_health_summaries_with_defaults(self) -> None:
        """List summaries should expose V3 verification and health fields with defaults."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="plain",
                payload=self._capsule_payload(subject_kind="user", subject_id="plain"),
            )
            self._write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="verified",
                payload=self._capsule_payload(
                    subject_kind="user",
                    subject_id="verified",
                    verification_status="system_confirmed",
                    health_status="degraded",
                    health_reasons=["source drift"],
                ),
            )

            out = continuity_list_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityListRequest(limit=10),
                now=datetime.now(timezone.utc),
                audit=lambda *_args: None,
            )

            by_id = {item["subject_id"]: item for item in out["capsules"]}
            self.assertEqual(by_id["plain"]["verification_status"], "unverified")
            self.assertEqual(by_id["plain"]["health_status"], "healthy")
            self.assertEqual(by_id["plain"]["health_reasons"], [])
            self.assertEqual(by_id["verified"]["verification_status"], "system_confirmed")
            self.assertEqual(by_id["verified"]["health_status"], "degraded")
            self.assertEqual(by_id["verified"]["health_reasons"], ["source drift"])
