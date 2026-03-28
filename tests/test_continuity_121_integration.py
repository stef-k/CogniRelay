"""Integration tests for trust signals on retrieval endpoints (issue #121).

Tests trust_signals presence, nullability, and structure through the
actual endpoint paths: /v1/continuity/read, startup view, and
build_continuity_state.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import continuity_read, context_retrieve
from app.models import ContinuityReadRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


class _AuthStub(AllowAllAuthStub):
    pass


class _GitManagerStub(SimpleGitManagerStub):
    pass


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


def _capsule_payload(
    *,
    subject_kind: str = "user",
    subject_id: str = "stef",
    freshness_class: str = "situational",
    capsule_health: dict | None = None,
    verification_state: dict | None = None,
    updated_at: str | None = None,
    verified_at: str | None = None,
) -> dict:
    now = updated_at or _iso(_now())
    payload: dict = {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": verified_at or now,
        "verification_kind": "self_review",
        "source": {"producer": "test", "update_reason": "pre_compaction", "inputs": []},
        "continuity": {
            "top_priorities": ["priority one"],
            "active_concerns": ["concern one"],
            "active_constraints": ["constraint one"],
            "open_loops": ["loop one"],
            "stance_summary": "Current stance text with at least thirty chars",
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.85, "relationship_model": 0.0},
        "freshness": {"freshness_class": freshness_class},
    }
    if capsule_health is not None:
        payload["capsule_health"] = capsule_health
    if verification_state is not None:
        payload["verification_state"] = verification_state
    return payload


def _write_capsule(repo_root: Path, payload: dict) -> None:
    kind = payload["subject_kind"]
    sid = payload["subject_id"]
    capsule_dir = repo_root / "memory" / "continuity"
    capsule_dir.mkdir(parents=True, exist_ok=True)
    path = capsule_dir / f"{kind}-{sid}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_fallback(repo_root: Path, capsule: dict) -> None:
    kind = capsule["subject_kind"]
    sid = capsule["subject_id"]
    fallback_dir = repo_root / "memory" / "continuity" / "fallback"
    fallback_dir.mkdir(parents=True, exist_ok=True)
    envelope = {
        "schema_type": "continuity_fallback_snapshot",
        "schema_version": "1.0",
        "subject_kind": kind,
        "subject_id": sid,
        "capsule": capsule,
        "snapshot_reason": "test",
        "snapshot_source": "test",
    }
    path = fallback_dir / f"{kind}-{sid}.json"
    path.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# /v1/continuity/read — trust_signals
# ---------------------------------------------------------------------------


class TestReadEndpointTrustSignals(unittest.TestCase):
    """Trust signals on /v1/continuity/read."""

    def test_active_capsule_has_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            payload = _capsule_payload()
            _write_capsule(repo, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertIn("trust_signals", out)
            ts = out["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertEqual(list(ts.keys()), ["recency", "completeness", "integrity", "scope_match"])

    def test_missing_capsule_trust_signals_null(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="gone", allow_fallback=True),
                    auth=_AuthStub(),
                )
            self.assertIsNone(out["trust_signals"])
            self.assertEqual(out["source_state"], "missing")

    def test_fallback_capsule_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            payload = _capsule_payload()
            _write_fallback(repo, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["source_state"], "fallback")
            ts = out["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertEqual(ts["integrity"]["source_state"], "fallback")
            self.assertFalse(ts["scope_match"]["exact"])

    def test_active_capsule_scope_match_exact_true(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertTrue(out["trust_signals"]["scope_match"]["exact"])

    def test_trimmed_always_false_on_read_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertFalse(out["trust_signals"]["completeness"]["trimmed"])
            self.assertEqual(out["trust_signals"]["completeness"]["trimmed_fields"], [])

    def test_recency_phase_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["trust_signals"]["recency"]["phase"], "fresh")

    def test_degraded_health_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            payload = _capsule_payload(capsule_health={"status": "degraded", "reasons": ["source drift"], "last_checked_at": "2026-03-28T12:00:00Z"})
            _write_capsule(repo, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["trust_signals"]["integrity"]["health_status"], "degraded")
            self.assertEqual(out["trust_signals"]["integrity"]["health_reasons"], ["source drift"])

    def test_completeness_adequate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertTrue(out["trust_signals"]["completeness"]["orientation_adequate"])

    def test_backward_compat_existing_fields_unchanged(self) -> None:
        """Existing response keys are preserved alongside trust_signals."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertTrue(out["ok"])
            self.assertIn("capsule", out)
            self.assertIn("source_state", out)
            self.assertIn("recovery_warnings", out)
            self.assertIn("trust_signals", out)


# ---------------------------------------------------------------------------
# startup view — trust_signals
# ---------------------------------------------------------------------------


class TestStartupViewTrustSignals(unittest.TestCase):
    """Trust signals in startup_summary."""

    def test_startup_summary_includes_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertIn("trust_signals", ss)
            self.assertIsNotNone(ss["trust_signals"])

    def test_startup_summary_key_order(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(
                list(ss.keys()),
                ["recovery", "orientation", "context", "updated_at", "trust_signals"],
            )

    def test_startup_missing_capsule_trust_signals_null(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="gone", allow_fallback=True, view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertIsNone(ss["trust_signals"])

    def test_startup_trust_signals_matches_top_level(self) -> None:
        """startup_summary.trust_signals should equal the top-level trust_signals."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["trust_signals"], out["startup_summary"]["trust_signals"])


# ---------------------------------------------------------------------------
# build_continuity_state — per-capsule and aggregate trust_signals
# ---------------------------------------------------------------------------


class TestBuildContinuityStateTrustSignals(unittest.TestCase):
    """Trust signals in build_continuity_state (context retrieval)."""

    def test_single_capsule_per_capsule_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertTrue(state["present"])
            self.assertEqual(len(state["capsules"]), 1)
            cap = state["capsules"][0]
            self.assertIn("trust_signals", cap)
            ts = cap["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertEqual(list(ts.keys()), ["recency", "completeness", "integrity", "scope_match"])

    def test_aggregate_trust_signals_present(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertIn("trust_signals", state)
            agg = state["trust_signals"]
            self.assertIsNotNone(agg)
            self.assertEqual(list(agg.keys()), ["recency", "completeness", "integrity", "scope_match"])

    def test_aggregate_scope_match_counts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload(subject_id="a"))
            # subject "b" not written → will be omitted
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                ],
                continuity_resilience_policy="require_active",
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            agg = state["trust_signals"]
            self.assertIsNotNone(agg)
            self.assertEqual(agg["scope_match"]["selectors_requested"], 2)
            self.assertEqual(agg["scope_match"]["selectors_returned"], 1)
            self.assertFalse(agg["scope_match"]["all_returned"])

    def test_no_capsules_loaded_trust_signals_absent(self) -> None:
        """When no capsules load, trust_signals should not appear in state."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "gone"}],
                continuity_mode="auto",
                continuity_resilience_policy="require_active",
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertFalse(state["present"])
            # trust_signals key not expected in early-return paths
            self.assertNotIn("trust_signals", state)

    def test_continuity_off_no_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            req = ContextRetrieveRequest(
                task="resume",
                continuity_mode="off",
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertFalse(state["present"])

    def test_multi_capsule_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload(subject_id="a"))
            _write_capsule(repo, _capsule_payload(subject_id="b"))
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                ],
                continuity_max_capsules=2,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertEqual(len(state["capsules"]), 2)
            # Both per-capsule trust_signals present
            for cap in state["capsules"]:
                self.assertIn("trust_signals", cap)
                self.assertIsNotNone(cap["trust_signals"])
            # Aggregate present
            agg = state["trust_signals"]
            self.assertEqual(agg["completeness"]["total_count"], 2)
            self.assertTrue(agg["scope_match"]["all_returned"])

    def test_backward_compat_warnings_unchanged(self) -> None:
        """Existing warnings/recovery_warnings are unchanged by trust_signals addition."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(req=req, auth=_AuthStub())
            state = out["bundle"]["continuity_state"]
            self.assertIn("warnings", state)
            self.assertIn("recovery_warnings", state)
            self.assertIsInstance(state["warnings"], list)
            self.assertIsInstance(state["recovery_warnings"], list)


if __name__ == "__main__":
    unittest.main()
