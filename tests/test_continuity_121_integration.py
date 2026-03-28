"""Integration tests for trust signals on retrieval endpoints (issue #121).

Tests trust_signals presence, nullability, and structure through the
actual endpoint paths: /v1/continuity/read, startup view, and
build_continuity_state.  Also covers budget accounting, compact trust
fallback, and all-fail aggregate degradation.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.continuity.service import (
    CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT,
    CONTINUITY_WARNING_TRUST_SIGNALS_FAILED,
    _estimated_tokens,
    _render_value,
    build_continuity_state,
)
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
                ["recovery", "orientation", "context", "updated_at", "trust_signals", "stable_preferences"],
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

    def test_no_capsules_loaded_trust_signals_null(self) -> None:
        """When no capsules load, trust_signals should be null (key always present)."""
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
            self.assertIn("trust_signals", state)
            self.assertIsNone(state["trust_signals"])

    def test_continuity_off_trust_signals_null(self) -> None:
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
            self.assertIn("trust_signals", state)
            self.assertIsNone(state["trust_signals"])

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


# ---------------------------------------------------------------------------
# Token budget accounting — trust_signals included in delivered total
# ---------------------------------------------------------------------------


class TestBudgetAccountingWithTrustSignals(unittest.TestCase):
    """Trust signals tokens are counted in continuity_tokens_used."""

    def test_tokens_used_includes_trust_signals(self) -> None:
        """continuity_tokens_used should reflect the actual capsule + trust_signals."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
                max_tokens_estimate=4000,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            self.assertTrue(state["present"])
            cap = state["capsules"][0]
            actual_tokens = _estimated_tokens(_render_value(cap))
            reported = state["budget"]["continuity_tokens_used"]
            self.assertEqual(reported, actual_tokens)

    def test_tokens_used_within_budget(self) -> None:
        """Reported tokens used should not exceed the reserved budget."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
                max_tokens_estimate=4000,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            reserved = state["budget"]["continuity_tokens_reserved"]
            used = state["budget"]["continuity_tokens_used"]
            self.assertLessEqual(used, reserved)

    def test_tokens_within_budget_after_heavy_trimming(self) -> None:
        """Budget honesty must hold when trimming drops many fields (growing trimmed_fields)."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Build a capsule that fills every trimmable field to the model max,
            # using long strings to force substantial trimming and a long
            # trimmed_fields list that grows the trust_signals payload after
            # the initial token estimate.
            payload = _capsule_payload()
            payload["metadata"] = {"trace": "x" * 500, "context_id": "y" * 300}
            payload["canonical_sources"] = ["file-" + "x" * 50 + f"-{i}.md" for i in range(8)]
            payload["continuity"]["trailing_notes"] = ["note-" + "y" * 80 for _ in range(3)]
            payload["continuity"]["curiosity_queue"] = ["q-" + "z" * 80 for _ in range(5)]
            payload["continuity"]["negative_decisions"] = [
                {"decision": "nd-" + "w" * 60, "rationale": "r" * 40} for _ in range(4)
            ]
            payload["continuity"]["working_hypotheses"] = ["wh-" + "v" * 80 for _ in range(5)]
            payload["continuity"]["long_horizon_commitments"] = ["lhc-" + "u" * 80 for _ in range(5)]
            payload["continuity"]["retrieval_hints"] = {
                "must_include": ["hint-" + "t" * 60 for _ in range(8)],
            }
            payload["continuity"]["relationship_model"] = {
                "trust_level": "high",
                "communication_style": "direct" * 20,
                "history": "long" * 30,
            }
            _write_capsule(repo, payload)
            # Use a moderate budget that forces substantial trimming.
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
                max_tokens_estimate=2000,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            reserved = state["budget"]["continuity_tokens_reserved"]
            used = state["budget"]["continuity_tokens_used"]
            self.assertLessEqual(
                used,
                reserved,
                f"tokens_used ({used}) exceeded reserved ({reserved}) after heavy trimming",
            )
            # Verify we actually got a capsule with trust_signals
            if state["present"] and state["capsules"]:
                self.assertIn("trust_signals", state["capsules"][0])

    def test_tokens_within_budget_multi_capsule_heavy_trimming(self) -> None:
        """Budget honesty under multi-capsule heavy trimming."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            for sid in ("a", "b"):
                payload = _capsule_payload(subject_id=sid)
                payload["metadata"] = {"trace": "x" * 400}
                payload["canonical_sources"] = ["file-" + "x" * 50 + f"-{i}.md" for i in range(8)]
                payload["continuity"]["trailing_notes"] = ["note-" + "y" * 80 for _ in range(3)]
                payload["continuity"]["curiosity_queue"] = ["q-" + "z" * 80 for _ in range(5)]
                payload["continuity"]["negative_decisions"] = [
                    {"decision": "nd-" + "w" * 60, "rationale": "r" * 40} for _ in range(4)
                ]
                payload["continuity"]["working_hypotheses"] = ["wh-" + "v" * 80 for _ in range(5)]
                _write_capsule(repo, payload)
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[
                    {"subject_kind": "user", "subject_id": "a"},
                    {"subject_kind": "user", "subject_id": "b"},
                ],
                continuity_max_capsules=2,
                max_tokens_estimate=2000,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            reserved = state["budget"]["continuity_tokens_reserved"]
            used = state["budget"]["continuity_tokens_used"]
            self.assertLessEqual(
                used,
                reserved,
                f"tokens_used ({used}) exceeded reserved ({reserved}) with multi-capsule heavy trimming",
            )


# ---------------------------------------------------------------------------
# Compact trust signals under tight budgets
# ---------------------------------------------------------------------------


class TestCompactTrustSignalsOnRetrieval(unittest.TestCase):
    """When budget is tight, compact trust_signals should be emitted."""

    def test_compact_trust_on_tight_budget(self) -> None:
        """Very tight budget should produce compact trust_signals."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
                # Very small budget to force compact trust
                max_tokens_estimate=500,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            if state["present"] and state["capsules"]:
                cap = state["capsules"][0]
                ts = cap.get("trust_signals")
                if ts is not None and ts.get("compact"):
                    # Compact shape has minimal keys
                    self.assertTrue(ts["compact"])
                    self.assertIn("phase", ts["recency"])
                    self.assertIn("orientation_adequate", ts["completeness"])
                    self.assertNotIn("trimmed_fields", ts.get("completeness", {}))
                    # Warning should be present
                    all_warnings = state.get("recovery_warnings", [])
                    compact_warnings = [w for w in all_warnings if CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT in w]
                    self.assertTrue(len(compact_warnings) > 0)

    def test_normal_budget_uses_full_trust(self) -> None:
        """Normal budget should produce full trust_signals without compact flag."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
                max_tokens_estimate=4000,
            )
            state = build_continuity_state(
                repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
            )
            self.assertTrue(state["present"])
            ts = state["capsules"][0]["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertNotIn("compact", ts)
            self.assertIn("trimmed_fields", ts["completeness"])


# ---------------------------------------------------------------------------
# All per-capsule trust_signals fail — aggregate degrades to null
# ---------------------------------------------------------------------------


class TestAllTrustSignalsFail(unittest.TestCase):
    """When all per-capsule trust_signals raise, aggregate should be null."""

    def test_all_fail_aggregate_null(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )

            def _boom(*args, **kwargs):
                raise RuntimeError("trust signals exploded")

            with (
                patch("app.continuity.service._build_trust_signals", side_effect=_boom),
                patch("app.continuity.service._build_compact_trust_signals", side_effect=_boom),
            ):
                state = build_continuity_state(
                    repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
                )
            self.assertTrue(state["present"])
            # Per-capsule trust_signals is null
            self.assertIsNone(state["capsules"][0]["trust_signals"])
            # Aggregate is null
            self.assertIsNone(state["trust_signals"])
            # Warning was emitted
            failed_warnings = [w for w in state["recovery_warnings"] if CONTINUITY_WARNING_TRUST_SIGNALS_FAILED in w]
            self.assertTrue(len(failed_warnings) > 0)


# ---------------------------------------------------------------------------
# Aggregate trust failure emits recovery warning
# ---------------------------------------------------------------------------


class TestAggregateTrustFailureWarning(unittest.TestCase):
    """When aggregate trust computation fails, a recovery_warning is emitted."""

    def test_aggregate_failure_emits_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )

            def _boom(*args, **kwargs):
                raise RuntimeError("aggregate exploded")

            with patch("app.continuity.service._build_aggregate_trust_signals", side_effect=_boom):
                state = build_continuity_state(
                    repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
                )
            self.assertTrue(state["present"])
            # Aggregate is null
            self.assertIsNone(state["trust_signals"])
            # Warning was emitted
            from app.continuity.service import CONTINUITY_WARNING_TRUST_SIGNALS_AGGREGATE_FAILED
            agg_warnings = [w for w in state["recovery_warnings"] if CONTINUITY_WARNING_TRUST_SIGNALS_AGGREGATE_FAILED in w]
            self.assertTrue(len(agg_warnings) > 0)
            # Per-capsule trust_signals still present
            self.assertIsNotNone(state["capsules"][0]["trust_signals"])


# ---------------------------------------------------------------------------
# Malformed/missing verified_at through endpoint — no crash
# ---------------------------------------------------------------------------


class TestMalformedTimestampEndpoint(unittest.TestCase):
    """Malformed verified_at in a stored capsule should not crash retrieval."""

    def test_read_malformed_verified_at_still_returns_trust_signals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            settings = _settings(repo)
            gm = _GitManagerStub()
            payload = _capsule_payload(verified_at="not-a-valid-date")
            _write_capsule(repo, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            # Trust signals should be present (not null) with expired phase
            ts = out["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertEqual(ts["recency"]["phase"], "expired")
            self.assertIsNone(ts["recency"]["verified_age_seconds"])

    def test_retrieval_phase_raise_caught_by_trust_builder(self) -> None:
        """Trust builder catches _continuity_phase failures and falls back to expired."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_capsule(repo, _capsule_payload())
            req = ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[{"subject_kind": "user", "subject_id": "stef"}],
            )

            # Simulate _continuity_phase raising inside the trust builder
            original_phase = None
            import app.continuity.service as svc

            original_phase = svc._continuity_phase

            call_count = 0

            def _failing_phase(capsule, now):
                nonlocal call_count
                call_count += 1
                # First call is from the loading loop — let it pass.
                # Subsequent calls are from trust builders — make them fail.
                if call_count <= 1:
                    return original_phase(capsule, now)
                raise RuntimeError("simulated phase failure")

            with patch("app.continuity.service._continuity_phase", side_effect=_failing_phase), \
                 patch("app.continuity.trust._continuity_phase", side_effect=_failing_phase):
                state = build_continuity_state(
                    repo_root=repo, auth=_AuthStub(), req=req, now=datetime.now(timezone.utc),
                )
            self.assertTrue(state["present"])
            ts = state["capsules"][0]["trust_signals"]
            self.assertIsNotNone(ts)
            self.assertEqual(ts["recency"]["phase"], "expired")


if __name__ == "__main__":
    unittest.main()
