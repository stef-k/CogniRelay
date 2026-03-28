"""Unit tests for per-capsule trust signals (issue #121).

Tests _build_trust_signals purity, determinism, field coverage, and
_trim_capsule trimmed_fields tracking.
"""

from __future__ import annotations

import copy
import json
import unittest
from datetime import datetime, timedelta, timezone

from app.continuity.service import (
    _build_trust_signals,
    _estimated_tokens,
    _render_value,
    _trim_capsule,
)


def _now() -> datetime:
    return datetime(2026, 3, 28, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _capsule(
    *,
    updated_at: str | None = None,
    verified_at: str | None = None,
    freshness_class: str | None = "situational",
    stale_after_seconds: int | None = None,
    expires_at: str | None = None,
    open_loops: list[str] | None = None,
    top_priorities: list[str] | None = None,
    active_constraints: list[str] | None = None,
    active_concerns: list[str] | None = None,
    stance_summary: str | None = None,
    drift_signals: list[str] | None = None,
    verification_state: dict | None = None,
    capsule_health: dict | None = None,
) -> dict:
    now_iso = _iso(_now())
    freshness: dict = {}
    if freshness_class is not None:
        freshness["freshness_class"] = freshness_class
    if stale_after_seconds is not None:
        freshness["stale_after_seconds"] = stale_after_seconds
    if expires_at is not None:
        freshness["expires_at"] = expires_at
    payload: dict = {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "test",
        "updated_at": updated_at or now_iso,
        "verified_at": verified_at or now_iso,
        "verification_kind": "self_review",
        "source": {"producer": "test", "update_reason": "pre_compaction", "inputs": []},
        "continuity": {
            "top_priorities": top_priorities if top_priorities is not None else ["p1"],
            "active_concerns": active_concerns if active_concerns is not None else ["c1"],
            "active_constraints": active_constraints if active_constraints is not None else ["ac1"],
            "open_loops": open_loops if open_loops is not None else ["ol1"],
            "stance_summary": stance_summary if stance_summary is not None else "A" * 40,
            "drift_signals": drift_signals if drift_signals is not None else [],
        },
        "confidence": {"continuity": 0.85, "relationship_model": 0.0},
    }
    if freshness:
        payload["freshness"] = freshness
    if verification_state is not None:
        payload["verification_state"] = verification_state
    if capsule_health is not None:
        payload["capsule_health"] = capsule_health
    return payload


# ---------------------------------------------------------------------------
# _build_trust_signals — structure and key order
# ---------------------------------------------------------------------------


class TestBuildTrustSignalsStructure(unittest.TestCase):
    """Verify structural contracts of _build_trust_signals."""

    def test_top_level_key_order(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(list(ts.keys()), ["recency", "completeness", "integrity", "scope_match"])

    def test_recency_key_order(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(
            list(ts["recency"].keys()),
            ["updated_age_seconds", "verified_age_seconds", "phase", "freshness_class", "stale_threshold_seconds"],
        )

    def test_completeness_key_order(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(
            list(ts["completeness"].keys()),
            ["orientation_adequate", "empty_orientation_fields", "trimmed", "trimmed_fields"],
        )

    def test_integrity_key_order(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(
            list(ts["integrity"].keys()),
            ["source_state", "health_status", "health_reasons", "verification_status"],
        )

    def test_scope_match_key_order(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(list(ts["scope_match"].keys()), ["exact"])


# ---------------------------------------------------------------------------
# _build_trust_signals — recency
# ---------------------------------------------------------------------------


class TestTrustSignalsRecency(unittest.TestCase):
    """Verify recency dimension computation."""

    def test_fresh_capsule(self) -> None:
        now = _now()
        ts = _build_trust_signals(_capsule(updated_at=_iso(now), verified_at=_iso(now)), now, source_state="active")
        self.assertEqual(ts["recency"]["updated_age_seconds"], 0)
        self.assertEqual(ts["recency"]["verified_age_seconds"], 0)
        self.assertEqual(ts["recency"]["phase"], "fresh")

    def test_age_seconds_positive(self) -> None:
        now = _now()
        one_hour_ago = _iso(now - timedelta(hours=1))
        ts = _build_trust_signals(_capsule(updated_at=one_hour_ago, verified_at=one_hour_ago), now, source_state="active")
        self.assertEqual(ts["recency"]["updated_age_seconds"], 3600)
        self.assertEqual(ts["recency"]["verified_age_seconds"], 3600)

    def test_stale_soft_phase(self) -> None:
        now = _now()
        # situational stale_after = 2592000 (30 days); 1.0x < age <= 1.5x → stale_soft
        age = timedelta(seconds=int(2592000 * 1.2))
        ts = _build_trust_signals(
            _capsule(verified_at=_iso(now - age), updated_at=_iso(now - age)),
            now,
            source_state="active",
        )
        self.assertEqual(ts["recency"]["phase"], "stale_soft")

    def test_stale_hard_phase(self) -> None:
        now = _now()
        # 1.5x < age <= 2.0x → stale_hard
        age = timedelta(seconds=int(2592000 * 1.8))
        ts = _build_trust_signals(
            _capsule(verified_at=_iso(now - age), updated_at=_iso(now - age)),
            now,
            source_state="active",
        )
        self.assertEqual(ts["recency"]["phase"], "stale_hard")

    def test_expired_by_age_phase(self) -> None:
        now = _now()
        # > 2.0x → expired_by_age
        age = timedelta(seconds=int(2592000 * 2.5))
        ts = _build_trust_signals(
            _capsule(verified_at=_iso(now - age), updated_at=_iso(now - age)),
            now,
            source_state="active",
        )
        self.assertEqual(ts["recency"]["phase"], "expired_by_age")

    def test_expired_via_expires_at(self) -> None:
        now = _now()
        past = _iso(now - timedelta(hours=1))
        ts = _build_trust_signals(
            _capsule(expires_at=past, verified_at=_iso(now)),
            now,
            source_state="active",
        )
        self.assertEqual(ts["recency"]["phase"], "expired")

    def test_freshness_class_passthrough(self) -> None:
        ts = _build_trust_signals(_capsule(freshness_class="durable"), _now(), source_state="active")
        self.assertEqual(ts["recency"]["freshness_class"], "durable")

    def test_freshness_class_null_when_absent(self) -> None:
        ts = _build_trust_signals(_capsule(freshness_class=None), _now(), source_state="active")
        self.assertIsNone(ts["recency"]["freshness_class"])

    def test_stale_threshold_situational(self) -> None:
        ts = _build_trust_signals(_capsule(freshness_class="situational"), _now(), source_state="active")
        self.assertEqual(ts["recency"]["stale_threshold_seconds"], 2592000)

    def test_stale_threshold_persistent_is_null(self) -> None:
        ts = _build_trust_signals(_capsule(freshness_class="persistent"), _now(), source_state="active")
        self.assertIsNone(ts["recency"]["stale_threshold_seconds"])

    def test_age_clamped_to_zero(self) -> None:
        """Future timestamps should produce 0, not negative ages."""
        now = _now()
        future = _iso(now + timedelta(hours=1))
        ts = _build_trust_signals(_capsule(updated_at=future, verified_at=future), now, source_state="active")
        self.assertEqual(ts["recency"]["updated_age_seconds"], 0)
        self.assertEqual(ts["recency"]["verified_age_seconds"], 0)


# ---------------------------------------------------------------------------
# _build_trust_signals — completeness
# ---------------------------------------------------------------------------


class TestTrustSignalsCompleteness(unittest.TestCase):
    """Verify completeness dimension computation."""

    def test_adequate_when_all_present(self) -> None:
        ts = _build_trust_signals(
            _capsule(drift_signals=["d1"]),
            _now(),
            source_state="active",
        )
        self.assertTrue(ts["completeness"]["orientation_adequate"])
        self.assertEqual(ts["completeness"]["empty_orientation_fields"], [])

    def test_inadequate_missing_open_loops(self) -> None:
        ts = _build_trust_signals(_capsule(open_loops=[]), _now(), source_state="active")
        self.assertFalse(ts["completeness"]["orientation_adequate"])
        self.assertIn("open_loops", ts["completeness"]["empty_orientation_fields"])

    def test_inadequate_short_stance_summary(self) -> None:
        ts = _build_trust_signals(_capsule(stance_summary="short"), _now(), source_state="active")
        self.assertFalse(ts["completeness"]["orientation_adequate"])
        self.assertIn("stance_summary", ts["completeness"]["empty_orientation_fields"])

    def test_empty_fields_lists_all_missing(self) -> None:
        ts = _build_trust_signals(
            _capsule(
                open_loops=[],
                top_priorities=[],
                active_constraints=[],
                active_concerns=[],
                stance_summary="",
                drift_signals=[],
            ),
            _now(),
            source_state="active",
        )
        self.assertFalse(ts["completeness"]["orientation_adequate"])
        self.assertEqual(
            sorted(ts["completeness"]["empty_orientation_fields"]),
            ["active_concerns", "active_constraints", "drift_signals", "open_loops", "stance_summary", "top_priorities"],
        )

    def test_adequate_but_non_required_fields_empty(self) -> None:
        """orientation_adequate=True with non-empty empty_orientation_fields is valid."""
        ts = _build_trust_signals(
            _capsule(active_concerns=[], drift_signals=[]),
            _now(),
            source_state="active",
        )
        self.assertTrue(ts["completeness"]["orientation_adequate"])
        self.assertIn("active_concerns", ts["completeness"]["empty_orientation_fields"])
        self.assertIn("drift_signals", ts["completeness"]["empty_orientation_fields"])

    def test_trimmed_false_by_default(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertFalse(ts["completeness"]["trimmed"])
        self.assertEqual(ts["completeness"]["trimmed_fields"], [])

    def test_trimmed_true_with_fields(self) -> None:
        ts = _build_trust_signals(
            _capsule(),
            _now(),
            source_state="active",
            trimmed=True,
            trimmed_fields=["metadata", "continuity.trailing_notes"],
        )
        self.assertTrue(ts["completeness"]["trimmed"])
        self.assertEqual(ts["completeness"]["trimmed_fields"], ["metadata", "continuity.trailing_notes"])

    def test_trimmed_fields_is_copy(self) -> None:
        original = ["metadata"]
        ts = _build_trust_signals(_capsule(), _now(), source_state="active", trimmed=True, trimmed_fields=original)
        ts["completeness"]["trimmed_fields"].append("extra")
        self.assertEqual(original, ["metadata"])


# ---------------------------------------------------------------------------
# _build_trust_signals — integrity
# ---------------------------------------------------------------------------


class TestTrustSignalsIntegrity(unittest.TestCase):
    """Verify integrity dimension computation."""

    def test_active_healthy_defaults(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertEqual(ts["integrity"]["source_state"], "active")
        self.assertEqual(ts["integrity"]["health_status"], "healthy")
        self.assertEqual(ts["integrity"]["health_reasons"], [])
        self.assertEqual(ts["integrity"]["verification_status"], "unverified")

    def test_fallback_source_state(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="fallback")
        self.assertEqual(ts["integrity"]["source_state"], "fallback")

    def test_degraded_health(self) -> None:
        ts = _build_trust_signals(
            _capsule(capsule_health={"status": "degraded", "reasons": ["source drift"]}),
            _now(),
            source_state="active",
        )
        self.assertEqual(ts["integrity"]["health_status"], "degraded")
        self.assertEqual(ts["integrity"]["health_reasons"], ["source drift"])

    def test_conflicted_health(self) -> None:
        ts = _build_trust_signals(
            _capsule(capsule_health={"status": "conflicted", "reasons": ["two sources disagree"]}),
            _now(),
            source_state="active",
        )
        self.assertEqual(ts["integrity"]["health_status"], "conflicted")

    def test_verification_self_attested(self) -> None:
        ts = _build_trust_signals(
            _capsule(verification_state={"status": "self_attested"}),
            _now(),
            source_state="active",
        )
        self.assertEqual(ts["integrity"]["verification_status"], "self_attested")

    def test_verification_peer_confirmed(self) -> None:
        ts = _build_trust_signals(
            _capsule(verification_state={"status": "peer_confirmed"}),
            _now(),
            source_state="active",
        )
        self.assertEqual(ts["integrity"]["verification_status"], "peer_confirmed")

    def test_health_reasons_is_copy(self) -> None:
        ts = _build_trust_signals(
            _capsule(capsule_health={"status": "degraded", "reasons": ["r1"]}),
            _now(),
            source_state="active",
        )
        ts["integrity"]["health_reasons"].append("extra")
        # Original capsule reasons unaffected (we check that the function creates a copy)
        ts2 = _build_trust_signals(
            _capsule(capsule_health={"status": "degraded", "reasons": ["r1"]}),
            _now(),
            source_state="active",
        )
        self.assertEqual(ts2["integrity"]["health_reasons"], ["r1"])


# ---------------------------------------------------------------------------
# _build_trust_signals — scope_match
# ---------------------------------------------------------------------------


class TestTrustSignalsScopeMatch(unittest.TestCase):
    """Verify scope_match dimension computation."""

    def test_exact_true_for_active(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="active")
        self.assertTrue(ts["scope_match"]["exact"])

    def test_exact_false_for_fallback(self) -> None:
        ts = _build_trust_signals(_capsule(), _now(), source_state="fallback")
        self.assertFalse(ts["scope_match"]["exact"])


# ---------------------------------------------------------------------------
# _build_trust_signals — purity and determinism
# ---------------------------------------------------------------------------


class TestTrustSignalsPurity(unittest.TestCase):
    """Verify pure function guarantees."""

    def test_deterministic_output(self) -> None:
        capsule = _capsule()
        now = _now()
        ts1 = _build_trust_signals(capsule, now, source_state="active")
        ts2 = _build_trust_signals(capsule, now, source_state="active")
        self.assertEqual(ts1, ts2)
        self.assertEqual(json.dumps(ts1, sort_keys=False), json.dumps(ts2, sort_keys=False))

    def test_does_not_mutate_capsule(self) -> None:
        capsule = _capsule()
        snapshot = copy.deepcopy(capsule)
        _build_trust_signals(capsule, _now(), source_state="active")
        self.assertEqual(capsule, snapshot)

    def test_graceful_with_missing_freshness(self) -> None:
        capsule = _capsule(freshness_class=None)
        capsule.pop("freshness", None)
        ts = _build_trust_signals(capsule, _now(), source_state="active")
        self.assertIsNone(ts["recency"]["freshness_class"])
        # When freshness dict is absent, _effective_stale_seconds defaults
        # to "situational" (2592000) per the existing fallback logic.
        self.assertEqual(ts["recency"]["stale_threshold_seconds"], 2592000)

    def test_graceful_with_missing_continuity(self) -> None:
        capsule = _capsule()
        capsule.pop("continuity")
        ts = _build_trust_signals(capsule, _now(), source_state="active")
        self.assertFalse(ts["completeness"]["orientation_adequate"])
        self.assertEqual(len(ts["completeness"]["empty_orientation_fields"]), 6)


# ---------------------------------------------------------------------------
# _trim_capsule — trimmed_fields tracking
# ---------------------------------------------------------------------------


class TestTrimCapsuleTracksFields(unittest.TestCase):
    """Verify _trim_capsule returns trimmed_fields list."""

    def _large_capsule(self) -> dict:
        return _capsule(
            open_loops=["ol1"],
            top_priorities=["p1"],
            active_constraints=["ac1"],
            active_concerns=["c1"],
            stance_summary="A" * 60,
        )

    def test_no_trimming_returns_empty_list(self) -> None:
        capsule = self._large_capsule()
        trimmed, dropped = _trim_capsule(capsule, 99999)
        self.assertIsNotNone(trimmed)
        self.assertEqual(dropped, [])

    def test_metadata_dropped_appears_in_list(self) -> None:
        capsule = self._large_capsule()
        capsule["metadata"] = {"trace": "x" * 2000}
        # Use very tight budget to force trimming
        just_enough = _estimated_tokens(_render_value(capsule)) - 100
        trimmed, dropped = _trim_capsule(capsule, just_enough)
        self.assertIn("metadata", dropped)

    def test_multiple_fields_tracked(self) -> None:
        capsule = self._large_capsule()
        capsule["metadata"] = {"trace": "x" * 500}
        capsule["canonical_sources"] = ["file1.md", "file2.md"]
        capsule["continuity"]["trailing_notes"] = "notes " * 50
        capsule["continuity"]["curiosity_queue"] = ["q1", "q2"]
        # Very tight budget
        trimmed, dropped = _trim_capsule(capsule, 50)
        # At least some of these should be dropped
        self.assertTrue(len(dropped) > 0)

    def test_none_return_still_has_dropped(self) -> None:
        capsule = self._large_capsule()
        capsule["metadata"] = {"trace": "x" * 2000}
        trimmed, dropped = _trim_capsule(capsule, 1)  # impossibly small
        self.assertIsNone(trimmed)
        self.assertIsInstance(dropped, list)

    def test_tuple_return_type(self) -> None:
        capsule = self._large_capsule()
        result = _trim_capsule(capsule, 99999)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
