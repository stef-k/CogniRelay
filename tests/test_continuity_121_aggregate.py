"""Unit tests for aggregate trust signals (issue #121).

Tests _build_aggregate_trust_signals purity, determinism, and
aggregation correctness.
"""

from __future__ import annotations

import json
import unittest

from app.continuity.service import _build_aggregate_trust_signals


def _per_capsule(
    *,
    phase: str = "fresh",
    updated_age: int = 0,
    verified_age: int = 0,
    adequate: bool = True,
    trimmed: bool = False,
    health: str = "healthy",
    source_state: str = "active",
    exact: bool = True,
) -> dict:
    return {
        "recency": {
            "updated_age_seconds": updated_age,
            "verified_age_seconds": verified_age,
            "phase": phase,
            "freshness_class": "situational",
            "stale_threshold_seconds": 2592000,
        },
        "completeness": {
            "orientation_adequate": adequate,
            "empty_orientation_fields": [] if adequate else ["open_loops"],
            "trimmed": trimmed,
            "trimmed_fields": ["metadata"] if trimmed else [],
        },
        "integrity": {
            "source_state": source_state,
            "health_status": health,
            "health_reasons": [] if health == "healthy" else ["reason"],
            "verification_status": "unverified",
        },
        "scope_match": {
            "exact": exact,
        },
    }


# ---------------------------------------------------------------------------
# Structure and key order
# ---------------------------------------------------------------------------


class TestAggregateStructure(unittest.TestCase):
    """Verify structural contracts of _build_aggregate_trust_signals."""

    def test_top_level_key_order(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertEqual(list(agg.keys()), ["recency", "completeness", "integrity", "scope_match"])

    def test_recency_key_order(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertEqual(
            list(agg["recency"].keys()),
            ["worst_phase", "oldest_updated_age_seconds", "oldest_verified_age_seconds"],
        )

    def test_completeness_key_order(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertEqual(
            list(agg["completeness"].keys()),
            ["all_adequate", "adequate_count", "total_count", "any_trimmed"],
        )

    def test_integrity_key_order(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertEqual(
            list(agg["integrity"].keys()),
            ["worst_health", "any_fallback", "any_degraded", "any_conflicted"],
        )

    def test_scope_match_key_order(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertEqual(
            list(agg["scope_match"].keys()),
            ["selectors_requested", "selectors_returned", "selectors_omitted", "all_returned"],
        )


# ---------------------------------------------------------------------------
# Recency aggregation
# ---------------------------------------------------------------------------


class TestAggregateRecency(unittest.TestCase):
    """Verify worst-case recency aggregation."""

    def test_worst_phase_picks_most_severe(self) -> None:
        signals = [
            _per_capsule(phase="fresh"),
            _per_capsule(phase="stale_hard"),
            _per_capsule(phase="stale_soft"),
        ]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=3, selectors_returned=3, selectors_omitted=0)
        self.assertEqual(agg["recency"]["worst_phase"], "stale_hard")

    def test_worst_phase_expired_trumps_all(self) -> None:
        signals = [
            _per_capsule(phase="stale_hard"),
            _per_capsule(phase="expired"),
        ]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertEqual(agg["recency"]["worst_phase"], "expired")

    def test_oldest_ages(self) -> None:
        signals = [
            _per_capsule(updated_age=100, verified_age=200),
            _per_capsule(updated_age=500, verified_age=300),
        ]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertEqual(agg["recency"]["oldest_updated_age_seconds"], 500)
        self.assertEqual(agg["recency"]["oldest_verified_age_seconds"], 300)

    def test_single_capsule(self) -> None:
        signals = [_per_capsule(phase="stale_soft", updated_age=42, verified_age=84)]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=1, selectors_returned=1, selectors_omitted=0)
        self.assertEqual(agg["recency"]["worst_phase"], "stale_soft")
        self.assertEqual(agg["recency"]["oldest_updated_age_seconds"], 42)
        self.assertEqual(agg["recency"]["oldest_verified_age_seconds"], 84)


# ---------------------------------------------------------------------------
# Completeness aggregation
# ---------------------------------------------------------------------------


class TestAggregateCompleteness(unittest.TestCase):
    """Verify completeness aggregation."""

    def test_all_adequate_true(self) -> None:
        signals = [_per_capsule(adequate=True), _per_capsule(adequate=True)]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertTrue(agg["completeness"]["all_adequate"])
        self.assertEqual(agg["completeness"]["adequate_count"], 2)
        self.assertEqual(agg["completeness"]["total_count"], 2)

    def test_all_adequate_false_when_one_inadequate(self) -> None:
        signals = [_per_capsule(adequate=True), _per_capsule(adequate=False)]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertFalse(agg["completeness"]["all_adequate"])
        self.assertEqual(agg["completeness"]["adequate_count"], 1)

    def test_any_trimmed_true(self) -> None:
        signals = [_per_capsule(trimmed=False), _per_capsule(trimmed=True)]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertTrue(agg["completeness"]["any_trimmed"])

    def test_any_trimmed_false(self) -> None:
        signals = [_per_capsule(trimmed=False)]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=1, selectors_returned=1, selectors_omitted=0)
        self.assertFalse(agg["completeness"]["any_trimmed"])


# ---------------------------------------------------------------------------
# Integrity aggregation
# ---------------------------------------------------------------------------


class TestAggregateIntegrity(unittest.TestCase):
    """Verify integrity aggregation."""

    def test_worst_health_degraded(self) -> None:
        signals = [_per_capsule(health="healthy"), _per_capsule(health="degraded")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertEqual(agg["integrity"]["worst_health"], "degraded")

    def test_worst_health_conflicted(self) -> None:
        signals = [_per_capsule(health="degraded"), _per_capsule(health="conflicted")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertEqual(agg["integrity"]["worst_health"], "conflicted")

    def test_any_fallback(self) -> None:
        signals = [_per_capsule(source_state="active"), _per_capsule(source_state="fallback")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertTrue(agg["integrity"]["any_fallback"])

    def test_no_fallback(self) -> None:
        signals = [_per_capsule(source_state="active")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=1, selectors_returned=1, selectors_omitted=0)
        self.assertFalse(agg["integrity"]["any_fallback"])

    def test_any_degraded(self) -> None:
        signals = [_per_capsule(health="healthy"), _per_capsule(health="degraded")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertTrue(agg["integrity"]["any_degraded"])
        self.assertFalse(agg["integrity"]["any_conflicted"])

    def test_any_conflicted(self) -> None:
        signals = [_per_capsule(health="conflicted")]
        agg = _build_aggregate_trust_signals(signals, selectors_requested=1, selectors_returned=1, selectors_omitted=0)
        self.assertTrue(agg["integrity"]["any_conflicted"])


# ---------------------------------------------------------------------------
# Scope match aggregation
# ---------------------------------------------------------------------------


class TestAggregateScopeMatch(unittest.TestCase):
    """Verify scope_match aggregation."""

    def test_all_returned_true(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=1,
            selectors_returned=1,
            selectors_omitted=0,
        )
        self.assertTrue(agg["scope_match"]["all_returned"])
        self.assertEqual(agg["scope_match"]["selectors_requested"], 1)
        self.assertEqual(agg["scope_match"]["selectors_returned"], 1)
        self.assertEqual(agg["scope_match"]["selectors_omitted"], 0)

    def test_partial_coverage(self) -> None:
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=3,
            selectors_returned=1,
            selectors_omitted=2,
        )
        self.assertFalse(agg["scope_match"]["all_returned"])
        self.assertEqual(agg["scope_match"]["selectors_omitted"], 2)

    def test_zero_requested_not_all_returned(self) -> None:
        """all_returned is False when selectors_requested == 0."""
        agg = _build_aggregate_trust_signals(
            [_per_capsule()],
            selectors_requested=0,
            selectors_returned=0,
            selectors_omitted=0,
        )
        self.assertFalse(agg["scope_match"]["all_returned"])


# ---------------------------------------------------------------------------
# Purity and determinism
# ---------------------------------------------------------------------------


class TestAggregateEdgeCases(unittest.TestCase):
    """Verify edge case handling."""

    def test_empty_signals_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            _build_aggregate_trust_signals(
                [],
                selectors_requested=0,
                selectors_returned=0,
                selectors_omitted=0,
            )


class TestAggregatePurity(unittest.TestCase):
    """Verify pure function guarantees."""

    def test_deterministic(self) -> None:
        signals = [_per_capsule(phase="stale_soft"), _per_capsule(phase="fresh")]
        agg1 = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        agg2 = _build_aggregate_trust_signals(signals, selectors_requested=2, selectors_returned=2, selectors_omitted=0)
        self.assertEqual(agg1, agg2)
        self.assertEqual(json.dumps(agg1, sort_keys=False), json.dumps(agg2, sort_keys=False))

    def test_does_not_mutate_input(self) -> None:
        signals = [_per_capsule()]
        import copy

        snapshot = copy.deepcopy(signals)
        _build_aggregate_trust_signals(signals, selectors_requested=1, selectors_returned=1, selectors_omitted=0)
        self.assertEqual(signals, snapshot)


if __name__ == "__main__":
    unittest.main()
