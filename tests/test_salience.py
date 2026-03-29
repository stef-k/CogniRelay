"""Unit and integration tests for continuity salience ranking (issue #123).

Tests sort-key computation for all five signals, rank value mappings,
tiebreaker ordering, salience block schema validation, salience metadata
aggregation, edge cases, and end-to-end integration through
build_continuity_state and continuity_list_service.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.continuity.constants import (
    CONTINUITY_HEALTH_ORDER,
    CONTINUITY_PHASE_SEVERITY,
    CONTINUITY_SIGNAL_RANK,
    CONTINUITY_WARNING_SALIENCE_OMITTED,
    SALIENCE_LIFECYCLE_NO_DESCRIPTOR,
    SALIENCE_LIFECYCLE_RANK,
)
from app.continuity.salience import (
    _freshness_rank,
    _health_rank,
    _lifecycle_rank,
    _resume_adequate,
    _salience_block,
    _salience_metadata,
    _salience_sort,
    _salience_sort_key,
    _updated_age_seconds,
    _verification_rank,
)
from app.continuity.service import build_continuity_state, continuity_list_service
from app.models import ContinuityListRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


_STANCE_ADEQUATE = "A" * 30  # Exactly at the minimum threshold.
_STANCE_INADEQUATE = "B" * 29  # One char below threshold.


def _capsule(
    *,
    subject_kind: str = "thread",
    subject_id: str = "t1",
    updated_at: str | None = None,
    verified_at: str | None = None,
    freshness_class: str = "situational",
    stale_after_seconds: int | None = None,
    lifecycle: str | None = "active",
    verification_kind: str = "self_review",
    verification_state: dict | None = None,
    capsule_health: dict | None = None,
    open_loops: list[str] | None = None,
    top_priorities: list[str] | None = None,
    active_constraints: list[str] | None = None,
    stance_summary: str | None = None,
    thread_descriptor: dict | None = ...,  # type: ignore[assignment]
) -> dict:
    """Build a capsule dict suitable for salience tests."""
    now_iso = _iso(_now())
    freshness: dict = {"freshness_class": freshness_class}
    if stale_after_seconds is not None:
        freshness["stale_after_seconds"] = stale_after_seconds

    payload: dict = {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": updated_at or now_iso,
        "verified_at": verified_at or now_iso,
        "verification_kind": verification_kind,
        "source": {"producer": "test", "update_reason": "pre_compaction", "inputs": []},
        "continuity": {
            "top_priorities": top_priorities if top_priorities is not None else ["p1"],
            "active_concerns": ["c1"],
            "active_constraints": active_constraints if active_constraints is not None else ["ac1"],
            "open_loops": open_loops if open_loops is not None else ["ol1"],
            "stance_summary": stance_summary if stance_summary is not None else _STANCE_ADEQUATE,
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.85, "relationship_model": 0.0},
        "freshness": freshness,
    }
    if verification_state is not None:
        payload["verification_state"] = verification_state
    if capsule_health is not None:
        payload["capsule_health"] = capsule_health
    # Build thread_descriptor unless explicitly set to None.
    if thread_descriptor is ...:
        if lifecycle is not None:
            payload["thread_descriptor"] = {"label": "test", "keywords": [], "lifecycle": lifecycle}
    elif thread_descriptor is not None:
        payload["thread_descriptor"] = thread_descriptor
    return payload


def _row(capsule: dict, *, source_state: str = "active", resolution: str = "exact") -> dict:
    """Wrap a capsule dict in a loaded-capsule row as produced by _load_selectors_with_fallback."""
    from app.continuity.freshness import _capsule_health_summary, _verification_status

    health_status, health_reasons = _capsule_health_summary(capsule)
    return {
        "selector": {
            "subject_kind": capsule["subject_kind"],
            "subject_id": capsule["subject_id"],
            "resolution": resolution,
        },
        "capsule": capsule,
        "verification_status": _verification_status(capsule),
        "health_status": health_status,
        "health_reasons": health_reasons,
        "source_state": source_state,
    }


# ---------------------------------------------------------------------------
# 1) Sort-key component: lifecycle rank
# ---------------------------------------------------------------------------


class TestLifecycleRank(unittest.TestCase):
    """Verify lifecycle rank values and missing-descriptor fallback."""

    def test_active_is_zero(self) -> None:
        self.assertEqual(_lifecycle_rank(_capsule(lifecycle="active")), 0)

    def test_suspended_is_one(self) -> None:
        self.assertEqual(_lifecycle_rank(_capsule(lifecycle="suspended")), 1)

    def test_concluded_is_two(self) -> None:
        self.assertEqual(_lifecycle_rank(_capsule(lifecycle="concluded")), 2)

    def test_superseded_is_three(self) -> None:
        self.assertEqual(_lifecycle_rank(_capsule(lifecycle="superseded")), 3)

    def test_no_descriptor_uses_sentinel(self) -> None:
        c = _capsule(thread_descriptor=None, lifecycle=None)
        self.assertEqual(_lifecycle_rank(c), SALIENCE_LIFECYCLE_NO_DESCRIPTOR)

    def test_unknown_lifecycle_uses_sentinel(self) -> None:
        c = _capsule()
        c["thread_descriptor"]["lifecycle"] = "unknown_value"
        self.assertEqual(_lifecycle_rank(c), SALIENCE_LIFECYCLE_NO_DESCRIPTOR)

    def test_rank_ordering_matches_constant(self) -> None:
        for name, rank in SALIENCE_LIFECYCLE_RANK.items():
            with self.subTest(lifecycle=name):
                self.assertEqual(_lifecycle_rank(_capsule(lifecycle=name)), rank)


# ---------------------------------------------------------------------------
# 2) Sort-key component: health rank
# ---------------------------------------------------------------------------


class TestHealthRank(unittest.TestCase):
    """Verify health rank values from both row and capsule paths."""

    def test_healthy_is_zero(self) -> None:
        row = _row(_capsule())
        self.assertEqual(_health_rank(row), 0)

    def test_degraded_is_one(self) -> None:
        row = _row(_capsule(capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())}))
        self.assertEqual(_health_rank(row), 1)

    def test_conflicted_is_two(self) -> None:
        row = _row(_capsule(capsule_health={"status": "conflicted", "reasons": ["r"], "last_checked_at": _iso(_now())}))
        self.assertEqual(_health_rank(row), 2)

    def test_unknown_health_maps_to_conflicted(self) -> None:
        row = _row(_capsule())
        row["health_status"] = "mystery"
        self.assertEqual(_health_rank(row), CONTINUITY_HEALTH_ORDER["conflicted"])

    def test_list_summary_row_uses_health_status_key(self) -> None:
        """List-summary rows carry health_status directly, no capsule sub-key."""
        summary_row = {"health_status": "degraded", "subject_kind": "thread", "subject_id": "t1"}
        self.assertEqual(_health_rank(summary_row), 1)


# ---------------------------------------------------------------------------
# 3) Sort-key component: freshness rank
# ---------------------------------------------------------------------------


class TestFreshnessRank(unittest.TestCase):
    """Verify freshness-phase rank values."""

    def test_fresh_is_zero(self) -> None:
        self.assertEqual(_freshness_rank(_capsule(), _now()), 0)

    def test_stale_soft(self) -> None:
        age = 100
        stale_threshold = 80
        past = _now() - timedelta(seconds=age)
        c = _capsule(verified_at=_iso(past), stale_after_seconds=stale_threshold)
        self.assertEqual(_freshness_rank(c, _now()), CONTINUITY_PHASE_SEVERITY["stale_soft"])

    def test_stale_hard(self) -> None:
        stale_threshold = 100
        age = int(stale_threshold * 1.6)
        past = _now() - timedelta(seconds=age)
        c = _capsule(verified_at=_iso(past), stale_after_seconds=stale_threshold)
        self.assertEqual(_freshness_rank(c, _now()), CONTINUITY_PHASE_SEVERITY["stale_hard"])

    def test_pre_computed_phase_in_list_row(self) -> None:
        """List-summary rows carry a pre-computed 'phase' key."""
        row = {"phase": "stale_soft", "subject_kind": "thread", "subject_id": "t1"}
        self.assertEqual(_freshness_rank(row, _now()), CONTINUITY_PHASE_SEVERITY["stale_soft"])

    def test_rank_ordering_matches_constants(self) -> None:
        for phase, severity in CONTINUITY_PHASE_SEVERITY.items():
            with self.subTest(phase=phase):
                row = {"phase": phase}
                self.assertEqual(_freshness_rank(row, _now()), severity)


# ---------------------------------------------------------------------------
# 4) Sort-key component: resume adequacy
# ---------------------------------------------------------------------------


class TestResumeAdequacy(unittest.TestCase):
    """Verify resume-quality derivation from raw dict."""

    def test_adequate_when_all_present(self) -> None:
        c = _capsule(
            open_loops=["ol"],
            top_priorities=["tp"],
            active_constraints=["ac"],
            stance_summary=_STANCE_ADEQUATE,
        )
        self.assertTrue(_resume_adequate(c))

    def test_inadequate_when_stance_too_short(self) -> None:
        c = _capsule(stance_summary=_STANCE_INADEQUATE)
        self.assertFalse(_resume_adequate(c))

    def test_inadequate_when_open_loops_empty(self) -> None:
        c = _capsule(open_loops=[])
        self.assertFalse(_resume_adequate(c))

    def test_inadequate_when_top_priorities_empty(self) -> None:
        c = _capsule(top_priorities=[])
        self.assertFalse(_resume_adequate(c))

    def test_inadequate_when_active_constraints_empty(self) -> None:
        c = _capsule(active_constraints=[])
        self.assertFalse(_resume_adequate(c))

    def test_inadequate_when_no_continuity(self) -> None:
        c = _capsule()
        del c["continuity"]
        self.assertFalse(_resume_adequate(c))


# ---------------------------------------------------------------------------
# 5) Sort-key component: verification rank
# ---------------------------------------------------------------------------


class TestVerificationRank(unittest.TestCase):
    """Verify verification signal rank derivation."""

    def test_absent_state_returns_zero(self) -> None:
        self.assertEqual(_verification_rank(_capsule()), 0)

    def test_self_review_returns_zero(self) -> None:
        c = _capsule(verification_state={"status": "self_attested", "kind": "self_review"})
        self.assertEqual(_verification_rank(c), CONTINUITY_SIGNAL_RANK["self_review"])

    def test_user_confirmation_returns_three(self) -> None:
        c = _capsule(verification_state={"status": "user_confirmed", "kind": "user_confirmation"})
        self.assertEqual(_verification_rank(c), CONTINUITY_SIGNAL_RANK["user_confirmation"])

    def test_system_check_returns_four(self) -> None:
        c = _capsule(verification_state={"status": "system_confirmed", "kind": "system_check"})
        self.assertEqual(_verification_rank(c), CONTINUITY_SIGNAL_RANK["system_check"])

    def test_falls_back_to_verification_kind(self) -> None:
        """When verification_state has no 'kind', falls back to top-level verification_kind."""
        c = _capsule(verification_kind="peer_confirmation", verification_state={"status": "confirmed"})
        self.assertEqual(_verification_rank(c), CONTINUITY_SIGNAL_RANK["peer_confirmation"])

    def test_list_summary_row_with_verification_kind(self) -> None:
        row = {"verification_kind": "user_confirmation"}
        self.assertEqual(_verification_rank(row), CONTINUITY_SIGNAL_RANK["user_confirmation"])


# ---------------------------------------------------------------------------
# 6) Updated-age tiebreaker
# ---------------------------------------------------------------------------


class TestUpdatedAgeSeconds(unittest.TestCase):
    """Verify updated_age_seconds computation and edge cases."""

    def test_zero_age_for_current_timestamp(self) -> None:
        c = _capsule(updated_at=_iso(_now()))
        self.assertEqual(_updated_age_seconds(c, _now()), 0)

    def test_positive_age(self) -> None:
        past = _now() - timedelta(hours=1)
        c = _capsule(updated_at=_iso(past))
        self.assertEqual(_updated_age_seconds(c, _now()), 3600)

    def test_missing_updated_at_returns_max(self) -> None:
        c = _capsule()
        del c["updated_at"]
        self.assertEqual(_updated_age_seconds(c, _now()), 2**31 - 1)

    def test_future_timestamp_clamped_to_zero(self) -> None:
        future = _now() + timedelta(hours=1)
        c = _capsule(updated_at=_iso(future))
        self.assertEqual(_updated_age_seconds(c, _now()), 0)


# ---------------------------------------------------------------------------
# 7) Full sort-key computation
# ---------------------------------------------------------------------------


class TestSalienceSortKey(unittest.TestCase):
    """Verify the composite sort key tuple."""

    def test_key_has_eight_components(self) -> None:
        row = _row(_capsule())
        key = _salience_sort_key(row, _now())
        self.assertEqual(len(key), 8)

    def test_deterministic_same_input_same_key(self) -> None:
        """Acceptance criterion #10: identical state always produces identical key."""
        row = _row(_capsule())
        self.assertEqual(
            _salience_sort_key(row, _now()),
            _salience_sort_key(row, _now()),
        )

    def test_active_before_suspended(self) -> None:
        active = _row(_capsule(lifecycle="active"))
        suspended = _row(_capsule(lifecycle="suspended"))
        self.assertLess(
            _salience_sort_key(active, _now()),
            _salience_sort_key(suspended, _now()),
        )

    def test_healthy_before_degraded(self) -> None:
        healthy = _row(_capsule())
        degraded = _row(_capsule(capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())}))
        self.assertLess(
            _salience_sort_key(healthy, _now()),
            _salience_sort_key(degraded, _now()),
        )

    def test_fresh_before_stale(self) -> None:
        fresh = _row(_capsule())
        stale = _row(_capsule(
            verified_at=_iso(_now() - timedelta(seconds=200)),
            stale_after_seconds=100,
        ))
        self.assertLess(
            _salience_sort_key(fresh, _now()),
            _salience_sort_key(stale, _now()),
        )

    def test_adequate_before_inadequate(self) -> None:
        adequate = _row(_capsule(stance_summary=_STANCE_ADEQUATE))
        inadequate = _row(_capsule(stance_summary=_STANCE_INADEQUATE))
        self.assertLess(
            _salience_sort_key(adequate, _now()),
            _salience_sort_key(inadequate, _now()),
        )

    def test_stronger_verification_before_weaker(self) -> None:
        strong = _row(_capsule(verification_state={"status": "confirmed", "kind": "system_check"}))
        weak = _row(_capsule(verification_state={"status": "self_attested", "kind": "self_review"}))
        self.assertLess(
            _salience_sort_key(strong, _now()),
            _salience_sort_key(weak, _now()),
        )

    def test_more_recent_before_older(self) -> None:
        recent = _row(_capsule(updated_at=_iso(_now())))
        older = _row(_capsule(subject_id="t1", updated_at=_iso(_now() - timedelta(hours=2))))
        # Same structural signals, different recency — must tiebreak.
        self.assertLess(
            _salience_sort_key(recent, _now()),
            _salience_sort_key(older, _now()),
        )

    def test_alphabetical_tiebreak_on_kind(self) -> None:
        """When all signals identical except subject_kind, alphabetical wins."""
        row_a = _row(_capsule(subject_kind="peer", subject_id="x"))
        row_b = _row(_capsule(subject_kind="thread", subject_id="x"))
        self.assertLess(
            _salience_sort_key(row_a, _now()),
            _salience_sort_key(row_b, _now()),
        )

    def test_alphabetical_tiebreak_on_id(self) -> None:
        """When all signals identical except subject_id, alphabetical wins."""
        row_a = _row(_capsule(subject_id="aaa"))
        row_b = _row(_capsule(subject_id="zzz"))
        self.assertLess(
            _salience_sort_key(row_a, _now()),
            _salience_sort_key(row_b, _now()),
        )

    def test_lifecycle_dominates_health(self) -> None:
        """Active-degraded sorts before suspended-healthy."""
        active_degraded = _row(_capsule(lifecycle="active", capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())}))
        suspended_healthy = _row(_capsule(lifecycle="suspended"))
        self.assertLess(
            _salience_sort_key(active_degraded, _now()),
            _salience_sort_key(suspended_healthy, _now()),
        )

    def test_no_descriptor_sorts_after_lifecycle_bearing(self) -> None:
        """Acceptance criterion #8: user capsule sorts after thread capsule."""
        thread = _row(_capsule(subject_kind="thread", subject_id="t1", lifecycle="active"))
        user = _row(_capsule(subject_kind="user", subject_id="u1", thread_descriptor=None, lifecycle=None))
        self.assertLess(
            _salience_sort_key(thread, _now()),
            _salience_sort_key(user, _now()),
        )


# ---------------------------------------------------------------------------
# 8) _salience_sort
# ---------------------------------------------------------------------------


class TestSalienceSort(unittest.TestCase):
    """Verify _salience_sort ordering."""

    def test_three_capsules_ordered(self) -> None:
        """Acceptance criterion #1: multi-capsule sort by lifecycle/health/freshness."""
        active_healthy = _row(_capsule(subject_id="t1", lifecycle="active"))
        active_degraded = _row(_capsule(subject_id="t2", lifecycle="active", capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())}))
        suspended_healthy = _row(_capsule(subject_id="t3", lifecycle="suspended"))
        rows = [suspended_healthy, active_degraded, active_healthy]
        sorted_rows = _salience_sort(rows, _now())
        ids = [r["selector"]["subject_id"] for r in sorted_rows]
        self.assertEqual(ids, ["t1", "t2", "t3"])

    def test_does_not_mutate_input(self) -> None:
        rows = [_row(_capsule(subject_id="b")), _row(_capsule(subject_id="a"))]
        original_order = [r["selector"]["subject_id"] for r in rows]
        _salience_sort(rows, _now())
        self.assertEqual([r["selector"]["subject_id"] for r in rows], original_order)

    def test_single_capsule(self) -> None:
        rows = [_row(_capsule())]
        sorted_rows = _salience_sort(rows, _now())
        self.assertEqual(len(sorted_rows), 1)

    def test_empty_list(self) -> None:
        self.assertEqual(_salience_sort([], _now()), [])


# ---------------------------------------------------------------------------
# 9) _salience_block schema
# ---------------------------------------------------------------------------


class TestSalienceBlock(unittest.TestCase):
    """Verify per-capsule salience block schema (§4a)."""

    def test_schema_keys(self) -> None:
        """Acceptance criterion #2: block has rank and sort_key."""
        row = _row(_capsule())
        block = _salience_block(row, _now(), rank=1)
        self.assertIn("rank", block)
        self.assertIn("sort_key", block)

    def test_sort_key_fields(self) -> None:
        """Acceptance criterion #2: sort_key has all five signals plus age."""
        row = _row(_capsule())
        sk = _salience_block(row, _now(), rank=1)["sort_key"]
        expected = {"lifecycle_rank", "health_rank", "freshness_rank", "resume_adequate", "verification_rank", "updated_age_seconds"}
        self.assertEqual(set(sk.keys()), expected)

    def test_rank_matches_input(self) -> None:
        row = _row(_capsule())
        block = _salience_block(row, _now(), rank=42)
        self.assertEqual(block["rank"], 42)

    def test_sort_key_values_match_components(self) -> None:
        """Each sort_key field value matches the individual component functions."""
        c = _capsule(
            lifecycle="suspended",
            capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())},
            verification_state={"status": "confirmed", "kind": "peer_confirmation"},
        )
        row = _row(c)
        sk = _salience_block(row, _now(), rank=1)["sort_key"]
        self.assertEqual(sk["lifecycle_rank"], _lifecycle_rank(c))
        self.assertEqual(sk["health_rank"], _health_rank(row))
        self.assertEqual(sk["freshness_rank"], _freshness_rank(c, _now()))
        self.assertEqual(sk["resume_adequate"], _resume_adequate(c))
        self.assertEqual(sk["verification_rank"], _verification_rank(c))
        self.assertEqual(sk["updated_age_seconds"], _updated_age_seconds(c, _now()))

    def test_verification_rank_is_natural_direction(self) -> None:
        """Sort key in block is NOT negated — natural direction (higher=better)."""
        c = _capsule(verification_state={"status": "confirmed", "kind": "system_check"})
        row = _row(c)
        sk = _salience_block(row, _now(), rank=1)["sort_key"]
        self.assertEqual(sk["verification_rank"], CONTINUITY_SIGNAL_RANK["system_check"])
        self.assertGreater(sk["verification_rank"], 0)


# ---------------------------------------------------------------------------
# 10) _salience_metadata
# ---------------------------------------------------------------------------


class TestSalienceMetadata(unittest.TestCase):
    """Verify aggregate salience metadata (§4b)."""

    def test_null_when_empty(self) -> None:
        """Acceptance criterion #4: no capsules → null."""
        self.assertIsNone(_salience_metadata([], _now()))

    def test_schema_keys(self) -> None:
        """Acceptance criterion #3: metadata has expected keys."""
        rows = [_row(_capsule())]
        meta = _salience_metadata(rows, _now())
        expected = {"sort_applied", "capsule_count", "best_lifecycle_rank", "worst_health_rank", "worst_freshness_rank", "all_resume_adequate"}
        self.assertEqual(set(meta.keys()), expected)  # type: ignore[union-attr]

    def test_sort_applied_is_true(self) -> None:
        meta = _salience_metadata([_row(_capsule())], _now())
        self.assertTrue(meta["sort_applied"])  # type: ignore[index]

    def test_capsule_count(self) -> None:
        rows = [_row(_capsule(subject_id="a")), _row(_capsule(subject_id="b"))]
        meta = _salience_metadata(rows, _now())
        self.assertEqual(meta["capsule_count"], 2)  # type: ignore[index]

    def test_best_lifecycle_rank(self) -> None:
        """Acceptance criterion #3: best (lowest) lifecycle rank across capsules."""
        rows = [
            _row(_capsule(subject_id="a", lifecycle="suspended")),
            _row(_capsule(subject_id="b", lifecycle="active")),
        ]
        meta = _salience_metadata(rows, _now())
        self.assertEqual(meta["best_lifecycle_rank"], 0)  # type: ignore[index]

    def test_worst_health_rank(self) -> None:
        """Acceptance criterion #3: worst (highest) health rank."""
        rows = [
            _row(_capsule(subject_id="a")),
            _row(_capsule(subject_id="b", capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())})),
        ]
        meta = _salience_metadata(rows, _now())
        self.assertEqual(meta["worst_health_rank"], 1)  # type: ignore[index]

    def test_worst_freshness_rank(self) -> None:
        rows = [
            _row(_capsule(subject_id="a")),
            _row(_capsule(subject_id="b", verified_at=_iso(_now() - timedelta(seconds=200)), stale_after_seconds=100)),
        ]
        meta = _salience_metadata(rows, _now())
        self.assertGreater(meta["worst_freshness_rank"], 0)  # type: ignore[index]

    def test_all_resume_adequate_true(self) -> None:
        rows = [_row(_capsule(stance_summary=_STANCE_ADEQUATE))]
        meta = _salience_metadata(rows, _now())
        self.assertTrue(meta["all_resume_adequate"])  # type: ignore[index]

    def test_all_resume_adequate_false(self) -> None:
        """Acceptance criterion #3: one inadequate → all_resume_adequate=false."""
        rows = [
            _row(_capsule(subject_id="a", stance_summary=_STANCE_ADEQUATE)),
            _row(_capsule(subject_id="b", stance_summary=_STANCE_INADEQUATE)),
        ]
        meta = _salience_metadata(rows, _now())
        self.assertFalse(meta["all_resume_adequate"])  # type: ignore[index]


# ---------------------------------------------------------------------------
# 11) Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases(unittest.TestCase):
    """Edge-case tests for salience ranking."""

    def test_all_degraded_still_deterministic(self) -> None:
        """§6d: all signals degraded, sort still deterministic."""
        c1 = _capsule(
            subject_id="a",
            lifecycle="concluded",
            capsule_health={"status": "conflicted", "reasons": ["r"], "last_checked_at": _iso(_now())},
            stance_summary=_STANCE_INADEQUATE,
        )
        c2 = _capsule(
            subject_id="b",
            lifecycle="concluded",
            capsule_health={"status": "conflicted", "reasons": ["r"], "last_checked_at": _iso(_now())},
            stance_summary=_STANCE_INADEQUATE,
        )
        rows = [_row(c2), _row(c1)]
        sorted_rows = _salience_sort(rows, _now())
        ids = [r["selector"]["subject_id"] for r in sorted_rows]
        # Alphabetical tiebreak on subject_id.
        self.assertEqual(ids, ["a", "b"])

    def test_all_signals_identical_except_recency(self) -> None:
        older = _capsule(subject_id="x", updated_at=_iso(_now() - timedelta(hours=2)))
        newer = _capsule(subject_id="x", updated_at=_iso(_now() - timedelta(hours=1)))
        rows = [_row(older), _row(newer)]
        sorted_rows = _salience_sort(rows, _now())
        ages = [_updated_age_seconds(r["capsule"], _now()) for r in sorted_rows]
        self.assertLessEqual(ages[0], ages[1])

    def test_all_signals_identical_except_subject_id(self) -> None:
        rows = [_row(_capsule(subject_id="z")), _row(_capsule(subject_id="a"))]
        sorted_rows = _salience_sort(rows, _now())
        ids = [r["selector"]["subject_id"] for r in sorted_rows]
        self.assertEqual(ids, ["a", "z"])

    def test_missing_thread_descriptor_sorts_last(self) -> None:
        thread = _row(_capsule(subject_id="t1", lifecycle="active"))
        user = _row(_capsule(subject_kind="user", subject_id="u1", thread_descriptor=None, lifecycle=None))
        rows = [user, thread]
        sorted_rows = _salience_sort(rows, _now())
        kinds = [r["selector"]["subject_kind"] for r in sorted_rows]
        self.assertEqual(kinds, ["thread", "user"])


# ---------------------------------------------------------------------------
# 12) Integration: build_continuity_state
# ---------------------------------------------------------------------------


def _persist_capsule(root: Path, capsule: dict) -> None:
    """Write a capsule JSON to the expected file path under root."""
    from app.continuity.paths import continuity_rel_path

    rel = continuity_rel_path(capsule["subject_kind"], capsule["subject_id"])
    full = root / rel
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_text(json.dumps(capsule), encoding="utf-8")


class TestBuildContinuityStateSalience(unittest.TestCase):
    """Integration: salience ordering and metadata in build_continuity_state."""

    def _build(self, capsules: list[dict], max_tokens: int = 4000) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for c in capsules:
                _persist_capsule(root, c)
            req = ContextRetrieveRequest(
                task="test-task",
                max_tokens_estimate=max_tokens,
                continuity_max_capsules=len(capsules),
                continuity_selectors=[
                    {"subject_kind": c["subject_kind"], "subject_id": c["subject_id"]}
                    for c in capsules
                ],
            )
            return build_continuity_state(
                repo_root=root,
                auth=AllowAllAuthStub(),
                req=req,
                now=_now(),
            )

    def test_selection_order_reflects_salience(self) -> None:
        """Acceptance criterion #1: selection_order matches salience sort."""
        active = _capsule(subject_id="t1", lifecycle="active")
        suspended = _capsule(subject_id="t2", lifecycle="suspended")
        state = self._build([suspended, active])
        self.assertTrue(state["present"])
        order = state["selection_order"]
        # Active capsule should come first.
        self.assertIn("t1", order[0])
        self.assertIn("t2", order[1])

    def test_capsules_have_salience_block(self) -> None:
        """Acceptance criterion #2: each capsule has salience with rank and sort_key."""
        state = self._build([_capsule(subject_id="t1"), _capsule(subject_id="t2")])
        for capsule in state["capsules"]:
            self.assertIn("salience", capsule)
            sal = capsule["salience"]
            if sal is not None:
                self.assertIn("rank", sal)
                self.assertIn("sort_key", sal)

    def test_salience_metadata_present(self) -> None:
        """Acceptance criterion #3: salience_metadata in response."""
        state = self._build([_capsule()])
        meta = state["salience_metadata"]
        self.assertIsNotNone(meta)
        self.assertTrue(meta["sort_applied"])
        self.assertEqual(meta["capsule_count"], 1)

    def test_salience_metadata_null_when_no_capsules(self) -> None:
        """Acceptance criterion #4: salience_metadata is null when present=false."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            req = ContextRetrieveRequest(
                task="test-task",
                max_tokens_estimate=4000,
                continuity_selectors=[{"subject_kind": "thread", "subject_id": "nonexistent"}],
                continuity_mode="auto",
            )
            state = build_continuity_state(
                repo_root=root,
                auth=AllowAllAuthStub(),
                req=req,
                now=_now(),
            )
        self.assertFalse(state["present"])
        self.assertIsNone(state["salience_metadata"])

    def test_salience_ranks_are_sequential(self) -> None:
        """Ranks are 1-indexed and sequential."""
        state = self._build([
            _capsule(subject_id="t1"),
            _capsule(subject_id="t2"),
            _capsule(subject_id="t3"),
        ])
        ranks = [
            c["salience"]["rank"]
            for c in state["capsules"]
            if c.get("salience") is not None
        ]
        self.assertEqual(ranks, list(range(1, len(ranks) + 1)))

    def test_budget_too_tight_omits_capsule_entirely(self) -> None:
        """At 256 tokens the capsule cannot fit even without salience and is fully omitted."""
        state = self._build([_capsule()], max_tokens=256)
        self.assertEqual(state["capsules"], [])
        self.assertGreater(len(state["omitted_selectors"]), 0)

    def test_soft_budget_drops_salience_but_keeps_capsule(self) -> None:
        """Acceptance criterion #9: salience is a soft cost — capsule survives without it."""
        state = self._build([_capsule()], max_tokens=1175)
        capsules = state["capsules"]
        self.assertEqual(len(capsules), 1, "capsule must survive at this budget")
        self.assertIsNone(capsules[0].get("salience"))
        self.assertTrue(
            any(CONTINUITY_WARNING_SALIENCE_OMITTED in w for w in state.get("recovery_warnings", [])),
        )

    def test_sufficient_budget_includes_salience(self) -> None:
        """When budget is ample, salience block is present on the capsule."""
        state = self._build([_capsule()], max_tokens=4000)
        capsules = state["capsules"]
        self.assertEqual(len(capsules), 1)
        self.assertIsNotNone(capsules[0].get("salience"))


# ---------------------------------------------------------------------------
# 13) Integration: continuity_list_service with sort="salience"
# ---------------------------------------------------------------------------


def _noop_audit(*_args: object) -> None:
    pass


class TestContinuityListSalience(unittest.TestCase):
    """Integration: salience sorting in the list endpoint."""

    def _list(
        self,
        capsules: list[dict],
        sort: str = "salience",
        include_fallback: bool = False,
    ) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for c in capsules:
                _persist_capsule(root, c)
            req = ContinuityListRequest(sort=sort, include_fallback=include_fallback)
            return continuity_list_service(
                repo_root=root,
                auth=AllowAllAuthStub(),
                req=req,
                now=_now(),
                audit=_noop_audit,
            )

    def test_salience_sort_orders_active_rows(self) -> None:
        """Acceptance criterion #5: sort=salience orders active rows by salience."""
        active = _capsule(subject_id="t1", lifecycle="active")
        suspended = _capsule(subject_id="t2", lifecycle="suspended")
        result = self._list([suspended, active])
        ids = [r["subject_id"] for r in result["capsules"]]
        self.assertEqual(ids[0], "t1")
        self.assertEqual(ids[1], "t2")

    def test_salience_rank_assigned(self) -> None:
        """Acceptance criterion #5: each active row gets salience_rank."""
        result = self._list([_capsule(subject_id="t1"), _capsule(subject_id="t2")])
        ranks = [r.get("salience_rank") for r in result["capsules"]]
        self.assertEqual(ranks, [1, 2])

    def test_default_sort_preserves_alphabetical(self) -> None:
        """Acceptance criterion #6: sort=default preserves current order."""
        result = self._list([_capsule(subject_id="b"), _capsule(subject_id="a")], sort="default")
        ids = [r["subject_id"] for r in result["capsules"]]
        # Alphabetical by subject_kind + subject_id.
        self.assertEqual(ids, sorted(ids))

    def test_default_sort_has_no_salience_rank(self) -> None:
        result = self._list([_capsule()], sort="default")
        for row in result["capsules"]:
            self.assertNotIn("salience_rank", row)

    def test_non_active_rows_get_null_rank(self) -> None:
        """Acceptance criterion #7: fallback/archive rows get salience_rank: null."""
        # Create a capsule and its fallback.
        c = _capsule(subject_id="t1")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _persist_capsule(root, c)
            # Create a fallback file.
            from app.continuity.paths import continuity_fallback_rel_path
            fb_rel = continuity_fallback_rel_path("thread", "t1")
            fb_path = root / fb_rel
            fb_path.parent.mkdir(parents=True, exist_ok=True)
            fb_envelope = {
                "type": "continuity_fallback_snapshot",
                "schema_version": "1.0",
                "created_at": _iso(_now()),
                "source_subject_kind": "thread",
                "source_subject_id": "t1",
                "payload": c,
            }
            fb_path.write_text(json.dumps(fb_envelope), encoding="utf-8")

            req = ContinuityListRequest(sort="salience", include_fallback=True)
            result = continuity_list_service(
                repo_root=root,
                auth=AllowAllAuthStub(),
                req=req,
                now=_now(),
                audit=_noop_audit,
            )
        fallback_rows = [r for r in result["capsules"] if r.get("artifact_state") == "fallback"]
        for fb in fallback_rows:
            self.assertIsNone(fb.get("salience_rank"))

    def test_three_active_capsules_ranked(self) -> None:
        """Three active capsules with varying signals produce correct ordering and ranks."""
        c1 = _capsule(subject_id="healthy_active", lifecycle="active")
        c2 = _capsule(subject_id="degraded_active", lifecycle="active", capsule_health={"status": "degraded", "reasons": ["r"], "last_checked_at": _iso(_now())})
        c3 = _capsule(subject_id="suspended", lifecycle="suspended")
        result = self._list([c3, c2, c1])
        ids = [r["subject_id"] for r in result["capsules"]]
        ranks = [r["salience_rank"] for r in result["capsules"]]
        self.assertEqual(ids, ["healthy_active", "degraded_active", "suspended"])
        self.assertEqual(ranks, [1, 2, 3])


# ---------------------------------------------------------------------------
# 14) Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility(unittest.TestCase):
    """Ensure existing behavior is preserved."""

    def test_salience_metadata_key_always_present_in_state(self) -> None:
        """salience_metadata key is always in the response, even when off."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            req = ContextRetrieveRequest(
                task="test-task",
                max_tokens_estimate=4000,
                continuity_mode="off",
            )
            state = build_continuity_state(
                repo_root=root,
                auth=AllowAllAuthStub(),
                req=req,
                now=_now(),
            )
        self.assertIn("salience_metadata", state)
        self.assertIsNone(state["salience_metadata"])


if __name__ == "__main__":
    unittest.main()
