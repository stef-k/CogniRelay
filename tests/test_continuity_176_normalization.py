"""Tests for #176 Move D: write-path normalization expansion.

Covers strip/dedup normalization for all ContinuityState string-list fields,
canonical_sources, stable_preferences, rationale_entries, and negative_decisions.
Validates both the _normalize_capsule_fields unit function and the
normalizations_applied response field from continuity_upsert_service.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import Settings
from app.continuity.service import continuity_upsert_service
from app.continuity.validation import _normalize_capsule_fields
from app.models import ContinuityCapsule, ContinuityUpsertRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


def _base_capsule_dict(*, updated_at: str | None = None) -> dict[str, Any]:
    """Return a minimal valid capsule dict."""
    now = updated_at or _now_iso()
    return {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "test-agent",
        "updated_at": now,
        "verified_at": now,
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": {
            "top_priorities": ["p1"],
            "active_concerns": ["c1"],
            "active_constraints": ["k1"],
            "open_loops": ["o1"],
            "stance_summary": "Stable stance summary for testing",
            "drift_signals": ["d1"],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.8},
    }


def _make_capsule(overrides: dict[str, Any] | None = None) -> ContinuityCapsule:
    """Build a ContinuityCapsule from the base dict with optional overrides."""
    data = _base_capsule_dict()
    if overrides:
        for key, value in overrides.items():
            if key == "continuity" and isinstance(value, dict):
                data["continuity"].update(value)
            else:
                data[key] = value
    return ContinuityCapsule.model_validate(data)


def _pref(tag: str, content: str, set_at: str = "2025-01-01T00:00:00Z") -> dict[str, Any]:
    """Build a stable_preferences dict entry."""
    return {"tag": tag, "content": content, "set_at": set_at}


def _rationale(
    tag: str,
    *,
    kind: str = "decision",
    status: str = "active",
    summary: str = "summary here",
    reasoning: str = "reasoning text",
    set_at: str = "2025-01-01T00:00:00Z",
) -> dict[str, Any]:
    """Build a rationale_entries dict entry."""
    return {
        "tag": tag,
        "kind": kind,
        "status": status,
        "summary": summary,
        "reasoning": reasoning,
        "set_at": set_at,
    }


def _neg(decision: str, rationale: str) -> dict[str, Any]:
    """Build a negative_decisions dict entry."""
    return {"decision": decision, "rationale": rationale}


# ---------------------------------------------------------------------------
# Unit tests: _normalize_capsule_fields
# ---------------------------------------------------------------------------


class TestNormalizeCapsuleFieldsCleanData(unittest.TestCase):
    """Scenario 1 & 15: clean data triggers no normalizations."""

    def test_clean_data_returns_empty_list(self) -> None:
        capsule = _make_capsule()
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(applied, [])

    def test_idempotency_on_already_clean_capsule(self) -> None:
        """Normalizing twice yields the same result and no actions on second pass."""
        capsule = _make_capsule()
        first = _normalize_capsule_fields(capsule)
        second = _normalize_capsule_fields(capsule)
        self.assertEqual(first, [])
        self.assertEqual(second, [])


class TestStringListStripWhitespace(unittest.TestCase):
    """Scenario 2: strip leading/trailing whitespace on string-list fields."""

    def test_strip_open_loops(self) -> None:
        capsule = _make_capsule({"continuity": {"open_loops": ["  item1 ", "item2  "]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.open_loops, ["item1", "item2"])
        self.assertIn("strip:continuity.open_loops", applied)

    def test_strip_top_priorities(self) -> None:
        capsule = _make_capsule({"continuity": {"top_priorities": [" p1 "]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.top_priorities, ["p1"])
        self.assertIn("strip:continuity.top_priorities", applied)

    def test_strip_session_trajectory(self) -> None:
        capsule = _make_capsule({"continuity": {"session_trajectory": ["\tstep1\t"]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.session_trajectory, ["step1"])
        self.assertIn("strip:continuity.session_trajectory", applied)

    def test_strip_trailing_notes(self) -> None:
        capsule = _make_capsule({"continuity": {"trailing_notes": [" note "]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.trailing_notes, ["note"])
        self.assertIn("strip:continuity.trailing_notes", applied)

    def test_strip_curiosity_queue(self) -> None:
        capsule = _make_capsule({"continuity": {"curiosity_queue": [" curious "]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.curiosity_queue, ["curious"])
        self.assertIn("strip:continuity.curiosity_queue", applied)


class TestStringListDropEmpty(unittest.TestCase):
    """Scenario 3: drop empty strings after stripping."""

    def test_drop_empty_after_strip(self) -> None:
        capsule = _make_capsule({"continuity": {"open_loops": ["  ", "valid", ""]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.open_loops, ["valid"])
        self.assertIn("strip:continuity.open_loops", applied)
        self.assertIn("drop_empty:continuity.open_loops", applied)

    def test_drop_empty_without_strip(self) -> None:
        capsule = _make_capsule({"continuity": {"drift_signals": ["keep", ""]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.drift_signals, ["keep"])
        self.assertIn("drop_empty:continuity.drift_signals", applied)
        # No strip action since "keep" and "" don't need stripping
        self.assertNotIn("strip:continuity.drift_signals", applied)


class TestStringListDedupFirstWins(unittest.TestCase):
    """Scenario 4: dedup preserving first occurrence."""

    def test_dedup_open_loops(self) -> None:
        capsule = _make_capsule({"continuity": {"open_loops": ["a", "b", "a"]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.open_loops, ["a", "b"])
        self.assertIn("dedup:continuity.open_loops", applied)

    def test_dedup_preserves_first_occurrence_order(self) -> None:
        capsule = _make_capsule({"continuity": {"active_concerns": ["x", "y", "z", "y", "x"]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.active_concerns, ["x", "y", "z"])
        self.assertIn("dedup:continuity.active_concerns", applied)


class TestStringListStripAndDedup(unittest.TestCase):
    """Scenario 5: strip + dedup combined."""

    def test_strip_then_dedup(self) -> None:
        capsule = _make_capsule({"continuity": {"working_hypotheses": [" hyp ", "hyp"]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.working_hypotheses, ["hyp"])
        self.assertIn("strip:continuity.working_hypotheses", applied)
        self.assertIn("dedup:continuity.working_hypotheses", applied)

    def test_strip_drop_empty_dedup_all_at_once(self) -> None:
        capsule = _make_capsule({
            "continuity": {"long_horizon_commitments": [" a ", "  ", "a", "b"]}
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.long_horizon_commitments, ["a", "b"])
        self.assertIn("strip:continuity.long_horizon_commitments", applied)
        self.assertIn("drop_empty:continuity.long_horizon_commitments", applied)
        self.assertIn("dedup:continuity.long_horizon_commitments", applied)


class TestCanonicalSourcesNormalization(unittest.TestCase):
    """Scenarios 6-8: canonical_sources strip, drop empty, dedup first-wins."""

    def test_strip_canonical_sources(self) -> None:
        capsule = _make_capsule({"canonical_sources": [" memory/a.md "]})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.canonical_sources, ["memory/a.md"])
        self.assertIn("strip:canonical_sources", applied)

    def test_drop_empty_canonical_sources(self) -> None:
        capsule = _make_capsule({"canonical_sources": ["memory/a.md", ""]})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.canonical_sources, ["memory/a.md"])
        self.assertIn("drop_empty:canonical_sources", applied)

    def test_dedup_first_wins_canonical_sources(self) -> None:
        capsule = _make_capsule({
            "canonical_sources": ["memory/a.md", "memory/b.md", "memory/a.md"]
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.canonical_sources, ["memory/a.md", "memory/b.md"])
        self.assertIn("dedup:canonical_sources", applied)

    def test_combined_canonical_sources(self) -> None:
        capsule = _make_capsule({
            "canonical_sources": [" memory/a.md ", "", "memory/a.md"]
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.canonical_sources, ["memory/a.md"])
        self.assertIn("strip:canonical_sources", applied)
        self.assertIn("drop_empty:canonical_sources", applied)
        self.assertIn("dedup:canonical_sources", applied)

    def test_empty_canonical_sources_no_action(self) -> None:
        capsule = _make_capsule()
        self.assertEqual(capsule.canonical_sources, [])
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(applied, [])


class TestStablePreferencesNormalization(unittest.TestCase):
    """Scenarios 9-10: stable_preferences strip tag/content, dedup by tag last-wins."""

    def test_strip_tag_and_content(self) -> None:
        capsule = _make_capsule({
            "stable_preferences": [_pref(" pref-1 ", " some content ")]
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.stable_preferences[0].tag, "pref-1")
        self.assertEqual(capsule.stable_preferences[0].content, "some content")
        self.assertIn("strip:stable_preferences", applied)

    def test_dedup_by_tag_last_wins(self) -> None:
        capsule = _make_capsule({
            "stable_preferences": [
                _pref("pref-1", "old content", "2025-01-01T00:00:00Z"),
                _pref("pref-2", "other pref", "2025-01-01T00:00:00Z"),
                _pref("pref-1", "new content", "2025-02-01T00:00:00Z"),
            ]
        })
        applied = _normalize_capsule_fields(capsule)
        tags = [p.tag for p in capsule.stable_preferences]
        self.assertEqual(tags, ["pref-2", "pref-1"])
        # Last-wins: the kept pref-1 should have "new content"
        pref_1 = next(p for p in capsule.stable_preferences if p.tag == "pref-1")
        self.assertEqual(pref_1.content, "new content")
        self.assertIn("dedup:stable_preferences", applied)

    def test_strip_then_dedup_stable_preferences(self) -> None:
        capsule = _make_capsule({
            "stable_preferences": [
                _pref(" pref-1 ", " old ", "2025-01-01T00:00:00Z"),
                _pref("pref-1", "new", "2025-02-01T00:00:00Z"),
            ]
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(len(capsule.stable_preferences), 1)
        self.assertEqual(capsule.stable_preferences[0].content, "new")
        self.assertIn("strip:stable_preferences", applied)
        self.assertIn("dedup:stable_preferences", applied)

    def test_no_stable_preferences_no_action(self) -> None:
        capsule = _make_capsule()
        self.assertEqual(capsule.stable_preferences, [])
        applied = _normalize_capsule_fields(capsule)
        self.assertNotIn("strip:stable_preferences", applied)
        self.assertNotIn("dedup:stable_preferences", applied)


class TestRationaleEntriesNormalization(unittest.TestCase):
    """Scenarios 11-12: rationale_entries strip tag, dedup by tag last-wins."""

    def test_strip_tag(self) -> None:
        capsule = _make_capsule({
            "continuity": {"rationale_entries": [_rationale(" re-1 ")]}
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(capsule.continuity.rationale_entries[0].tag, "re-1")
        self.assertIn("strip:rationale_entries.tag", applied)

    def test_dedup_by_tag_last_wins(self) -> None:
        capsule = _make_capsule({
            "continuity": {
                "rationale_entries": [
                    _rationale("re-1", summary="old summary"),
                    _rationale("re-2", summary="other entry"),
                    _rationale("re-1", summary="new summary"),
                ]
            }
        })
        applied = _normalize_capsule_fields(capsule)
        tags = [e.tag for e in capsule.continuity.rationale_entries]
        self.assertEqual(tags, ["re-2", "re-1"])
        re_1 = next(e for e in capsule.continuity.rationale_entries if e.tag == "re-1")
        self.assertEqual(re_1.summary, "new summary")
        self.assertIn("dedup:rationale_entries", applied)

    def test_no_rationale_entries_no_action(self) -> None:
        capsule = _make_capsule()
        self.assertEqual(capsule.continuity.rationale_entries, [])
        applied = _normalize_capsule_fields(capsule)
        self.assertNotIn("strip:rationale_entries.tag", applied)
        self.assertNotIn("dedup:rationale_entries", applied)


class TestNegativeDecisionsNormalization(unittest.TestCase):
    """Scenarios 13-14: negative_decisions strip decision/rationale, dedup by decision last-wins."""

    def test_strip_decision_and_rationale(self) -> None:
        capsule = _make_capsule({
            "continuity": {
                "negative_decisions": [_neg(" Decided not to X ", " Because Y ")]
            }
        })
        applied = _normalize_capsule_fields(capsule)
        nd = capsule.continuity.negative_decisions[0]
        self.assertEqual(nd.decision, "Decided not to X")
        self.assertEqual(nd.rationale, "Because Y")
        self.assertIn("strip:negative_decisions", applied)

    def test_dedup_by_decision_last_wins(self) -> None:
        capsule = _make_capsule({
            "continuity": {
                "negative_decisions": [
                    _neg("Decided not to X", "old rationale"),
                    _neg("Decided not to Y", "other reason"),
                    _neg("Decided not to X", "new rationale"),
                ]
            }
        })
        applied = _normalize_capsule_fields(capsule)
        decisions = [nd.decision for nd in capsule.continuity.negative_decisions]
        self.assertEqual(decisions, ["Decided not to Y", "Decided not to X"])
        kept = next(
            nd for nd in capsule.continuity.negative_decisions
            if nd.decision == "Decided not to X"
        )
        self.assertEqual(kept.rationale, "new rationale")
        self.assertIn("dedup:negative_decisions", applied)

    def test_strip_then_dedup_negative_decisions(self) -> None:
        capsule = _make_capsule({
            "continuity": {
                "negative_decisions": [
                    _neg(" Decided not to X ", "old reason"),
                    _neg("Decided not to X", "new reason"),
                ]
            }
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(len(capsule.continuity.negative_decisions), 1)
        self.assertEqual(capsule.continuity.negative_decisions[0].rationale, "new reason")
        self.assertIn("strip:negative_decisions", applied)
        self.assertIn("dedup:negative_decisions", applied)

    def test_no_negative_decisions_no_action(self) -> None:
        capsule = _make_capsule()
        self.assertEqual(capsule.continuity.negative_decisions, [])
        applied = _normalize_capsule_fields(capsule)
        self.assertNotIn("strip:negative_decisions", applied)
        self.assertNotIn("dedup:negative_decisions", applied)


class TestNormalizationsAppliedAccuracy(unittest.TestCase):
    """Scenario 16-17: normalizations_applied lists exact actions that fired."""

    def test_single_field_single_action(self) -> None:
        capsule = _make_capsule({"continuity": {"open_loops": ["a", "a"]}})
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(applied, ["dedup:continuity.open_loops"])

    def test_multiple_fields_multiple_actions(self) -> None:
        capsule = _make_capsule({
            "continuity": {
                "open_loops": [" a ", "a"],
                "drift_signals": ["x", ""],
            }
        })
        applied = _normalize_capsule_fields(capsule)
        # open_loops: strip + dedup; drift_signals: drop_empty
        self.assertIn("strip:continuity.open_loops", applied)
        self.assertIn("dedup:continuity.open_loops", applied)
        self.assertIn("drop_empty:continuity.drift_signals", applied)

    def test_all_field_types_in_one_capsule(self) -> None:
        """Multiple normalization types across string-lists, canonical_sources,
        stable_preferences, rationale_entries, and negative_decisions."""
        capsule = _make_capsule({
            "continuity": {
                "open_loops": [" a "],
                "rationale_entries": [_rationale(" re-1 ")],
                "negative_decisions": [_neg(" dec ", " rat ")],
            },
            "canonical_sources": [" memory/a.md "],
            "stable_preferences": [_pref(" p1 ", " c1 ")],
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertIn("strip:continuity.open_loops", applied)
        self.assertIn("strip:canonical_sources", applied)
        self.assertIn("strip:stable_preferences", applied)
        self.assertIn("strip:rationale_entries.tag", applied)
        self.assertIn("strip:negative_decisions", applied)

    def test_no_false_positives_on_clean_data(self) -> None:
        """Already-clean data must produce an empty normalizations list."""
        capsule = _make_capsule({
            "continuity": {
                "open_loops": ["a", "b"],
                "rationale_entries": [_rationale("re-1")],
                "negative_decisions": [_neg("dec", "rat")],
            },
            "canonical_sources": ["memory/a.md"],
            "stable_preferences": [_pref("p1", "content")],
        })
        applied = _normalize_capsule_fields(capsule)
        self.assertEqual(applied, [])


class TestAllStringListFieldsCovered(unittest.TestCase):
    """Verify normalization touches every declared string-list field."""

    _ALL_FIELDS = (
        "top_priorities",
        "active_concerns",
        "active_constraints",
        "open_loops",
        "drift_signals",
        "working_hypotheses",
        "long_horizon_commitments",
        "session_trajectory",
        "trailing_notes",
        "curiosity_queue",
    )

    def test_strip_fires_for_each_field(self) -> None:
        for field in self._ALL_FIELDS:
            with self.subTest(field=field):
                capsule = _make_capsule({"continuity": {field: [" val "]}})
                applied = _normalize_capsule_fields(capsule)
                self.assertIn(f"strip:continuity.{field}", applied)
                self.assertEqual(getattr(capsule.continuity, field), ["val"])

    def test_dedup_fires_for_each_field(self) -> None:
        for field in self._ALL_FIELDS:
            with self.subTest(field=field):
                capsule = _make_capsule({"continuity": {field: ["dup", "dup"]}})
                applied = _normalize_capsule_fields(capsule)
                self.assertIn(f"dedup:continuity.{field}", applied)
                self.assertEqual(getattr(capsule.continuity, field), ["dup"])


# ---------------------------------------------------------------------------
# Integration tests: continuity_upsert_service
# ---------------------------------------------------------------------------


def _do_upsert(repo_root: Path, capsule_dict: dict[str, Any]) -> dict[str, Any]:
    """Call continuity_upsert_service with the given capsule dict."""
    (repo_root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
    (repo_root / "memory" / "continuity" / "fallback").mkdir(parents=True, exist_ok=True)
    gm = SimpleGitManagerStub(repo_root)
    req = ContinuityUpsertRequest(
        subject_kind=capsule_dict["subject_kind"],
        subject_id=capsule_dict["subject_id"],
        capsule=capsule_dict,
    )
    return continuity_upsert_service(
        repo_root=repo_root,
        gm=gm,
        auth=AllowAllAuthStub(),
        req=req,
        audit=lambda *_args: None,
    )


class TestUpsertNormalizationsAppliedField(unittest.TestCase):
    """Scenario 18: normalizations_applied appears in upsert response."""

    def test_clean_upsert_returns_empty_normalizations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = _do_upsert(Path(td), _base_capsule_dict())
        self.assertTrue(result["ok"])
        self.assertIn("normalizations_applied", result)
        self.assertEqual(result["normalizations_applied"], [])

    def test_upsert_returns_strip_normalization(self) -> None:
        caps = _base_capsule_dict()
        caps["continuity"]["open_loops"] = [" spaced "]
        with tempfile.TemporaryDirectory() as td:
            result = _do_upsert(Path(td), caps)
        self.assertTrue(result["ok"])
        self.assertIn("strip:continuity.open_loops", result["normalizations_applied"])

    def test_upsert_returns_dedup_normalization(self) -> None:
        caps = _base_capsule_dict()
        caps["continuity"]["open_loops"] = ["dup", "dup"]
        with tempfile.TemporaryDirectory() as td:
            result = _do_upsert(Path(td), caps)
        self.assertTrue(result["ok"])
        self.assertIn("dedup:continuity.open_loops", result["normalizations_applied"])

    def test_upsert_persists_normalized_data(self) -> None:
        """After upsert, the on-disk capsule reflects normalized values."""
        caps = _base_capsule_dict()
        caps["continuity"]["open_loops"] = [" a ", "a", "b"]
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            result = _do_upsert(repo, caps)
            self.assertTrue(result["ok"])
            # Read persisted capsule
            stored_path = repo / "memory" / "continuity" / "user-test-agent.json"
            stored = json.loads(stored_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["continuity"]["open_loops"], ["a", "b"])

    def test_upsert_multiple_normalizations_reported(self) -> None:
        caps = _base_capsule_dict()
        caps["continuity"]["open_loops"] = [" x ", "x"]
        caps["continuity"]["drift_signals"] = ["d", ""]
        with tempfile.TemporaryDirectory() as td:
            result = _do_upsert(Path(td), caps)
        applied = result["normalizations_applied"]
        self.assertIn("strip:continuity.open_loops", applied)
        self.assertIn("dedup:continuity.open_loops", applied)
        self.assertIn("drop_empty:continuity.drift_signals", applied)

    def test_upsert_idempotent_second_write_no_normalizations(self) -> None:
        """Upserting already-normalized data returns empty normalizations."""
        caps = _base_capsule_dict()
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            result1 = _do_upsert(repo, caps)
            self.assertEqual(result1["normalizations_applied"], [])
            # Upsert again with same clean data
            result2 = _do_upsert(repo, caps)
            self.assertEqual(result2["normalizations_applied"], [])


if __name__ == "__main__":
    unittest.main()
