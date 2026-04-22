"""Tests for context retrieval search, recency, and limit behavior."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.constants import CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS
from app.context.service import _select_top_raw_scan_candidates
from app.indexer import rebuild_index
from app.main import context_retrieve, recent_list, search
from app.models import ContextRetrieveRequest, RecentRequest, SearchRequest


class _AuthStub:
    """Auth stub that permits all reads used in context retrieval tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None


class _GitManagerStub:
    """Git manager stub for retrieval tests."""

    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


def _now_iso() -> str:
    """Return a deterministic ISO timestamp for continuity fixtures."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_continuity_capsule(repo_root: Path, payload: dict) -> None:
    """Persist a continuity capsule fixture under the repo root."""
    continuity_dir = repo_root / "memory" / "continuity"
    continuity_dir.mkdir(parents=True, exist_ok=True)
    path = continuity_dir / f"{payload['subject_kind']}-{payload['subject_id']}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")


def _rich_capsule(subject_kind: str, subject_id: str, *, include_prefs: bool = False) -> dict:
    """Return a bounded but rich continuity capsule used for #227 practicality checks."""
    now = _now_iso()
    capsule = {
        "schema_version": "1.1",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "test-hook",
            "update_reason": "manual",
            "inputs": [f"memory/core/input-{idx}.md" for idx in range(6)],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.7},
        "attention_policy": {
            "early_load": [f"early-load {idx} " + "x" * 40 for idx in range(5)],
            "presence_bias_overrides": [f"presence-bias {idx} " + "y" * 40 for idx in range(4)],
        },
        "freshness": {
            "freshness_class": "situational",
            "expires_at": "2099-01-01T00:00:00Z",
            "stale_after_seconds": 2592000,
        },
        "canonical_sources": [f"docs/source-{idx}.md" for idx in range(4)],
        "metadata": {"project": "CogniRelay", "slice": "#227"},
        "continuity": {
            "top_priorities": [f"top priority {idx} " + "a" * 80 for idx in range(6)],
            "active_concerns": [f"concern {idx} " + "b" * 80 for idx in range(4)],
            "active_constraints": [f"constraint {idx} " + "c" * 80 for idx in range(6)],
            "open_loops": [f"loop {idx} " + "d" * 80 for idx in range(6)],
            "stance_summary": "Rich orientation stance " + "e" * 120,
            "drift_signals": [f"drift {idx} " + "f" * 80 for idx in range(4)],
            "working_hypotheses": [f"hypothesis {idx} " + "g" * 60 for idx in range(3)],
            "long_horizon_commitments": [f"commitment {idx} " + "h" * 60 for idx in range(3)],
            "trailing_notes": [f"note {idx} " + "i" * 60 for idx in range(2)],
            "curiosity_queue": [f"question {idx} " + "j" * 60 for idx in range(3)],
            "rationale_entries": [
                {
                    "tag": f"rationale-{idx}",
                    "kind": "decision",
                    "status": "active",
                    "summary": "summary " + "k" * 120,
                    "reasoning": "reasoning " + "l" * 180,
                    "alternatives_considered": ["alt " + "m" * 40],
                    "depends_on": ["dep " + "n" * 30],
                    "created_at": now,
                    "updated_at": now,
                }
                for idx in range(2)
            ],
            "related_documents": [
                {"path": f"docs/doc-{idx}.md", "kind": "spec", "label": "label " + "o" * 40, "relevance": "supporting"}
                for idx in range(2)
            ],
            "relationship_model": {
                "trust_level": "high",
                "preferred_style": [f"style {idx} " + "p" * 40 for idx in range(3)],
                "sensitivity_notes": [f"sensitivity {idx} " + "q" * 40 for idx in range(3)],
            },
            "retrieval_hints": {
                "must_include": [f"must-include {idx} " + "r" * 40 for idx in range(4)],
                "avoid": [f"avoid {idx} " + "s" * 40 for idx in range(3)],
                "load_next": [f"load-next {idx} " + "t" * 40 for idx in range(3)],
            },
        },
    }
    if include_prefs:
        capsule["stable_preferences"] = [
            {
                "tag": f"pref-{idx}",
                "content": "preference " + "u" * 120,
                "created_at": now,
                "updated_at": now,
                "last_confirmed_at": now,
            }
            for idx in range(6)
        ]
    return capsule


class TestContextRetrieval(unittest.TestCase):
    """Validate search ordering, filtering, and retrieval defaults."""

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

    def test_select_top_raw_scan_candidates_keeps_recent_paths_deterministically(self) -> None:
        """Raw-scan candidate selection should keep only the most recent deterministic slice."""
        candidates = [
            (-10.0, "journal/2026/2026-03-10.md", "older"),
            (-30.0, "messages/threads/thread-3.jsonl", "newest"),
            (-20.0, "messages/threads/thread-2.jsonl", "middle"),
            (-20.0, "journal/2026/2026-03-20.md", "middle-tiebreak"),
        ]

        selected = _select_top_raw_scan_candidates(candidates, limit=3)

        self.assertEqual(
            selected,
            [
                (-30.0, "messages/threads/thread-3.jsonl", "newest"),
                (-20.0, "journal/2026/2026-03-20.md", "middle-tiebreak"),
                (-20.0, "messages/threads/thread-2.jsonl", "middle"),
            ],
        )

    def test_search_recent_orders_only_matching_results(self) -> None:
        """Recent search should order matching results by recency."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)
            older_match = repo_root / "journal" / "2026" / "2026-03-09.md"
            newer_match = repo_root / "journal" / "2026" / "2026-03-11.md"
            newer_non_match = repo_root / "journal" / "2026" / "2026-03-12.md"
            older_match.write_text("---\ntype: journal_entry\n---\nSession 145 older note.", encoding="utf-8")
            newer_match.write_text("---\ntype: journal_entry\n---\nSession 145 latest note.", encoding="utf-8")
            newer_non_match.write_text("---\ntype: journal_entry\n---\nDifferent session entirely.", encoding="utf-8")

            now = datetime.now(timezone.utc)
            os.utime(newer_non_match, (now.timestamp(), now.timestamp()))
            older_dt = now - timedelta(hours=24)
            newer_dt = now - timedelta(hours=1)
            os.utime(older_match, (older_dt.timestamp(), older_dt.timestamp()))
            os.utime(newer_match, (newer_dt.timestamp(), newer_dt.timestamp()))
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = search(
                    SearchRequest(query="145", sort_by="recent", include_types=["journal_entry"], time_window_hours=48, limit=5),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["sort_by"], "recent")
            self.assertEqual(result["count"], 2)
            self.assertEqual(result["results"][0]["path"], "journal/2026/2026-03-11.md")
            self.assertEqual(result["results"][1]["path"], "journal/2026/2026-03-09.md")

    def test_search_recent_expands_candidates_before_truncating(self) -> None:
        """Recent search should expand candidates before applying final limits."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)

            for i in range(1, 131):
                path = repo_root / "journal" / "2026" / f"2026-03-{i:02d}.md"
                path.write_text(
                    f"---\ntype: journal_entry\n---\nneedle {'needle ' * 8}older item {i}.",
                    encoding="utf-8",
                )
                dt = datetime.now(timezone.utc) - timedelta(days=10 + i)
                os.utime(path, (dt.timestamp(), dt.timestamp()))

            newest = repo_root / "journal" / "2026" / "2026-03-20.md"
            newest.write_text("---\ntype: journal_entry\n---\nneedle newest item.", encoding="utf-8")
            now = datetime.now(timezone.utc)
            os.utime(newest, (now.timestamp(), now.timestamp()))
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = search(
                    SearchRequest(query="needle", sort_by="recent", include_types=["journal_entry"], limit=1),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["results"][0]["path"], "journal/2026/2026-03-20.md")

    def test_recent_list_returns_latest_files_with_time_filter(self) -> None:
        """Recent listing should respect the time window filter before limiting."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)
            recent_path = repo_root / "journal" / "2026" / "2026-03-11.md"
            old_path = repo_root / "journal" / "2026" / "2026-03-01.md"
            recent_path.write_text("---\ntype: journal_entry\n---\nLatest session.", encoding="utf-8")
            old_path.write_text("---\ntype: journal_entry\n---\nOlder session.", encoding="utf-8")

            now = datetime.now(timezone.utc)
            os.utime(recent_path, (now.timestamp(), now.timestamp()))
            old_dt = now - timedelta(hours=48)
            os.utime(old_path, (old_dt.timestamp(), old_dt.timestamp()))
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = recent_list(
                    RecentRequest(include_types=["journal_entry"], time_window_hours=24, limit=5),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["results"][0]["path"], "journal/2026/2026-03-11.md")

    def test_context_retrieve_default_limit_stays_ten(self) -> None:
        """Default context retrieval should preserve the ten-result limit."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)

            identity = repo_root / "memory" / "core" / "identity.md"
            identity.write_text("---\ntype: core_memory\n---\nAgent identity.", encoding="utf-8")

            for day in range(1, 13):
                path = repo_root / "journal" / "2026" / f"2026-03-{day:02d}.md"
                path.write_text(f"---\ntype: journal_entry\n---\nstartup session {day}.", encoding="utf-8")
                dt = datetime.now(timezone.utc) - timedelta(hours=day)
                os.utime(path, (dt.timestamp(), dt.timestamp()))

            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(
                    ContextRetrieveRequest(
                        task="startup",
                        include_types=["journal_entry"],
                        time_window_days=7,
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            bundle = result["bundle"]
            self.assertEqual(len(bundle["recent_relevant"]), 10)

    def test_context_retrieve_time_window_filters_before_final_limit(self) -> None:
        """Time-window filtering should happen before the final result truncation."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text(
                "---\ntype: core_memory\n---\nAgent identity.",
                encoding="utf-8",
            )

            for i in range(1, 131):
                path = repo_root / "journal" / "2026" / f"2026-03-{i:02d}.md"
                path.write_text(
                    f"---\ntype: journal_entry\n---\nstartup {'startup ' * 8}older item {i}.",
                    encoding="utf-8",
                )
                dt = datetime.now(timezone.utc) - timedelta(days=10 + i)
                os.utime(path, (dt.timestamp(), dt.timestamp()))

            newest = repo_root / "journal" / "2026" / "2026-03-20.md"
            newest.write_text("---\ntype: journal_entry\n---\nstartup recent item.", encoding="utf-8")
            now = datetime.now(timezone.utc)
            os.utime(newest, (now.timestamp(), now.timestamp()))
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(
                    ContextRetrieveRequest(
                        task="startup",
                        include_types=["journal_entry"],
                        time_window_days=7,
                        limit=1,
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            bundle = result["bundle"]
            self.assertEqual(len(bundle["recent_relevant"]), 1)
            self.assertEqual(bundle["recent_relevant"][0]["path"], "journal/2026/2026-03-20.md")

    def test_context_retrieve_uses_contract_default_budget_when_omitted(self) -> None:
        """Omitted max_tokens_estimate should use the contract default without hidden down-clamping."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text(
                "---\ntype: core_memory\n---\nAgent identity.",
                encoding="utf-8",
            )

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(ContextRetrieveRequest(task="startup"), auth=_AuthStub())

            self.assertTrue(result["ok"])
            bundle = result["bundle"]
            budget = bundle["continuity_state"]["budget"]
            self.assertEqual(bundle["token_budget_hint"], CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS)
            self.assertEqual(budget["requested_max_tokens_estimate"], CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS)
            self.assertEqual(budget["token_budget_hint"], CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS)
            self.assertEqual(budget["continuity_tokens_reserved"], CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS)

    def test_context_retrieve_preserves_explicit_budget_override(self) -> None:
        """Explicit max_tokens_estimate should remain the effective continuity budget."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text(
                "---\ntype: core_memory\n---\nAgent identity.",
                encoding="utf-8",
            )

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(
                    ContextRetrieveRequest(task="startup", max_tokens_estimate=2048),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            bundle = result["bundle"]
            budget = bundle["continuity_state"]["budget"]
            self.assertEqual(bundle["token_budget_hint"], 2048)
            self.assertEqual(budget["requested_max_tokens_estimate"], 2048)
            self.assertEqual(budget["token_budget_hint"], 2048)
            self.assertEqual(budget["continuity_tokens_reserved"], 2048)

    def test_context_retrieve_default_budget_supports_rich_thread_task_user_capsules(self) -> None:
        """The #227 default budget should fit rich thread/task/user capsules without trim changes."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text(
                "---\ntype: core_memory\n---\nAgent identity.",
                encoding="utf-8",
            )
            capsules = [
                _rich_capsule("thread", "thread-227"),
                _rich_capsule("task", "task-227"),
                _rich_capsule("user", "user-227", include_prefs=True),
            ]
            for capsule in capsules:
                _write_continuity_capsule(repo_root, capsule)
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(
                    ContextRetrieveRequest(
                        task="resume issue 227 work",
                        continuity_selectors=[
                            {"subject_kind": "thread", "subject_id": "thread-227"},
                            {"subject_kind": "task", "subject_id": "task-227"},
                            {"subject_kind": "user", "subject_id": "user-227"},
                        ],
                        continuity_max_capsules=3,
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            continuity_state = result["bundle"]["continuity_state"]
            self.assertEqual(result["bundle"]["token_budget_hint"], CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS)
            self.assertEqual(len(continuity_state["capsules"]), 3)
            self.assertFalse(continuity_state["trust_signals"]["completeness"]["any_trimmed"])
            self.assertEqual(continuity_state["warnings"], [])
            user_capsule = next(c for c in continuity_state["capsules"] if c["subject_kind"] == "user")
            self.assertGreater(len(user_capsule.get("stable_preferences", [])), 0)
            for capsule in continuity_state["capsules"]:
                completeness = capsule["trust_signals"]["completeness"]
                self.assertFalse(completeness["trimmed"])
                self.assertEqual(completeness["trimmed_fields"], [])

    def test_payload_reference_matches_budget_contract(self) -> None:
        """Docs should expose the retrieval budget contract and fixed trim policy."""
        payload_reference = Path(__file__).resolve().parents[1] / "docs" / "payload-reference.md"
        text = payload_reference.read_text(encoding="utf-8")

        self.assertIn("| `max_tokens_estimate` | integer | no | 256–100,000, default 12000 |", text)
        self.assertIn('"token_budget_hint": 12000', text)
        self.assertIn("This trim order is a fixed product-contract rule.", text)
        self.assertIn(
            "trimming follows the fixed two-phase order documented above: whole optional "
            "sections drop first in deterministic order, then the remaining "
            "`retrieval_hints`, `relationship_model`, `long_horizon_commitments`, "
            "`stance_summary`, `drift_signals`, and finally the core lists are "
            "trimmed only as a last resort.",
            text,
        )


if __name__ == "__main__":
    unittest.main()
