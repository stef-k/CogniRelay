"""Tests for context retrieval search, recency, and limit behavior."""

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
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


if __name__ == "__main__":
    unittest.main()
