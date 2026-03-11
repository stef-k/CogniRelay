import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.indexer import rebuild_index
from app.main import context_retrieve, search
from app.models import ContextRetrieveRequest, SearchRequest


class _AuthStub:
    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def latest_commit(self) -> str:
        return "test-sha"


class TestContextRetrieval(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_search_recent_returns_latest_files_with_time_filter(self) -> None:
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
                result = search(
                    SearchRequest(query="", sort_by="recent", include_types=["journal_entry"], time_window_hours=24, limit=5),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["sort_by"], "recent")
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["results"][0]["path"], "journal/2026/2026-03-11.md")

    def test_context_retrieve_recent_prefers_latest_entries_over_keyword_score(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)

            identity = repo_root / "memory" / "core" / "identity.md"
            identity.write_text("---\ntype: core_memory\n---\nAgent identity.", encoding="utf-8")

            older_path = repo_root / "journal" / "2026" / "2026-03-09.md"
            newer_path = repo_root / "journal" / "2026" / "2026-03-11.md"
            older_path.write_text("---\ntype: journal_entry\n---\nSession 140 startup startup startup.", encoding="utf-8")
            newer_path.write_text("---\ntype: journal_entry\n---\nSession 145 handoff state.", encoding="utf-8")

            now = datetime.now(timezone.utc)
            newer_dt = now - timedelta(hours=2)
            older_dt = now - timedelta(days=3)
            os.utime(newer_path, (newer_dt.timestamp(), newer_dt.timestamp()))
            os.utime(older_path, (older_dt.timestamp(), older_dt.timestamp()))
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                result = context_retrieve(
                    ContextRetrieveRequest(
                        task="startup",
                        include_types=["journal_entry"],
                        sort_by="recent",
                        time_window_days=7,
                        limit=5,
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(result["ok"])
            bundle = result["bundle"]
            self.assertEqual(bundle["sort_by"], "recent")
            self.assertEqual(bundle["recent_relevant"][0]["path"], "journal/2026/2026-03-11.md")


if __name__ == "__main__":
    unittest.main()
