"""Tests for segment-history cold-store operation (issue #114, Phase 6)."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests.helpers import SimpleGitManagerStub

from app.segment_history.service import (
    segment_history_cold_store_service,
    segment_history_maintenance_service,
)


class _FakeSettings:
    audit_log_rollover_bytes: int = 100
    ops_run_rollover_bytes: int = 100
    message_stream_rollover_bytes: int = 100
    message_stream_max_hot_days: int = 14
    message_thread_rollover_bytes: int = 100
    message_thread_inactivity_days: int = 30
    episodic_rollover_bytes: int = 100
    segment_history_batch_limit: int = 500
    journal_cold_after_days: int = 0  # immediate eligibility for testing
    journal_retention_days: int = 365
    audit_log_cold_after_days: int = 0
    audit_log_retention_days: int = 365
    ops_run_cold_after_days: int = 0
    ops_run_retention_days: int = 365
    message_stream_cold_after_days: int = 0
    message_stream_retention_days: int = 180
    message_thread_cold_after_days: int = 0
    message_thread_retention_days: int = 365
    episodic_cold_after_days: int = 0
    episodic_retention_days: int = 180


def _setup_rolled_journal(repo: Path, gm: SimpleGitManagerStub) -> dict:
    """Create a repo with a rolled journal segment ready for cold-store."""
    year_dir = repo / "journal" / "2026"
    year_dir.mkdir(parents=True)
    (year_dir / "2026-03-19.md").write_text("entry 1\nentry 2\n")

    now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
    return segment_history_maintenance_service(
        family="journal",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=now,
    )


class TestColdStoreJournal(unittest.TestCase):
    def test_cold_stores_rolled_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            maint = _setup_rolled_journal(repo, gm)
            self.assertEqual(maint["rolled_count"], 1)

            # Cold-store with cold_after_days=0 (always eligible)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 1)
            # Response uses cold_segment_ids, not cold_stored_segment_ids
            self.assertEqual(len(result["cold_segment_ids"]), 1)
            self.assertIn("selection_count", result)

            # Hot payload should be removed
            history_dir = repo / "journal" / "history" / "2026"
            hot_payloads = list(history_dir.glob("*.md"))
            self.assertEqual(len(hot_payloads), 0)

            # Cold payload should exist
            cold_dir = history_dir / "cold"
            cold_payloads = list(cold_dir.glob("*.md.gz"))
            self.assertEqual(len(cold_payloads), 1)

            # Stub should be updated: payload_path points to cold, cold_stored_at present
            stub_dir = repo / "journal" / "history" / "2026" / "index"
            stubs = list(stub_dir.glob("*.json"))
            self.assertEqual(len(stubs), 1)
            stub = json.loads(stubs[0].read_text(encoding="utf-8"))
            self.assertIsNotNone(stub["cold_stored_at"])
            # payload_path now points to cold location
            self.assertIn("cold", stub["payload_path"])


class TestColdStoreNotEligible(unittest.TestCase):
    def test_cold_after_days_not_met(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            _setup_rolled_journal(repo, gm)

            settings = _FakeSettings()
            settings.journal_cold_after_days = 999  # Very far in the future

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 0)


class TestColdStoreIdempotent(unittest.TestCase):
    def test_already_cold_stored_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            _setup_rolled_journal(repo, gm)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            # First cold-store
            segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )
            # Second cold-store -- should find nothing new
            result = segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 0)


class TestColdStoreSegmentFilter(unittest.TestCase):
    def test_filter_by_segment_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            _setup_rolled_journal(repo, gm)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                segment_ids=["nonexistent_id"],
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 0)


if __name__ == "__main__":
    unittest.main()
