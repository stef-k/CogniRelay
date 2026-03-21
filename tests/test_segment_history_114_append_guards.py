"""Tests for segment-history append guards: family path lookup and journal day rejection."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.segment_history.append import (
    _family_for_path,
    _reject_non_current_day_journal,
)


class TestFamilyForPath(unittest.TestCase):
    """Tests for _family_for_path reverse lookup."""

    def test_journal_path(self) -> None:
        self.assertEqual(_family_for_path("journal/2026/2026-03-20.md"), "journal")

    def test_api_audit_path(self) -> None:
        self.assertEqual(_family_for_path("logs/api_audit.jsonl"), "api_audit")

    def test_ops_runs_path(self) -> None:
        self.assertEqual(_family_for_path("logs/ops_runs.jsonl"), "ops_runs")

    def test_message_inbox_path(self) -> None:
        self.assertEqual(_family_for_path("messages/inbox/alice.jsonl"), "message_stream")

    def test_message_outbox_path(self) -> None:
        self.assertEqual(_family_for_path("messages/outbox/bob.jsonl"), "message_stream")

    def test_message_acks_path(self) -> None:
        self.assertEqual(_family_for_path("messages/acks/alice.jsonl"), "message_stream")

    def test_message_relay_path(self) -> None:
        self.assertEqual(_family_for_path("messages/relay/r1.jsonl"), "message_stream")

    def test_message_thread_path(self) -> None:
        self.assertEqual(_family_for_path("messages/threads/t1.jsonl"), "message_thread")

    def test_episodic_path(self) -> None:
        self.assertEqual(_family_for_path("memory/episodic/observations.jsonl"), "episodic")

    def test_history_path_excluded(self) -> None:
        self.assertIsNone(_family_for_path("logs/history/api_audit/segment.jsonl"))

    def test_history_path_excluded_journal(self) -> None:
        self.assertIsNone(_family_for_path("journal/history/2026/segment.md"))

    def test_unknown_path_returns_none(self) -> None:
        self.assertIsNone(_family_for_path("tasks/todo.jsonl"))

    def test_empty_path_returns_none(self) -> None:
        self.assertIsNone(_family_for_path(""))


class TestRejectNonCurrentDayJournal(unittest.TestCase):
    """Tests for _reject_non_current_day_journal guard."""

    def test_current_day_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            path = Path(td) / f"{today}.md"
            path.touch()
            # Should not raise
            _reject_non_current_day_journal(path)

    def test_past_day_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "2020-01-01.md"
            path.touch()
            from app.audit import WriteTimeRolloverError

            with self.assertRaises(WriteTimeRolloverError) as ctx:
                _reject_non_current_day_journal(path)
            self.assertIn("segment_history_journal_non_current_day", ctx.exception.code)

    def test_non_day_pattern_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "readme.md"
            path.touch()
            # Non-day-bucket files should pass through
            _reject_non_current_day_journal(path)


if __name__ == "__main__":
    unittest.main()
