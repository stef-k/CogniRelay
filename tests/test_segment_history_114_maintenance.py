"""Tests for segment-history maintenance operation (issue #114, Phase 5)."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests.helpers import SimpleGitManagerStub

from app.segment_history.service import segment_history_maintenance_service


class _FakeSettings:
    audit_log_rollover_bytes: int = 100  # low for testing
    ops_run_rollover_bytes: int = 100
    message_stream_rollover_bytes: int = 100
    message_stream_max_hot_days: int = 14
    message_thread_rollover_bytes: int = 100
    message_thread_inactivity_days: int = 30
    episodic_rollover_bytes: int = 100
    segment_history_batch_limit: int = 500
    journal_cold_after_days: int = 30
    journal_retention_days: int = 365


class TestMaintenanceJournal(unittest.TestCase):
    def test_rolls_past_day_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Journal uses journal/<year>/<date>.md
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            (year_dir / "2026-03-19.md").write_text("entry 1\nentry 2\n")
            (year_dir / "2026-03-20.md").write_text("today entry\n")

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rolled_count"], 1)
            self.assertEqual(len(result["rolled_segment_ids"]), 1)
            self.assertTrue(result["rolled_segment_ids"][0].startswith("journal__"))
            # Source file should be removed for journal
            self.assertFalse((year_dir / "2026-03-19.md").exists())
            # Today's file should be untouched
            self.assertTrue((year_dir / "2026-03-20.md").exists())

    def test_nothing_to_roll(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            (year_dir / "2026-03-20.md").write_text("today\n")

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rolled_count"], 0)
            self.assertEqual(result["rolled_segment_ids"], [])


class TestMaintenanceApiAudit(unittest.TestCase):
    def test_rolls_oversized_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            # Create audit log exceeding 100 bytes threshold
            (logs / "api_audit.jsonl").write_text('{"ts":"2026-03-20","event":"write","peer_id":"p1"}\n' * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rolled_count"], 1)
            # Active file should be replaced with carry-forward (empty or partial)
            active = (logs / "api_audit.jsonl").read_text(encoding="utf-8")
            self.assertEqual(active.strip(), "")


class TestMaintenanceBatchLimit(unittest.TestCase):
    def test_batch_limit_reached(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            # Create 3 past-day files using .md extension
            for day in range(17, 20):
                (year_dir / f"2026-03-{day:02d}.md").write_text(f"entry {day}\n")

            settings = _FakeSettings()
            settings.segment_history_batch_limit = 2  # Only process 2

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rolled_count"], 2)
            self.assertTrue(result["batch_limit_reached"])


class TestMaintenanceEmptyDir(unittest.TestCase):
    def test_empty_family_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rolled_count"], 0)


class TestMaintenanceStubCreation(unittest.TestCase):
    def test_stubs_written_on_roll(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            (year_dir / "2026-03-19.md").write_text("entry\n")

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            gm = SimpleGitManagerStub(repo)

            segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            # Check stub was created in journal/history/<year>/index/
            stub_dir = repo / "journal" / "history" / "2026" / "index"
            self.assertTrue(stub_dir.is_dir())
            stubs = list(stub_dir.glob("*.json"))
            self.assertEqual(len(stubs), 1)

    def test_warnings_are_structured(self) -> None:
        """Warnings must be JSON objects with code, detail, path, segment_id."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            # Even with no warnings, verify the list type
            self.assertIsInstance(result["warnings"], list)
            for w in result["warnings"]:
                self.assertIsInstance(w, dict)
                self.assertIn("code", w)
                self.assertIn("detail", w)


class TestMaintenanceOpsRuns(unittest.TestCase):
    """End-to-end maintenance test for ops_runs family (H12)."""

    def test_rolls_oversized_ops_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            # ops_runs uses logs/ops_runs.jsonl
            logs = repo / "logs"
            logs.mkdir(parents=True)
            content = '{"started_at":"2026-03-19T12:00:00Z","finished_at":"2026-03-19T12:01:00Z","job_id":"backup"}\n'
            (logs / "ops_runs.jsonl").write_text(content * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_maintenance_service(
                family="ops_runs",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertGreater(result["rolled_count"], 0)
            # Verify payload and stub created in correct dirs
            history_dir = repo / "logs" / "history" / "ops_runs"
            self.assertTrue(history_dir.is_dir())
            payloads = list(history_dir.glob("*.jsonl"))
            self.assertGreater(len(payloads), 0)
            stub_dir = history_dir / "index"
            self.assertTrue(stub_dir.is_dir())


class TestMaintenanceMessageStream(unittest.TestCase):
    """End-to-end maintenance test for message_stream family (H12)."""

    def test_rolls_oversized_message_stream(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            # message_stream uses messages/{inbox,outbox,acks,relay}/<file>.jsonl
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            content = '{"sent_at":"2026-03-19T12:00:00Z","id":"msg1","thread_id":"t1"}\n'
            (inbox / "alice.jsonl").write_text(content * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_maintenance_service(
                family="message_stream",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertGreater(result["rolled_count"], 0)
            # Verify per-kind history dir routing
            history_dir = repo / "messages" / "history" / "inbox"
            self.assertTrue(history_dir.is_dir())


class TestMaintenanceMessageThread(unittest.TestCase):
    """End-to-end maintenance test for message_thread family (H12)."""

    def test_rolls_oversized_message_thread(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            threads = repo / "messages" / "threads"
            threads.mkdir(parents=True)
            content = '{"sent_at":"2026-03-19T12:00:00Z","from":"a","to":"b"}\n'
            (threads / "t1.jsonl").write_text(content * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_maintenance_service(
                family="message_thread",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertGreater(result["rolled_count"], 0)
            history_dir = repo / "messages" / "history" / "threads"
            self.assertTrue(history_dir.is_dir())


class TestMaintenanceEpisodic(unittest.TestCase):
    """End-to-end maintenance test for episodic family (H12)."""

    def test_rolls_oversized_episodic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            episodic = repo / "memory" / "episodic"
            episodic.mkdir(parents=True)
            content = '{"at":"2026-03-19T12:00:00Z","subject_kind":"observation"}\n'
            (episodic / "observations.jsonl").write_text(content * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_maintenance_service(
                family="episodic",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            self.assertGreater(result["rolled_count"], 0)
            history_dir = repo / "memory" / "episodic" / "history"
            self.assertTrue(history_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
