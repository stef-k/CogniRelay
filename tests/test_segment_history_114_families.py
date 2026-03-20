"""Tests for segment-history family definitions (issue #114, Phase 4)."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.segment_history.families import (
    FAMILIES,
    check_rollover_eligible,
    discover_active_sources,
    is_journal_day_rollover_eligible,
    is_size_rollover_eligible,
)


class _FakeSettings:
    """Minimal settings stub for rollover tests."""

    audit_log_rollover_bytes: int = 1_048_576
    ops_run_rollover_bytes: int = 1_048_576
    message_stream_rollover_bytes: int = 1_048_576
    message_stream_max_hot_days: int = 14
    message_thread_rollover_bytes: int = 2_097_152
    message_thread_inactivity_days: int = 30
    episodic_rollover_bytes: int = 1_048_576


class TestFamilyRegistry(unittest.TestCase):
    def test_all_six_families_registered(self) -> None:
        expected = {"journal", "api_audit", "ops_runs", "message_stream", "message_thread", "episodic"}
        self.assertEqual(set(FAMILIES.keys()), expected)

    def test_journal_no_size_rollover(self) -> None:
        self.assertFalse(FAMILIES["journal"].has_size_rollover)

    def test_message_thread_no_day_boundary(self) -> None:
        self.assertFalse(FAMILIES["message_thread"].has_day_boundary_rollover)

    def test_message_stream_no_day_boundary(self) -> None:
        self.assertFalse(FAMILIES["message_stream"].has_day_boundary_rollover)

    def test_message_stream_four_source_dirs(self) -> None:
        self.assertEqual(len(FAMILIES["message_stream"].source_dirs), 4)

    def test_stub_dirs_use_index(self) -> None:
        for name, config in FAMILIES.items():
            if name in ("message_stream", "journal"):
                # message_stream uses per-kind routing; stub_dir is a base path.
                # journal uses per-year stub dirs resolved at runtime.
                continue
            self.assertTrue(
                config.stub_dir.endswith("/index"),
                f"{name} stub_dir should end with /index, got {config.stub_dir}",
            )


class TestDiscoverActiveSources(unittest.TestCase):
    def test_discovers_jsonl_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            (logs / "api_audit.jsonl").write_text('{"a":1}\n')
            (logs / "other.txt").write_text("not jsonl")
            sources = discover_active_sources("api_audit", repo)
            self.assertEqual(len(sources), 1)
            self.assertEqual(sources[0].name, "api_audit.jsonl")

    def test_excludes_history_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            (logs / "api_audit.jsonl").write_text('{"a":1}\n')
            # History files should be excluded
            history = logs / "history" / "api_audit"
            history.mkdir(parents=True)
            (history / "old.jsonl").write_text('{"b":2}\n')
            sources = discover_active_sources("api_audit", repo)
            self.assertEqual(len(sources), 1)

    def test_journal_discovers_day_buckets(self) -> None:
        """Journal uses .md files in journal/<year>/ subdirectories."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            (year_dir / "2026-03-19.md").write_text("entry\n")
            (year_dir / "2026-03-20.md").write_text("entry\n")
            sources = discover_active_sources("journal", repo)
            self.assertEqual(len(sources), 2)

    def test_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            sources = discover_active_sources("api_audit", Path(td))
            self.assertEqual(sources, [])

    def test_message_stream_multi_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            for subdir in ["inbox", "outbox", "acks", "relay"]:
                d = repo / "messages" / subdir
                d.mkdir(parents=True)
                (d / f"peer-{subdir}.jsonl").write_text('{"x":1}\n')
            sources = discover_active_sources("message_stream", repo)
            self.assertEqual(len(sources), 4)


class TestJournalDayRollover(unittest.TestCase):
    def test_past_day_eligible(self) -> None:
        """Journal day-bucket files now use .md extension."""
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "2026-03-19.md"
            f.write_text("data")
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            self.assertTrue(is_journal_day_rollover_eligible(f, now))

    def test_today_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "2026-03-20.md"
            f.write_text("data")
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            self.assertFalse(is_journal_day_rollover_eligible(f, now))

    def test_non_day_bucket_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "random.md"
            f.write_text("data")
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            self.assertFalse(is_journal_day_rollover_eligible(f, now))


class TestSizeRollover(unittest.TestCase):
    def test_under_threshold_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "test.jsonl"
            f.write_text("small")
            settings = _FakeSettings()
            self.assertFalse(is_size_rollover_eligible(f, "api_audit", settings))

    def test_over_threshold_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "test.jsonl"
            f.write_bytes(b"x" * 1_048_576)
            settings = _FakeSettings()
            self.assertTrue(is_size_rollover_eligible(f, "api_audit", settings))

    def test_journal_never_size_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "2026-03-19.md"
            f.write_bytes(b"x" * 10_000_000)
            settings = _FakeSettings()
            self.assertFalse(is_size_rollover_eligible(f, "journal", settings))


class TestCheckRolloverEligible(unittest.TestCase):
    def test_journal_uses_day_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "2026-03-19.md"
            f.write_text("data")
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()
            self.assertTrue(check_rollover_eligible(f, "journal", settings, now))

    def test_api_audit_size_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "api_audit.jsonl"
            f.write_bytes(b"x" * 1_048_576)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()
            self.assertTrue(check_rollover_eligible(f, "api_audit", settings, now))


class TestSummaryBuilders(unittest.TestCase):
    def test_journal_summary(self) -> None:
        summary = FAMILIES["journal"].build_summary("entry 1\nentry 2\n")
        self.assertEqual(summary["line_count"], 2)
        self.assertIn("first_nonempty_line_preview", summary)

    def test_api_audit_summary(self) -> None:
        content = '{"ts":"2026-03-20","event":"write"}\n{"ts":"2026-03-21","event":"read"}\n'
        summary = FAMILIES["api_audit"].build_summary(content)
        self.assertEqual(summary["first_event_at"], "2026-03-20")
        self.assertEqual(summary["last_event_at"], "2026-03-21")
        self.assertIn("event_name_sample", summary)

    def test_ops_runs_summary(self) -> None:
        content = '{"ts":"t1","finished_at":"f1","job_id":"backup"}\n'
        summary = FAMILIES["ops_runs"].build_summary(content)
        self.assertIn("job_id_counts", summary)

    def test_episodic_summary(self) -> None:
        content = '{"at":"t1","subject_kind":"observation"}\n{"at":"t2","subject_kind":"reflection"}\n'
        summary = FAMILIES["episodic"].build_summary(content)
        self.assertIn("subject_kind_counts", summary)
        self.assertEqual(summary["first_event_at"], "t1")
        self.assertEqual(summary["last_event_at"], "t2")


if __name__ == "__main__":
    unittest.main()
