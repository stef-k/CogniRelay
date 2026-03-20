"""Tests for segment-history cold rehydrate operation (issue #114, Phase 7)."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.responses import JSONResponse

from tests.helpers import SimpleGitManagerStub

from app.segment_history.service import (
    segment_history_cold_rehydrate_service,
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
    journal_cold_after_days: int = 0
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


def _setup_cold_journal(repo: Path, gm: SimpleGitManagerStub) -> str:
    """Create a cold-stored journal segment and return its segment_id."""
    year_dir = repo / "journal" / "2026"
    year_dir.mkdir(parents=True)
    (year_dir / "2026-03-19.md").write_text("entry 1\nentry 2\n")

    now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
    segment_history_maintenance_service(
        family="journal",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=now,
    )
    cold = segment_history_cold_store_service(
        family="journal",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=now,
    )
    return cold["cold_segment_ids"][0]


class TestRehydrateJournal(unittest.TestCase):
    def test_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            seg_id = _setup_cold_journal(repo, gm)

            result = segment_history_cold_rehydrate_service(
                family="journal",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )

            self.assertNotIsInstance(result, JSONResponse)
            self.assertTrue(result["ok"])
            self.assertEqual(result["segment_id"], seg_id)
            # Response includes full rehydrate info
            self.assertIn("rehydrated_payload_path", result)
            self.assertIn("stub_path", result)
            self.assertIn("cold_payload_path", result)
            self.assertIn("removed_cold_payload_path", result)
            self.assertIn("mutated_stub_path", result)

            # Hot payload should exist
            hot = repo / result["rehydrated_payload_path"]
            self.assertTrue(hot.is_file())
            content = hot.read_text(encoding="utf-8")
            self.assertIn("entry 1", content)

            # Stub should be updated (cold_stored_at absent)
            stub_path = repo / result["stub_path"]
            stub = json.loads(stub_path.read_text(encoding="utf-8"))
            self.assertNotIn("cold_stored_at", stub)


class TestRehydrateErrors(unittest.TestCase):
    def test_stub_not_found(self) -> None:
        """Rehydrate returns structured error envelope, not HTTPException."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_cold_rehydrate_service(
                family="journal",
                segment_id="journal__20260320T120000Z__0001",
                repo_root=repo,
                gm=gm,
            )

            self.assertIsInstance(result, JSONResponse)
            self.assertEqual(result.status_code, 404)
            body = result.body
            parsed = json.loads(body)
            self.assertFalse(parsed["ok"])
            self.assertEqual(parsed["error"]["code"], "segment_history_stub_not_found")

    def test_not_cold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Set up a rolled but not cold-stored segment
            year_dir = repo / "journal" / "2026"
            year_dir.mkdir(parents=True)
            (year_dir / "2026-03-19.md").write_text("entry\n")

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            maint = segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )
            seg_id = maint["rolled_segment_ids"][0]

            result = segment_history_cold_rehydrate_service(
                family="journal",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )

            self.assertIsInstance(result, JSONResponse)
            self.assertEqual(result.status_code, 409)
            parsed = json.loads(result.body)
            self.assertEqual(parsed["error"]["code"], "segment_history_not_cold")

    def test_invalid_segment_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            result = segment_history_cold_rehydrate_service(
                family="journal",
                segment_id="invalid-id",
                repo_root=repo,
                gm=gm,
            )

            self.assertIsInstance(result, JSONResponse)
            self.assertEqual(result.status_code, 400)
            parsed = json.loads(result.body)
            self.assertFalse(parsed["ok"])
            self.assertEqual(parsed["error"]["code"], "segment_history_invalid_segment_id")

    def test_rehydrate_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            seg_id = _setup_cold_journal(repo, gm)

            # First rehydrate
            segment_history_cold_rehydrate_service(
                family="journal",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )

            # Re-cold-store for a clean second test
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            segment_history_cold_store_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            # Second rehydrate should succeed (no conflict since cold-store removed hot)
            result = segment_history_cold_rehydrate_service(
                family="journal",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )
            self.assertNotIsInstance(result, JSONResponse)
            self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
