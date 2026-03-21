"""Tests for segment-history cold-store and rehydrate across non-journal families (issue #114)."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

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


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
_NOW = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)


def _setup_rolled_api_audit(repo: Path, gm: SimpleGitManagerStub) -> dict:
    """Create a rolled api_audit segment and return the maintenance result."""
    logs = repo / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "api_audit.jsonl").write_text('{"ts":"2026-03-19T10:00:00Z","event":"write","peer_id":"p1"}\n' * 5)
    return segment_history_maintenance_service(
        family="api_audit",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=_NOW,
    )


def _setup_rolled_message_stream_inbox(repo: Path, gm: SimpleGitManagerStub) -> dict:
    """Create a rolled message_stream inbox segment and return the maintenance result."""
    inbox = repo / "messages" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    (inbox / "alice.jsonl").write_text('{"id":"m1","sent_at":"2026-03-19T08:00:00Z","from":"bob","to":"alice","thread_id":"t1"}\n' * 5)
    return segment_history_maintenance_service(
        family="message_stream",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=_NOW,
    )


def _setup_rolled_ops_runs(repo: Path, gm: SimpleGitManagerStub) -> dict:
    """Create a rolled ops_runs segment and return the maintenance result."""
    logs = repo / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "ops_runs.jsonl").write_text('{"job_id":"j1","started_at":"2026-03-19T09:00:00Z","finished_at":"2026-03-19T09:05:00Z"}\n' * 5)
    return segment_history_maintenance_service(
        family="ops_runs",
        repo_root=repo,
        settings=_FakeSettings(),
        gm=gm,
        now=_NOW,
    )


# ---------------------------------------------------------------------------
# Cold-store tests
# ---------------------------------------------------------------------------
class TestColdStoreApiAudit(unittest.TestCase):
    """Cold-store for api_audit family."""

    def test_cold_stores_rolled_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            maint = _setup_rolled_api_audit(repo, gm)
            self.assertEqual(maint["rolled_count"], 1)

            result = segment_history_cold_store_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=_NOW,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 1)
            self.assertEqual(len(result["cold_segment_ids"]), 1)

            # Hot payload should be removed
            history_dir = repo / "logs" / "history" / "api_audit"
            hot_payloads = list(history_dir.glob("*.jsonl"))
            self.assertEqual(len(hot_payloads), 0)

            # Cold payload should exist
            cold_dir = history_dir / "cold"
            cold_payloads = list(cold_dir.glob("*.jsonl.gz"))
            self.assertEqual(len(cold_payloads), 1)

            # Stub should be updated
            stub_dir = history_dir / "index"
            stubs = list(stub_dir.glob("*.json"))
            self.assertEqual(len(stubs), 1)
            stub = json.loads(stubs[0].read_text(encoding="utf-8"))
            self.assertIsNotNone(stub["cold_stored_at"])
            self.assertIn("cold", stub["payload_path"])


class TestColdStoreMessageStreamInbox(unittest.TestCase):
    """Cold-store for message_stream inbox family."""

    def test_cold_stores_rolled_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            maint = _setup_rolled_message_stream_inbox(repo, gm)
            self.assertEqual(maint["rolled_count"], 1)

            result = segment_history_cold_store_service(
                family="message_stream",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=_NOW,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 1)
            self.assertEqual(len(result["cold_segment_ids"]), 1)

            # Hot payload should be removed
            history_dir = repo / "messages" / "history" / "inbox"
            hot_payloads = list(history_dir.glob("*.jsonl"))
            self.assertEqual(len(hot_payloads), 0)

            # Cold payload should exist
            cold_dir = history_dir / "cold"
            cold_payloads = list(cold_dir.glob("*.jsonl.gz"))
            self.assertEqual(len(cold_payloads), 1)

            # Stub should be updated
            stub_dir = history_dir / "index"
            stubs = list(stub_dir.glob("*.json"))
            self.assertEqual(len(stubs), 1)
            stub = json.loads(stubs[0].read_text(encoding="utf-8"))
            self.assertIsNotNone(stub["cold_stored_at"])
            self.assertIn("cold", stub["payload_path"])


class TestColdStoreOpsRuns(unittest.TestCase):
    """Cold-store for ops_runs family."""

    def test_cold_stores_rolled_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            maint = _setup_rolled_ops_runs(repo, gm)
            self.assertEqual(maint["rolled_count"], 1)

            result = segment_history_cold_store_service(
                family="ops_runs",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=_NOW,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_stored_count"], 1)
            self.assertEqual(len(result["cold_segment_ids"]), 1)

            # Hot payload should be removed
            history_dir = repo / "logs" / "history" / "ops_runs"
            hot_payloads = list(history_dir.glob("*.jsonl"))
            self.assertEqual(len(hot_payloads), 0)

            # Cold payload should exist
            cold_dir = history_dir / "cold"
            cold_payloads = list(cold_dir.glob("*.jsonl.gz"))
            self.assertEqual(len(cold_payloads), 1)


# ---------------------------------------------------------------------------
# Rehydrate tests
# ---------------------------------------------------------------------------
class TestRehydrateApiAudit(unittest.TestCase):
    """Rehydrate round-trip for api_audit family."""

    def test_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Roll and cold-store
            _setup_rolled_api_audit(repo, gm)
            cold = segment_history_cold_store_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=_NOW,
            )
            seg_id = cold["cold_segment_ids"][0]

            # Rehydrate
            result = segment_history_cold_rehydrate_service(
                family="api_audit",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["segment_id"], seg_id)
            self.assertIn("rehydrated_payload_path", result)

            # Hot payload should exist and contain original content
            hot = repo / result["rehydrated_payload_path"]
            self.assertTrue(hot.is_file())
            content = hot.read_text(encoding="utf-8")
            self.assertIn("write", content)
            self.assertIn("p1", content)

            # Stub should no longer have cold_stored_at
            stub_path = repo / result["stub_path"]
            stub = json.loads(stub_path.read_text(encoding="utf-8"))
            self.assertNotIn("cold_stored_at", stub)


class TestRehydrateMessageStreamInbox(unittest.TestCase):
    """Rehydrate round-trip for message_stream inbox family."""

    def test_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Roll and cold-store
            _setup_rolled_message_stream_inbox(repo, gm)
            cold = segment_history_cold_store_service(
                family="message_stream",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=_NOW,
            )
            seg_id = cold["cold_segment_ids"][0]

            # Rehydrate
            result = segment_history_cold_rehydrate_service(
                family="message_stream",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["segment_id"], seg_id)
            self.assertIn("rehydrated_payload_path", result)

            # Hot payload should exist and contain original content
            hot = repo / result["rehydrated_payload_path"]
            self.assertTrue(hot.is_file())
            content = hot.read_text(encoding="utf-8")
            self.assertIn("alice", content)
            self.assertIn("bob", content)

            # Stub should no longer have cold_stored_at
            stub_path = repo / result["stub_path"]
            stub = json.loads(stub_path.read_text(encoding="utf-8"))
            self.assertNotIn("cold_stored_at", stub)


if __name__ == "__main__":
    unittest.main()
