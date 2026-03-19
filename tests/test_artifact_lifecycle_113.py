"""Tests for issue #113: artifact lifecycle for coordination history and terminal workflow artifacts.

Covers all five artifact families:
  - handoff (terminal maintenance pass)
  - shared_history (synchronous pre-write capture)
  - reconciliation (resolved maintenance pass)
  - task_done (done task maintenance pass)
  - patch_applied (applied patch maintenance pass)

Also covers the orchestrator, rollback, degradation, and deterministic ordering.
"""

from __future__ import annotations

import json
import shutil
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from app.artifact_lifecycle.service import (
    HANDOFFS_DIR_REL,
    HANDOFFS_HISTORY_DIR_REL,
    SHARED_HISTORY_DIR_REL,
    RECONCILIATIONS_DIR_REL,
    RECONCILIATIONS_HISTORY_DIR_REL,
    TASKS_DONE_DIR_REL,
    TASKS_HISTORY_DONE_DIR_REL,
    PATCHES_APPLIED_DIR_REL,
    PATCHES_HISTORY_APPLIED_DIR_REL,
    handoff_maintenance_pass,
    externalize_superseded_shared,
    reconciliation_maintenance_pass,
    task_done_maintenance_pass,
    patch_applied_maintenance_pass,
    artifact_lifecycle_maintenance_service,
)
from app.storage import safe_path, write_text_file


def _now() -> datetime:
    return datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)


def _write_artifact(repo: Path, rel: str, data: dict) -> Path:
    path = safe_path(repo, rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))
    return path


class HandoffMaintenanceTestCase(unittest.TestCase):
    """Tests for the handoff maintenance pass."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def _make_handoff(self, handoff_id: str, status: str, days_old: int, **extra) -> dict:
        ts = (self.now - timedelta(days=days_old)).isoformat()
        artifact = {
            "handoff_id": handoff_id,
            "created_at": ts,
            "updated_at": ts,
            "sender_peer": "peer-alpha",
            "recipient_peer": "peer-beta",
            "recipient_status": status,
            "task_id": "task-1",
            "thread_id": "thread-1",
            **extra,
        }
        _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{handoff_id}.json", artifact)
        return artifact

    def test_no_eligible_handoffs(self):
        """Pending handoffs are not externalized."""
        self._make_handoff("handoff_aaaa0000000000000000000000000001", "pending", 60)
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 0)

    def test_terminal_within_retention(self):
        """Terminal handoffs within retention window are not externalized."""
        self._make_handoff("handoff_aaaa0000000000000000000000000001", "accepted_advisory", 10)
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)

    def test_terminal_beyond_retention(self):
        """Terminal handoffs beyond retention window are externalized."""
        hid = "handoff_aaaa0000000000000000000000000001"
        self._make_handoff(hid, "accepted_advisory", 45)
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 1)

        # Hot artifact removed
        hot_path = safe_path(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json")
        self.assertFalse(hot_path.exists())

        # History payload created
        history_dir = safe_path(self.repo, HANDOFFS_HISTORY_DIR_REL)
        payloads = list(history_dir.glob("handoff__*.json"))
        self.assertEqual(len(payloads), 1)

        payload = json.loads(payloads[0].read_text())
        self.assertEqual(payload["schema_type"], "handoff_history_unit")
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertEqual(payload["family"], "handoff")
        self.assertEqual(payload["artifact_id"], hid)
        self.assertIn("artifact", payload)
        self.assertIn("summary", payload)
        self.assertEqual(payload["summary"]["recipient_status"], "accepted_advisory")

        # Stub created
        stub_dir = safe_path(self.repo, f"{HANDOFFS_HISTORY_DIR_REL}/index")
        stubs = list(stub_dir.glob("handoff__*.json"))
        self.assertEqual(len(stubs), 1)
        stub = json.loads(stubs[0].read_text())
        self.assertEqual(stub["schema_type"], "artifact_history_stub")
        self.assertEqual(stub["family"], "handoff")
        self.assertEqual(stub["summary"], payload["summary"])

    def test_consumed_at_preferred_over_updated_at(self):
        """consumed_at is used as retention timestamp when present."""
        hid = "handoff_aaaa0000000000000000000000000001"
        consumed_ts = (self.now - timedelta(days=45)).isoformat()
        updated_ts = (self.now - timedelta(days=5)).isoformat()
        self._make_handoff(hid, "deferred", 60, consumed_at=consumed_ts, updated_at=updated_ts)

        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 1)

    def test_all_three_terminal_statuses(self):
        """All terminal status values trigger externalization."""
        for i, status in enumerate(["accepted_advisory", "deferred", "rejected"]):
            hid = f"handoff_aaaa000000000000000000000000000{i+1}"
            self._make_handoff(hid, status, 45)

        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 3)

    def test_batch_limit(self):
        """Batch limit stops selection."""
        for i in range(5):
            hid = f"handoff_aaaa000000000000000000000000000{i+1}"
            self._make_handoff(hid, "accepted_advisory", 45 + i)

        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=3,
        )
        self.assertEqual(result["externalized"], 3)
        # 2 remaining hot
        hot_dir = safe_path(self.repo, HANDOFFS_DIR_REL)
        remaining = [p for p in hot_dir.iterdir() if p.is_file() and p.suffix == ".json"]
        self.assertEqual(len(remaining), 2)

    def test_missing_retention_timestamp_warning(self):
        """Handoffs with no parseable timestamps produce warnings."""
        hid = "handoff_aaaa0000000000000000000000000001"
        _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json", {
            "handoff_id": hid,
            "recipient_status": "accepted_advisory",
            "task_id": "t1",
        })
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)
        self.assertTrue(any("handoff_retention_missing" in w for w in result["warnings"]))

    def test_corrupt_artifact_warning(self):
        """Corrupt JSON produces warnings and is skipped."""
        path = safe_path(self.repo, f"{HANDOFFS_DIR_REL}/handoff_bad.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{invalid json", encoding="utf-8")

        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertTrue(any("artifact_corrupt" in w for w in result["warnings"]))

    def test_history_id_format(self):
        """history_id follows <family>__<YYYYMMDDTHHMMSSZ>__<seq> format."""
        hid = "handoff_aaaa0000000000000000000000000001"
        self._make_handoff(hid, "rejected", 45)

        handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )

        history_dir = safe_path(self.repo, HANDOFFS_HISTORY_DIR_REL)
        payloads = list(history_dir.glob("handoff__*.json"))
        payload = json.loads(payloads[0].read_text())
        hid_val = payload["history_id"]
        self.assertTrue(hid_val.startswith("handoff__"))
        parts = hid_val.split("__")
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], "handoff")
        self.assertTrue(parts[1].endswith("Z"))
        self.assertEqual(len(parts[2]), 4)
        self.assertEqual(int(parts[2]), 1)

    def test_sequence_allocation(self):
        """Sequence increments for same family+timestamp."""
        # Create two terminal handoffs
        for i in range(2):
            hid = f"handoff_aaaa000000000000000000000000000{i+1}"
            self._make_handoff(hid, "accepted_advisory", 45)

        handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )

        history_dir = safe_path(self.repo, HANDOFFS_HISTORY_DIR_REL)
        payloads = sorted(history_dir.glob("handoff__*.json"))
        self.assertEqual(len(payloads), 2)
        p1 = json.loads(payloads[0].read_text())
        p2 = json.loads(payloads[1].read_text())
        seq1 = int(p1["history_id"].split("__")[2])
        seq2 = int(p2["history_id"].split("__")[2])
        self.assertEqual(seq1, 1)
        self.assertEqual(seq2, 2)

    def test_sorted_key_order(self):
        """Artifacts are selected in sorted handoff_id order."""
        ids = ["handoff_cccc0000000000000000000000000001",
               "handoff_aaaa0000000000000000000000000001",
               "handoff_bbbb0000000000000000000000000001"]
        for hid in ids:
            self._make_handoff(hid, "accepted_advisory", 45)

        handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=2,
        )

        # Remaining should be the last in sorted order (cccc)
        hot_dir = safe_path(self.repo, HANDOFFS_DIR_REL)
        remaining = sorted([p.stem for p in hot_dir.iterdir() if p.is_file() and p.suffix == ".json"])
        self.assertEqual(remaining, ["handoff_cccc0000000000000000000000000001"])


class SharedHistoryTestCase(unittest.TestCase):
    """Tests for synchronous pre-write shared history capture."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def test_within_retention_returns_none(self):
        """Artifact within hot retention window is not externalized."""
        artifact = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "version": 2,
            "task_id": "task-1",
            "thread_id": "thread-1",
            "updated_at": (self.now - timedelta(days=5)).isoformat(),
        }
        result = externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=artifact, hot_retention_days=30,
        )
        self.assertIsNone(result)

    def test_beyond_retention_externalizes(self):
        """Artifact beyond hot retention window is externalized."""
        artifact = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "version": 2,
            "task_id": "task-1",
            "thread_id": "thread-1",
            "updated_at": (self.now - timedelta(days=45)).isoformat(),
        }
        result = externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=artifact, hot_retention_days=30,
        )
        self.assertIsNotNone(result)
        self.assertIn("history_id", result)
        self.assertIn("payload_path", result)
        self.assertIn("stub_path", result)

        # Verify payload
        payload_path = safe_path(self.repo, result["payload_path"])
        payload = json.loads(payload_path.read_text())
        self.assertEqual(payload["schema_type"], "shared_history_unit")
        self.assertEqual(payload["family"], "shared_history")
        self.assertEqual(payload["artifact"]["shared_id"], "shared_aaaa0000000000000000000000000001")
        self.assertEqual(payload["summary"]["version"], 2)
        self.assertEqual(payload["summary"]["participant_peer_count"], 1)

        # Verify stub
        stub_path = safe_path(self.repo, result["stub_path"])
        stub = json.loads(stub_path.read_text())
        self.assertEqual(stub["schema_type"], "artifact_history_stub")
        self.assertEqual(stub["summary"], payload["summary"])

    def test_missing_updated_at_returns_none(self):
        """Artifact with missing updated_at is not externalized."""
        artifact = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": [],
            "version": 1,
        }
        result = externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=artifact, hot_retention_days=30,
        )
        self.assertIsNone(result)

    def test_rollback_on_write_failure(self):
        """If stub write fails, payload is cleaned up."""
        from unittest.mock import patch

        artifact = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "version": 2,
            "task_id": "task-1",
            "thread_id": "thread-1",
            "updated_at": (self.now - timedelta(days=45)).isoformat(),
        }

        call_count = 0

        def failing_write_exclusive(path, data):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise OSError("disk full")
            import os
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            try:
                os.write(fd, json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
            finally:
                os.close(fd)

        with patch("app.artifact_lifecycle.service._write_json_exclusive", side_effect=failing_write_exclusive):
            with self.assertRaises(OSError):
                externalize_superseded_shared(
                    repo_root=self.repo, now=self.now,
                    previous_artifact=artifact, hot_retention_days=30,
                )

        # Nothing should remain
        history_dir = safe_path(self.repo, SHARED_HISTORY_DIR_REL)
        if history_dir.exists():
            payloads = list(history_dir.glob("shared_history__*.json"))
            self.assertEqual(len(payloads), 0)


class ReconciliationMaintenanceTestCase(unittest.TestCase):
    """Tests for the reconciliation maintenance pass."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def _make_reconciliation(self, recon_id: str, status: str, days_old: int) -> dict:
        ts = (self.now - timedelta(days=days_old)).isoformat()
        artifact = {
            "reconciliation_id": recon_id,
            "created_at": ts,
            "updated_at": ts,
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "status": status,
            "resolution_outcome": "advisory_only" if status == "resolved" else None,
            "claims": [{"claimant_peer": "peer-alpha"}, {"claimant_peer": "peer-beta"}],
            "task_id": "task-1",
            "thread_id": "thread-1",
        }
        _write_artifact(self.repo, f"{RECONCILIATIONS_DIR_REL}/{recon_id}.json", artifact)
        return artifact

    def test_open_not_externalized(self):
        """Open reconciliations are not externalized."""
        self._make_reconciliation("recon_aaaa0000000000000000000000000001", "open", 60)
        result = reconciliation_maintenance_pass(
            repo_root=self.repo, now=self.now,
            resolved_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)

    def test_resolved_beyond_retention(self):
        """Resolved reconciliations beyond retention are externalized."""
        rid = "recon_aaaa0000000000000000000000000001"
        self._make_reconciliation(rid, "resolved", 45)
        result = reconciliation_maintenance_pass(
            repo_root=self.repo, now=self.now,
            resolved_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 1)

        hot_path = safe_path(self.repo, f"{RECONCILIATIONS_DIR_REL}/{rid}.json")
        self.assertFalse(hot_path.exists())

        history_dir = safe_path(self.repo, RECONCILIATIONS_HISTORY_DIR_REL)
        payloads = list(history_dir.glob("reconciliation__*.json"))
        self.assertEqual(len(payloads), 1)

        payload = json.loads(payloads[0].read_text())
        self.assertEqual(payload["schema_type"], "reconciliation_history_unit")
        self.assertEqual(payload["summary"]["status"], "resolved")
        self.assertEqual(payload["summary"]["claim_count"], 2)

    def test_missing_updated_at_warning(self):
        """Resolved reconciliation with no updated_at produces warning."""
        rid = "recon_aaaa0000000000000000000000000001"
        _write_artifact(self.repo, f"{RECONCILIATIONS_DIR_REL}/{rid}.json", {
            "reconciliation_id": rid,
            "status": "resolved",
            "claims": [],
        })
        result = reconciliation_maintenance_pass(
            repo_root=self.repo, now=self.now,
            resolved_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)
        self.assertTrue(any("reconciliation_retention_missing" in w for w in result["warnings"]))


class TaskDoneMaintenanceTestCase(unittest.TestCase):
    """Tests for the done-task maintenance pass."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def _make_done_task(self, task_id: str, days_old: int) -> dict:
        ts = (self.now - timedelta(days=days_old)).isoformat()
        artifact = {
            "task_id": task_id,
            "status": "done",
            "owner_peer": "peer-alpha",
            "thread_id": "thread-1",
            "updated_at": ts,
        }
        _write_artifact(self.repo, f"{TASKS_DONE_DIR_REL}/{task_id}.json", artifact)
        return artifact

    def test_done_beyond_retention(self):
        """Done tasks beyond retention are externalized."""
        tid = "task-123"
        self._make_done_task(tid, 45)
        result = task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 1)

        hot_path = safe_path(self.repo, f"{TASKS_DONE_DIR_REL}/{tid}.json")
        self.assertFalse(hot_path.exists())

        history_dir = safe_path(self.repo, TASKS_HISTORY_DONE_DIR_REL)
        payloads = list(history_dir.glob("task_done__*.json"))
        self.assertEqual(len(payloads), 1)

        payload = json.loads(payloads[0].read_text())
        self.assertEqual(payload["schema_type"], "task_done_history_unit")
        self.assertEqual(payload["summary"]["task_id"], "task-123")
        self.assertEqual(payload["summary"]["status"], "done")

    def test_recent_done_not_externalized(self):
        """Recent done tasks within retention are not externalized."""
        self._make_done_task("task-recent", 10)
        result = task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)

    def test_missing_updated_at(self):
        """Done tasks with no updated_at produce warnings."""
        _write_artifact(self.repo, f"{TASKS_DONE_DIR_REL}/task-bad.json", {
            "task_id": "task-bad", "status": "done",
        })
        result = task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertTrue(any("task_done_retention_missing" in w for w in result["warnings"]))


class PatchAppliedMaintenanceTestCase(unittest.TestCase):
    """Tests for the applied-patch maintenance pass."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def _make_applied_patch(self, patch_id: str, days_old: int) -> dict:
        ts = (self.now - timedelta(days=days_old)).isoformat()
        artifact = {
            "patch_id": patch_id,
            "patch_type": "doc_patch",
            "target_path": "README.md",
            "status": "applied",
            "applied_commit": "abc123",
            "updated_at": ts,
        }
        _write_artifact(self.repo, f"{PATCHES_APPLIED_DIR_REL}/{patch_id}.json", artifact)
        return artifact

    def test_applied_beyond_retention(self):
        """Applied patches beyond retention are externalized."""
        pid = "patch_aaaaaaaaaaaa"
        self._make_applied_patch(pid, 45)
        result = patch_applied_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 1)

        hot_path = safe_path(self.repo, f"{PATCHES_APPLIED_DIR_REL}/{pid}.json")
        self.assertFalse(hot_path.exists())

        history_dir = safe_path(self.repo, PATCHES_HISTORY_APPLIED_DIR_REL)
        payloads = list(history_dir.glob("patch_applied__*.json"))
        self.assertEqual(len(payloads), 1)

        payload = json.loads(payloads[0].read_text())
        self.assertEqual(payload["schema_type"], "patch_applied_history_unit")
        self.assertEqual(payload["summary"]["patch_id"], "patch_aaaaaaaaaaaa")
        self.assertEqual(payload["summary"]["applied_commit"], "abc123")

    def test_recent_applied_not_externalized(self):
        """Recent applied patches within retention are not externalized."""
        self._make_applied_patch("patch_bbbbbbbbbbbb", 10)
        result = patch_applied_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertEqual(result["externalized"], 0)


class OrchestratorTestCase(unittest.TestCase):
    """Tests for the artifact lifecycle maintenance orchestrator."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    @dataclass(frozen=True)
    class FakeSettings:
        handoff_terminal_retention_days: int = 30
        handoff_cold_after_days: int = 90
        shared_history_hot_retention_days: int = 30
        shared_history_cold_after_days: int = 90
        reconciliation_resolved_retention_days: int = 30
        reconciliation_cold_after_days: int = 90
        task_done_hot_retention_days: int = 30
        task_done_cold_after_days: int = 90
        patch_applied_hot_retention_days: int = 30
        patch_applied_cold_after_days: int = 90
        artifact_history_batch_limit: int = 500

    def test_runs_all_families_empty(self):
        """Orchestrator runs all families with no eligible artifacts."""
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=self.FakeSettings(),
        )
        self.assertTrue(result["ok"])
        self.assertIn("handoff", result["families"])
        self.assertIn("reconciliation", result["families"])
        self.assertIn("task_done", result["families"])
        self.assertIn("patch_applied", result["families"])

    def test_family_order(self):
        """Orchestrator processes families in spec-defined order."""
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=self.FakeSettings(),
        )
        family_keys = list(result["families"].keys())
        self.assertEqual(family_keys, ["handoff", "reconciliation", "task_done", "patch_applied"])

    def test_stops_after_batch_limit(self):
        """Orchestrator stops processing families after one reaches the batch limit."""
        # Create 5 terminal handoffs
        for i in range(5):
            hid = f"handoff_aaaa000000000000000000000000000{i+1}"
            ts = (self.now - timedelta(days=45 + i)).isoformat()
            _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json", {
                "handoff_id": hid,
                "created_at": ts,
                "updated_at": ts,
                "recipient_status": "accepted_advisory",
                "sender_peer": "peer-alpha",
                "recipient_peer": "peer-beta",
                "task_id": "t1",
            })

        # Create a done task that should not be processed
        ts_old = (self.now - timedelta(days=60)).isoformat()
        _write_artifact(self.repo, f"{TASKS_DONE_DIR_REL}/task-old.json", {
            "task_id": "task-old", "status": "done",
            "updated_at": ts_old, "owner_peer": "p1",
        })

        settings = self.FakeSettings(artifact_history_batch_limit=3)
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=settings,
        )

        # Handoff should have been processed (3 externalized)
        self.assertEqual(result["families"]["handoff"]["externalized"], 3)
        # task_done should NOT be in results (stopped after handoff reached limit)
        self.assertNotIn("task_done", result["families"])

    def test_cumulative_budget_across_families(self):
        """Budget is tracked cumulatively: later families get reduced limit."""
        # Create 2 terminal handoffs and 2 resolved reconciliations, limit=3
        for i in range(2):
            hid = f"handoff_aaaa000000000000000000000000000{i+1}"
            ts = (self.now - timedelta(days=45)).isoformat()
            _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json", {
                "handoff_id": hid, "created_at": ts, "updated_at": ts,
                "recipient_status": "accepted_advisory",
                "sender_peer": "p1", "recipient_peer": "p2", "task_id": "t1",
            })
        for i in range(2):
            rid = f"recon_aaaa000000000000000000000000000{i+1}"
            ts = (self.now - timedelta(days=45)).isoformat()
            _write_artifact(self.repo, f"{RECONCILIATIONS_DIR_REL}/{rid}.json", {
                "reconciliation_id": rid, "created_at": ts, "updated_at": ts,
                "owner_peer": "p1", "participant_peers": ["p2"],
                "status": "resolved", "resolution_outcome": "advisory_only",
                "claims": [{"claimant_peer": "p1"}], "task_id": "t1",
            })

        settings = self.FakeSettings(artifact_history_batch_limit=3)
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now, settings=settings,
        )

        # Handoff externalizes 2, leaving budget=1 for reconciliation
        self.assertEqual(result["families"]["handoff"]["externalized"], 2)
        self.assertEqual(result["families"]["reconciliation"]["externalized"], 1)
        # Total across families must not exceed the batch limit
        total = sum(
            r.get("externalized", 0) for r in result["families"].values() if isinstance(r, dict)
        )
        self.assertLessEqual(total, 3)

    def test_selective_families(self):
        """Orchestrator runs only requested families."""
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            families=["task_done"],
            settings=self.FakeSettings(),
        )
        self.assertIn("task_done", result["families"])
        self.assertNotIn("handoff", result["families"])

    def test_family_failure_continues(self):
        """If one family fails, others still run."""
        from unittest.mock import patch

        with patch("app.artifact_lifecycle.service.handoff_maintenance_pass", side_effect=RuntimeError("boom")):
            result = artifact_lifecycle_maintenance_service(
                repo_root=self.repo, gm=None, now=self.now,
                settings=self.FakeSettings(),
            )

        self.assertFalse(result["families"]["handoff"]["ok"])
        self.assertTrue(result["families"]["reconciliation"]["ok"])
        self.assertTrue(any("artifact_maintenance_failed:handoff" in w for w in result["warnings"]))


class RollbackTestCase(unittest.TestCase):
    """Tests for rollback behavior on write failures."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def test_handoff_rollback_on_write_failure(self):
        """If history write fails, hot artifact is preserved."""
        hid = "handoff_aaaa0000000000000000000000000001"
        ts = (self.now - timedelta(days=45)).isoformat()
        _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json", {
            "handoff_id": hid,
            "created_at": ts,
            "updated_at": ts,
            "recipient_status": "accepted_advisory",
            "sender_peer": "peer-alpha",
            "recipient_peer": "peer-beta",
            "task_id": "t1",
        })

        from unittest.mock import patch

        def failing_write(path, data):
            raise OSError("disk full")

        with patch("app.artifact_lifecycle.service._write_json_exclusive", side_effect=failing_write):
            with self.assertRaises(OSError):
                handoff_maintenance_pass(
                    repo_root=self.repo, now=self.now,
                    terminal_retention_days=30, batch_limit=500,
                )

        # Hot artifact should still exist
        hot_path = safe_path(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json")
        self.assertTrue(hot_path.exists())


class StubPayloadSymmetryTestCase(unittest.TestCase):
    """Tests ensuring stub summary equals payload summary across all families."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def _assert_stub_payload_match(self, history_dir_rel: str, family_prefix: str):
        history_dir = safe_path(self.repo, history_dir_rel)
        index_dir = history_dir / "index"
        if not history_dir.exists():
            return

        payloads = sorted(history_dir.glob(f"{family_prefix}__*.json"))
        for payload_path in payloads:
            payload = json.loads(payload_path.read_text())
            stub_path = index_dir / payload_path.name
            self.assertTrue(stub_path.exists(), f"Missing stub for {payload_path.name}")
            stub = json.loads(stub_path.read_text())
            self.assertEqual(stub["summary"], payload["summary"],
                             f"Stub/payload summary mismatch for {payload_path.name}")
            self.assertEqual(stub["payload_path"], f"{history_dir_rel}/{payload_path.name}")

    def test_handoff_stub_payload_match(self):
        ts = (self.now - timedelta(days=45)).isoformat()
        _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/handoff_aaaa0000000000000000000000000001.json", {
            "handoff_id": "handoff_aaaa0000000000000000000000000001",
            "created_at": ts, "updated_at": ts,
            "recipient_status": "accepted_advisory",
            "sender_peer": "p1", "recipient_peer": "p2",
            "task_id": "t1", "thread_id": "th1",
        })
        handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self._assert_stub_payload_match(HANDOFFS_HISTORY_DIR_REL, "handoff")

    def test_reconciliation_stub_payload_match(self):
        ts = (self.now - timedelta(days=45)).isoformat()
        _write_artifact(self.repo, f"{RECONCILIATIONS_DIR_REL}/recon_aaaa0000000000000000000000000001.json", {
            "reconciliation_id": "recon_aaaa0000000000000000000000000001",
            "created_at": ts, "updated_at": ts,
            "owner_peer": "p1", "participant_peers": ["p2"],
            "status": "resolved", "resolution_outcome": "advisory_only",
            "claims": [{"claimant_peer": "p1"}, {"claimant_peer": "p2"}],
            "task_id": "t1", "thread_id": "th1",
        })
        reconciliation_maintenance_pass(
            repo_root=self.repo, now=self.now,
            resolved_retention_days=30, batch_limit=500,
        )
        self._assert_stub_payload_match(RECONCILIATIONS_HISTORY_DIR_REL, "reconciliation")

    def test_task_done_stub_payload_match(self):
        ts = (self.now - timedelta(days=45)).isoformat()
        _write_artifact(self.repo, f"{TASKS_DONE_DIR_REL}/task-1.json", {
            "task_id": "task-1", "status": "done", "owner_peer": "p1",
            "thread_id": "th1", "updated_at": ts,
        })
        task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self._assert_stub_payload_match(TASKS_HISTORY_DONE_DIR_REL, "task_done")

    def test_patch_applied_stub_payload_match(self):
        ts = (self.now - timedelta(days=45)).isoformat()
        _write_artifact(self.repo, f"{PATCHES_APPLIED_DIR_REL}/patch_aaaaaaaaaaaa.json", {
            "patch_id": "patch_aaaaaaaaaaaa", "patch_type": "doc_patch",
            "target_path": "README.md", "status": "applied",
            "applied_commit": "abc123", "updated_at": ts,
        })
        patch_applied_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self._assert_stub_payload_match(PATCHES_HISTORY_APPLIED_DIR_REL, "patch_applied")

    def test_shared_history_stub_payload_match(self):
        artifact = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "p1", "participant_peers": ["p2"],
            "version": 3, "task_id": "t1", "thread_id": "th1",
            "updated_at": (self.now - timedelta(days=45)).isoformat(),
        }
        externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=artifact, hot_retention_days=30,
        )
        self._assert_stub_payload_match(SHARED_HISTORY_DIR_REL, "shared_history")


class EmptyDirectoryTestCase(unittest.TestCase):
    """Tests for empty or missing directories."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def test_missing_handoffs_dir(self):
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 0)

    def test_missing_reconciliations_dir(self):
        result = reconciliation_maintenance_pass(
            repo_root=self.repo, now=self.now,
            resolved_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 0)

    def test_missing_tasks_done_dir(self):
        result = task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 0)

    def test_missing_patches_applied_dir(self):
        result = patch_applied_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 0)


class SettingsIntegrationTestCase(unittest.TestCase):
    """Test that settings are correctly loaded."""

    def test_settings_have_artifact_lifecycle_fields(self):
        """All 11 issue #113 settings are present on the Settings dataclass."""
        from app.config import Settings
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(Settings)}
        expected = {
            "handoff_terminal_retention_days",
            "handoff_cold_after_days",
            "shared_history_hot_retention_days",
            "shared_history_cold_after_days",
            "reconciliation_resolved_retention_days",
            "reconciliation_cold_after_days",
            "task_done_hot_retention_days",
            "task_done_cold_after_days",
            "patch_applied_hot_retention_days",
            "patch_applied_cold_after_days",
            "artifact_history_batch_limit",
        }
        self.assertTrue(expected.issubset(field_names))


class NaiveTimestampTestCase(unittest.TestCase):
    """Tests that naive (offset-less) timestamps don't crash comparisons."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def test_naive_timestamp_handoff_does_not_crash(self):
        """Handoff with naive timestamp (no offset) is handled gracefully."""
        hid = "handoff_aaaa0000000000000000000000000001"
        # Naive timestamp: no Z, no +00:00
        naive_ts = "2026-02-01T10:00:00"
        _write_artifact(self.repo, f"{HANDOFFS_DIR_REL}/{hid}.json", {
            "handoff_id": hid,
            "created_at": naive_ts,
            "updated_at": naive_ts,
            "recipient_status": "accepted_advisory",
            "sender_peer": "p1",
            "recipient_peer": "p2",
            "task_id": "t1",
        })
        # Should not raise TypeError
        result = handoff_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        # Naive timestamp treated as UTC, 46 days old → externalized
        self.assertEqual(result["externalized"], 1)

    def test_naive_timestamp_task_done(self):
        """Done task with naive timestamp is handled gracefully."""
        _write_artifact(self.repo, f"{TASKS_DONE_DIR_REL}/task-naive.json", {
            "task_id": "task-naive", "status": "done",
            "owner_peer": "p1", "updated_at": "2026-02-01T10:00:00",
        })
        result = task_done_maintenance_pass(
            repo_root=self.repo, now=self.now,
            hot_retention_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["externalized"], 1)


class OrchestratorResponseFieldsTestCase(unittest.TestCase):
    """Tests for the ok/degraded fields in orchestrator response."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    @dataclass(frozen=True)
    class FakeSettings:
        handoff_terminal_retention_days: int = 30
        reconciliation_resolved_retention_days: int = 30
        task_done_hot_retention_days: int = 30
        patch_applied_hot_retention_days: int = 30
        artifact_history_batch_limit: int = 500

    def test_ok_false_when_family_fails(self):
        """Top-level ok is False when any family raises an exception."""
        from unittest.mock import patch

        with patch("app.artifact_lifecycle.service.handoff_maintenance_pass", side_effect=RuntimeError("boom")):
            result = artifact_lifecycle_maintenance_service(
                repo_root=self.repo, gm=None, now=self.now,
                settings=self.FakeSettings(),
            )
        self.assertFalse(result["ok"])

    def test_degraded_false_on_clean_pass(self):
        """degraded is False when no git issues occur."""
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=self.FakeSettings(),
        )
        self.assertFalse(result["degraded"])

    def test_degraded_present_in_response(self):
        """degraded field is always present in orchestrator response."""
        result = artifact_lifecycle_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=self.FakeSettings(),
        )
        self.assertIn("degraded", result)


class SharedUpdateHookIntegrationTestCase(unittest.TestCase):
    """Integration tests for the shared_update_service pre-write capture hook."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.now = _now()

    def test_capture_produces_history_for_old_version(self):
        """When a shared artifact with an old updated_at is superseded, history is created."""
        old_ts = (self.now - timedelta(days=45)).isoformat()
        previous = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "version": 2,
            "task_id": "task-1",
            "thread_id": "thread-1",
            "updated_at": old_ts,
        }
        result = externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=previous, hot_retention_days=30,
        )
        self.assertIsNotNone(result)

        # Verify payload exists and contains the full previous artifact
        payload = json.loads(safe_path(self.repo, result["payload_path"]).read_text())
        self.assertEqual(payload["artifact"]["version"], 2)
        self.assertEqual(payload["artifact"]["shared_id"], "shared_aaaa0000000000000000000000000001")

        # Verify stub exists
        stub = json.loads(safe_path(self.repo, result["stub_path"]).read_text())
        self.assertEqual(stub["schema_type"], "artifact_history_stub")
        self.assertEqual(stub["family"], "shared_history")

    def test_capture_failure_is_nonfatal(self):
        """When externalize_superseded_shared raises, the caller should catch and proceed."""
        from unittest.mock import patch

        previous = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": [],
            "version": 1,
            "updated_at": (self.now - timedelta(days=45)).isoformat(),
        }

        with patch("app.artifact_lifecycle.service._write_json_exclusive", side_effect=OSError("disk full")):
            # The function itself raises, but the shared_update_service
            # wraps it in a try/except — here we test the function does raise
            with self.assertRaises(OSError):
                externalize_superseded_shared(
                    repo_root=self.repo, now=self.now,
                    previous_artifact=previous, hot_retention_days=30,
                )

    def test_concurrent_capture_retry_on_collision(self):
        """When O_EXCL detects a collision, the function retries with a new sequence."""
        previous = {
            "shared_id": "shared_aaaa0000000000000000000000000001",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "version": 3,
            "task_id": "t1",
            "thread_id": "th1",
            "updated_at": (self.now - timedelta(days=45)).isoformat(),
        }

        # Pre-create the first payload file to simulate a concurrent writer
        from app.artifact_lifecycle.service import (
            _history_timestamp_str,
            SHARED_HISTORY_DIR_REL,
        )
        ts_str = _history_timestamp_str(self.now)
        first_id = f"shared_history__{ts_str}__0001"
        first_path = safe_path(self.repo, f"{SHARED_HISTORY_DIR_REL}/{first_id}.json")
        first_path.parent.mkdir(parents=True, exist_ok=True)
        first_path.write_text("{}", encoding="utf-8")
        # Also create the index stub
        stub_dir = safe_path(self.repo, f"{SHARED_HISTORY_DIR_REL}/index")
        stub_dir.mkdir(parents=True, exist_ok=True)
        (stub_dir / f"{first_id}.json").write_text("{}", encoding="utf-8")

        result = externalize_superseded_shared(
            repo_root=self.repo, now=self.now,
            previous_artifact=previous, hot_retention_days=30,
        )
        self.assertIsNotNone(result)
        # Should have allocated sequence 0002
        self.assertIn("__0002", result["history_id"])


if __name__ == "__main__":
    unittest.main()
