"""Tests for segment-history residue detection and manifest reconciliation (issue #114, Phase 8)."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests.helpers import SimpleGitManagerStub

from app.segment_history.manifest import write_manifest
from app.segment_history.service import (
    _reconcile_manifest_residue,
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
    journal_cold_after_days: int = 30
    journal_retention_days: int = 365


class TestReconcileManifestResidue(unittest.TestCase):
    def test_no_manifest_no_residue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            result = _reconcile_manifest_residue(repo, "journal", "maintenance", gm)
            self.assertIsNone(result)

    def test_stale_manifest_cleaned_up(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            # Write a manifest from a completed-but-uncleared operation
            write_manifest(
                repo,
                operation="maintenance",
                family="journal",
                source_paths=["journal/2026-03-19.jsonl"],
                segment_ids=["journal__20260320T000000Z__0001"],
            )
            result = _reconcile_manifest_residue(repo, "journal", "maintenance", gm)
            # Should clean up and return a warning dict
            self.assertIsNotNone(result)
            self.assertIn("segment_history_manifest_residue", result.get("warning", ""))

    def test_corrupt_manifest_cleaned_up(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            from app.segment_history.manifest import manifest_path

            path = manifest_path(repo, "journal")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("not json", encoding="utf-8")

            result = _reconcile_manifest_residue(repo, "journal", "maintenance", gm)
            self.assertIsNotNone(result)
            self.assertIn("unreadable", result.get("warning", ""))

    def test_different_operation_type(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            # Write a cold_store manifest, but caller is maintenance
            write_manifest(
                repo,
                operation="cold_store",
                family="journal",
                source_paths=[],
                segment_ids=[],
            )
            result = _reconcile_manifest_residue(repo, "journal", "maintenance", gm)
            # Should still clean up cross-operation residue
            self.assertIsNotNone(result)


class TestMaintenanceWithResidue(unittest.TestCase):
    def test_maintenance_cleans_stale_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            # Use correct journal structure: journal/<year>/<YYYY-MM-DD>.md
            journal_year = repo / "journal" / "2026"
            journal_year.mkdir(parents=True)
            (journal_year / "2026-03-19.md").write_text("# Past day entry\n")

            # Plant a stale manifest with source paths that overlap the actual sources
            write_manifest(
                repo,
                operation="maintenance",
                family="journal",
                source_paths=["journal/2026/2026-03-19.md"],
                segment_ids=["journal__2026__2026-03-19__20260320T000000Z__0001"],
            )

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            result = segment_history_maintenance_service(
                family="journal",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
            )

            self.assertTrue(result["ok"])
            # Stale manifest should have been handled — verify warning was emitted
            warning_strs = [w.get("detail", "") if isinstance(w, dict) else str(w) for w in result["warnings"]]
            self.assertTrue(
                any("residue" in w or "manifest" in w for w in warning_strs),
                f"Expected residue/manifest warning, got: {result['warnings']}",
            )
            # Verify manifest was actually removed
            from app.segment_history.manifest import read_manifest

            self.assertIsNone(read_manifest(repo, "journal"))


if __name__ == "__main__":
    unittest.main()
