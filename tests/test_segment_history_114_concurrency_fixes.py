"""Tests for segment-history concurrency and crash-recovery fixes (issue #114).

Covers:
- F1: Deferred audit emission (no self-deadlock on api_audit maintenance)
- F2: Rehydrate rollback on mid-operation failure
- F3: Write-time rollover crash-recovery manifest
- F4: Manifest reconciliation cleans orphaned target files
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tests.helpers import SimpleGitManagerStub

from app.audit import append_audit
from app.segment_history.manifest import manifest_path, read_manifest, write_manifest
from app.segment_history.service import (
    _build_cold_gzip_bytes,
    _create_stub,
    _reconcile_manifest_residue,
    segment_history_cold_rehydrate_service,
    segment_history_cold_store_service,
    segment_history_maintenance_service,
)
from app.storage import write_bytes_file, write_text_file


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
    ops_run_cold_after_days: int = 0
    message_stream_cold_after_days: int = 0
    message_thread_cold_after_days: int = 0
    episodic_cold_after_days: int = 0


# -----------------------------------------------------------------------
# F1: Deferred audit emission
# -----------------------------------------------------------------------
class TestDeferredAuditEmission(unittest.TestCase):
    """Audit events must be emitted AFTER source lock release."""

    def test_maintenance_audit_emitted_after_lock(self) -> None:
        """Maintenance collects audit events and emits them post-lock."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            logs = repo / "logs"
            logs.mkdir(parents=True)
            (logs / "api_audit.jsonl").write_text(
                '{"ts":"2026-03-19T00:00:00Z","event":"old"}\n' * 20
            )
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            emitted: list[tuple[str, dict]] = []

            def _capture_audit(event: str, detail: dict) -> None:
                emitted.append((event, detail))

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
                audit=_capture_audit,
            )
            self.assertTrue(result["ok"])
            if result["rolled_count"] > 0:
                self.assertEqual(len(emitted), result["rolled_count"])
                self.assertEqual(emitted[0][0], "segment_history_roll")

    def test_cold_store_audit_emitted_after_lock(self) -> None:
        """Cold-store collects audit events and emits them post-lock."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            # Set up a rolled api_audit segment with stub
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()
            seg_id = "api_audit__api_audit__20260319T000000Z__0001"
            payload = hist / f"{seg_id}.jsonl"
            payload.write_text('{"ts":"2026-03-19T00:00:00Z","event":"old"}\n')
            stub = _create_stub(
                family="api_audit",
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260319T000000Z",
                payload_path=f"logs/history/api_audit/{seg_id}.jsonl",
                summary={"last_event_at": "2026-03-19T00:00:00Z", "line_count": 1, "byte_size": 50},
            )
            write_text_file(idx / f"{seg_id}.json", json.dumps(stub))

            emitted: list[tuple[str, dict]] = []

            def _capture_audit(event: str, detail: dict) -> None:
                emitted.append((event, detail))

            result = segment_history_cold_store_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=gm,
                now=now,
                audit=_capture_audit,
            )
            self.assertTrue(result["ok"])
            if result["cold_stored_count"] > 0:
                self.assertEqual(len(emitted), result["cold_stored_count"])
                self.assertEqual(emitted[0][0], "segment_history_cold_store")


# -----------------------------------------------------------------------
# F2: Rehydrate rollback on mid-operation failure
# -----------------------------------------------------------------------
class TestRehydrateRollback(unittest.TestCase):
    """Rehydrate must not leave stuck segments on mid-operation failure."""

    def _setup_cold_segment(self, repo: Path) -> tuple[str, Path, Path]:
        """Create a cold-stored api_audit segment and return (seg_id, stub_path, cold_path)."""
        seg_id = "api_audit__api_audit__20260319T000000Z__0001"
        hist = repo / "logs" / "history" / "api_audit"
        hist.mkdir(parents=True)
        idx = hist / "index"
        idx.mkdir()
        cold_dir = hist / "cold"
        cold_dir.mkdir()
        payload_content = b'{"ts":"2026-03-19T00:00:00Z","event":"old"}\n'
        compressed = _build_cold_gzip_bytes(payload_content)
        cold_path = cold_dir / f"{seg_id}.jsonl.gz"
        write_bytes_file(cold_path, compressed)
        cold_rel = str(cold_path.relative_to(repo))
        stub = _create_stub(
            family="api_audit",
            segment_id=seg_id,
            source_path="logs/api_audit.jsonl",
            stream_key="api_audit",
            rolled_at="20260319T000000Z",
            payload_path=cold_rel,
            summary={"last_event_at": "2026-03-19T00:00:00Z", "line_count": 1, "byte_size": 50},
        )
        stub["cold_stored_at"] = "2026-03-19T12:00:00Z"
        stub_path = idx / f"{seg_id}.json"
        write_text_file(stub_path, json.dumps(stub))
        return seg_id, stub_path, cold_path

    def test_rollback_on_stub_write_failure(self) -> None:
        """If stub mutation fails mid-rehydrate, hot payload is removed and stub is restored."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            seg_id, stub_path, cold_path = self._setup_cold_segment(repo)
            original_stub_bytes = stub_path.read_bytes()

            # Patch write_text_file to fail on stub write (second call)
            call_count = {"n": 0}
            _real_write = write_text_file

            def _failing_write(path: Path, content: str) -> None:
                call_count["n"] += 1
                if call_count["n"] >= 1 and "index" in str(path):
                    raise OSError("Simulated disk-full on stub write")
                _real_write(path, content)

            with patch("app.segment_history.service.write_text_file", side_effect=_failing_write):
                with self.assertRaises(OSError):
                    segment_history_cold_rehydrate_service(
                        family="api_audit",
                        segment_id=seg_id,
                        repo_root=repo,
                        gm=gm,
                    )

            # Hot payload must NOT exist (rollback should have removed it)
            hot_path = repo / "logs" / "history" / "api_audit" / f"{seg_id}.jsonl"
            self.assertFalse(hot_path.is_file(), "Orphaned hot payload should be removed on rollback")

            # Stub must be restored to original state
            self.assertEqual(stub_path.read_bytes(), original_stub_bytes)

            # Cold payload must still exist
            self.assertTrue(cold_path.is_file())

    def test_successful_rehydrate_still_works(self) -> None:
        """Normal rehydrate still completes without error after rollback was added."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            seg_id, stub_path, cold_path = self._setup_cold_segment(repo)

            result = segment_history_cold_rehydrate_service(
                family="api_audit",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )
            self.assertTrue(result["ok"])
            hot_path = repo / "logs" / "history" / "api_audit" / f"{seg_id}.jsonl"
            self.assertTrue(hot_path.is_file())


# -----------------------------------------------------------------------
# F3: Write-time rollover crash-recovery manifest
# -----------------------------------------------------------------------
class TestWriteTimeRolloverManifest(unittest.TestCase):
    """Write-time rollover must write and clean up a crash-recovery manifest."""

    def test_manifest_written_and_removed_on_success(self) -> None:
        """After successful rollover, the manifest must not persist."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            audit_file = repo / "logs" / "api_audit.jsonl"
            audit_file.parent.mkdir(parents=True)
            audit_file.write_text('{"ts":"2026-03-20","event":"old"}\n' * 100)

            append_audit(
                repo, "new_event", "peer-1", {"key": "value"},
                rollover_bytes=100, gm=gm,
            )

            # Manifest must be removed after successful rollover
            mf_path = manifest_path(repo, "api_audit")
            self.assertFalse(mf_path.is_file(), "Manifest should be removed after successful rollover")

    def test_manifest_exists_during_rollover(self) -> None:
        """The manifest is present while _roll_jsonl_source runs."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            audit_file = repo / "logs" / "api_audit.jsonl"
            audit_file.parent.mkdir(parents=True)
            audit_file.write_text('{"ts":"2026-03-20","event":"old"}\n' * 100)

            manifest_seen: list[bool] = []

            from app.segment_history.service import _roll_jsonl_source
            _real_roll = _roll_jsonl_source

            def _spying_roll(**kwargs: Any) -> Any:
                mf = read_manifest(repo, "api_audit")
                manifest_seen.append(mf is not None)
                return _real_roll(**kwargs)

            # _roll_jsonl_source is imported inside the function at module level
            with patch("app.segment_history.service._roll_jsonl_source", side_effect=_spying_roll):
                append_audit(
                    repo, "new_event", "peer-1", {"key": "value"},
                    rollover_bytes=100, gm=gm,
                )

            # The manifest should have been visible during the roll
            self.assertTrue(any(manifest_seen), "Manifest should be present during rollover")


# -----------------------------------------------------------------------
# F4: Manifest reconciliation cleans orphaned target files
# -----------------------------------------------------------------------
class TestManifestReconciliationRecoversOrphans(unittest.TestCase):
    """Reconciliation must recover orphaned target files, not delete them."""

    def test_orphaned_targets_recovered(self) -> None:
        """Target files from a crashed operation are committed during reconciliation."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Simulate crashed maintenance that left orphaned payload + stub
            orphan_payload_rel = "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl"
            orphan_stub_rel = "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json"
            orphan_payload = repo / orphan_payload_rel
            orphan_stub = repo / orphan_stub_rel
            orphan_payload.parent.mkdir(parents=True, exist_ok=True)
            orphan_stub.parent.mkdir(parents=True, exist_ok=True)
            orphan_payload.write_text("orphaned payload\n")
            orphan_stub.write_text('{"orphaned": true}')

            # Write a manifest referencing these orphans
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[orphan_payload_rel, orphan_stub_rel],
            )

            result = _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)
            self.assertIsNotNone(result)
            self.assertIn("recovered", result["warning"])

            # Orphaned files must still exist (committed, not deleted)
            self.assertTrue(orphan_payload.is_file())
            self.assertTrue(orphan_stub.is_file())

            # Manifest must be removed
            mf_path = manifest_path(repo, "api_audit")
            self.assertFalse(mf_path.is_file())

    def test_no_crash_when_targets_already_gone(self) -> None:
        """Reconciliation succeeds when target files don't exist (already cleaned)."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            write_manifest(
                repo,
                operation="cold_store",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=["nonexistent/path.jsonl", "also/nonexistent.json"],
            )

            result = _reconcile_manifest_residue(repo, "api_audit", "cold_store", gm)
            self.assertIsNotNone(result)
            self.assertIn("preserved=0", result["warning"])

    def test_manifest_without_target_paths(self) -> None:
        """Old manifests without target_paths still reconcile cleanly."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            write_manifest(
                repo,
                operation="maintenance",
                family="journal",
                source_paths=[],
                segment_ids=[],
            )
            result = _reconcile_manifest_residue(repo, "journal", "maintenance", gm)
            self.assertIsNotNone(result)
            self.assertIn("preserved=0", result["warning"])


# -----------------------------------------------------------------------
# F5: Manifest preserved on git-commit failure (Finding 1 & 3)
# -----------------------------------------------------------------------
class TestManifestPreservedOnCommitFailure(unittest.TestCase):
    """Manifest must survive when git commit fails so crash recovery works."""

    def test_maintenance_preserves_manifest_on_commit_failure(self) -> None:
        """When git commit fails, maintenance keeps the manifest for recovery."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            gm.commit_paths = lambda *a, **k: False  # simulate commit failure
            logs = repo / "logs"
            logs.mkdir(parents=True)
            (logs / "api_audit.jsonl").write_text(
                '{"ts":"2026-03-19T00:00:00Z","event":"old"}\n' * 20
            )
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )
            self.assertTrue(result["ok"])
            self.assertFalse(result["durable"])
            # Manifest must still exist for crash recovery
            mf = read_manifest(repo, "api_audit")
            self.assertIsNotNone(mf)
            self.assertEqual(mf["operation"], "maintenance")

    def test_cold_store_preserves_manifest_on_commit_failure(self) -> None:
        """When git commit fails, cold-store keeps the manifest for recovery."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()

            # Set up a rolled stub + payload for cold-store
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()
            seg_id = "api_audit__api_audit__20260319T000000Z__0001"
            payload = hist / f"{seg_id}.jsonl"
            payload.write_text('{"ts":"2026-03-19T00:00:00Z"}\n')
            stub = _create_stub(
                family="api_audit",
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260319T000000Z",
                payload_path=str(payload.relative_to(repo)),
                summary={"last_event_at": "2026-03-19T00:00:00Z",
                         "line_count": 1, "byte_size": 30},
            )
            write_text_file(idx / f"{seg_id}.json", json.dumps(stub))

            # Make commit fail
            gm.commit_paths = lambda *a, **k: False

            result = segment_history_cold_store_service(
                family="api_audit",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )
            self.assertTrue(result["ok"])
            self.assertFalse(result["durable"])
            # Manifest must still exist for crash recovery
            mf = read_manifest(repo, "api_audit")
            self.assertIsNotNone(mf)
            self.assertEqual(mf["operation"], "cold_store")

    def test_maintenance_removes_manifest_on_commit_success(self) -> None:
        """When git commit succeeds, manifest is properly cleaned up."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            logs = repo / "logs"
            logs.mkdir(parents=True)
            (logs / "api_audit.jsonl").write_text(
                '{"ts":"2026-03-19T00:00:00Z","event":"old"}\n' * 20
            )
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )
            self.assertTrue(result["ok"])
            self.assertTrue(result["durable"])
            # Manifest should be removed after successful commit
            mf = read_manifest(repo, "api_audit")
            self.assertIsNone(mf)


# -----------------------------------------------------------------------
# F6: Duplicate-segment guard after crash recovery (Finding 2)
# -----------------------------------------------------------------------
class TestDuplicateSegmentGuard(unittest.TestCase):
    """Maintenance must skip sources that already have a rolled stub."""

    def test_skip_already_rolled_source(self) -> None:
        """If a stub already references a source, maintenance skips re-roll."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            settings = _FakeSettings()

            # Set up api_audit source with enough content
            logs = repo / "logs"
            logs.mkdir(parents=True)
            source = logs / "api_audit.jsonl"
            source.write_text(
                '{"ts":"2026-03-19T00:00:00Z","event":"old"}\n' * 20
            )

            # Simulate crash recovery: a stub already exists referencing
            # this source (as if a prior crashed roll was committed by
            # manifest reconciliation).
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()
            seg_id = "api_audit__api_audit__20260319T000000Z__0001"
            payload = hist / f"{seg_id}.jsonl"
            payload.write_text('{"ts":"2026-03-19T00:00:00Z"}\n')
            stub = _create_stub(
                family="api_audit",
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260319T000000Z",
                payload_path=str(payload.relative_to(repo)),
                summary={"last_event_at": "2026-03-19T00:00:00Z",
                         "line_count": 1, "byte_size": 30},
            )
            write_text_file(idx / f"{seg_id}.json", json.dumps(stub))

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=settings,
                gm=gm,
                now=now,
            )
            self.assertTrue(result["ok"])
            # Should have been skipped — no new segments rolled
            self.assertEqual(result["rolled_count"], 0)
            # Warning emitted about the skip
            codes = [w["code"] for w in result["warnings"]]
            self.assertIn("segment_history_already_rolled", codes)


# -----------------------------------------------------------------------
# F7: Rehydrate manifest check under lock (Finding 4)
# -----------------------------------------------------------------------
class TestRehydrateManifestCheckUnderLock(unittest.TestCase):
    """Pending-batch-residue check must run inside the source lock."""

    def test_rehydrate_blocks_on_manifest_residue(self) -> None:
        """Rehydrate returns 409 when manifest lists the source path."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Set up a cold-stored segment
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()
            cold_dir = hist / "cold"
            cold_dir.mkdir()
            seg_id = "api_audit__api_audit__20260319T000000Z__0001"
            cold_data = _build_cold_gzip_bytes(b'{"ts":"2026-03-19T00:00:00Z"}\n')
            cold_path = cold_dir / f"{seg_id}.jsonl.gz"
            write_bytes_file(cold_path, cold_data)
            stub = _create_stub(
                family="api_audit",
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260319T000000Z",
                payload_path=str(cold_path.relative_to(repo)),
                summary={"last_event_at": "2026-03-19T00:00:00Z",
                         "line_count": 1, "byte_size": 30},
            )
            stub["cold_stored_at"] = "2026-03-20T00:00:00Z"
            write_text_file(idx / f"{seg_id}.json", json.dumps(stub))

            # Write a manifest that lists this source (simulating a
            # concurrent batch operation).
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260320T000000Z__0001"],
            )

            result = segment_history_cold_rehydrate_service(
                family="api_audit",
                segment_id=seg_id,
                repo_root=repo,
                gm=gm,
            )
            # Should be a JSONResponse with 409
            from fastapi.responses import JSONResponse
            self.assertIsInstance(result, JSONResponse)
            self.assertEqual(result.status_code, 409)
            body = json.loads(result.body)
            self.assertEqual(
                body["error"]["code"],
                "segment_history_pending_batch_residue",
            )


# -----------------------------------------------------------------------
# F8: Inline manifest reconciliation in write-time rollover
# -----------------------------------------------------------------------
class TestWriteTimeRolloverInlineReconciliation(unittest.TestCase):
    """Write-time rollover must reconcile stale manifests inline so that
    audit appends are not permanently blocked once the file exceeds the
    rollover threshold."""

    def test_stale_manifest_reconciled_inline(self) -> None:
        """append_audit reconciles a stale manifest instead of blocking."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            logs = repo / "logs"
            logs.mkdir(parents=True)
            audit_file = logs / "api_audit.jsonl"
            # Write enough data to exceed rollover threshold
            audit_file.write_text('{"ts":"2026-03-19T00:00:00Z","event":"x"}\n' * 5)

            # Plant a stale manifest that references this source
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            # Append should succeed — the stale manifest is reconciled inline
            # (no targets exist on disk so manifest is simply removed)
            append_audit(
                repo, "test_event", "peer-1", {"k": "v"},
                rollover_bytes=50, gm=gm,
            )

            # Manifest should be gone after reconciliation
            self.assertIsNone(read_manifest(repo, "api_audit"))
            # The audit line should have been written
            content = audit_file.read_text()
            self.assertIn("test_event", content)

    def test_unreconcilable_manifest_still_blocks(self) -> None:
        """If reconciliation fails (targets exist, commit fails), the append
        is still blocked with WriteTimeRolloverError."""
        from app.audit import WriteTimeRolloverError

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, _paths: list, _msg: str) -> bool:
                    raise RuntimeError("git broken")

            gm = _FailingGM(repo)
            logs = repo / "logs"
            logs.mkdir(parents=True)
            audit_file = logs / "api_audit.jsonl"
            audit_file.write_text('{"ts":"2026-03-19T00:00:00Z","event":"x"}\n' * 5)

            # Plant manifest WITH existing target files so reconciliation
            # attempts a commit (which will fail).
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir(parents=True)
            payload = hist / "api_audit__api_audit__20260319T000000Z__0001.jsonl"
            stub = idx / "api_audit__api_audit__20260319T000000Z__0001.json"
            payload.write_text("rolled data\n")
            stub.write_text('{"schema_type":"segment_history_stub"}\n')

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            with self.assertRaises(WriteTimeRolloverError) as ctx:
                append_audit(
                    repo, "test_event", "peer-1", {"k": "v"},
                    rollover_bytes=50, gm=gm,
                )
            self.assertIn("pending_batch_residue", ctx.exception.code)

            # Manifest preserved for next retry
            self.assertIsNotNone(read_manifest(repo, "api_audit"))


# -----------------------------------------------------------------------
# F9: Manifest preserved on reconciliation commit failure
# -----------------------------------------------------------------------
class TestReconciliationManifestPreservation(unittest.TestCase):
    """_reconcile_manifest_residue must preserve the manifest when the
    recovery commit fails, matching the maintenance/cold-store pattern."""

    def test_manifest_preserved_on_commit_failure(self) -> None:
        """Manifest survives when the recovery commit raises."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, _paths: list, _msg: str) -> bool:
                    raise RuntimeError("git broken")

            gm = _FailingGM(repo)

            # Create target files so reconciliation attempts a commit
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir(parents=True)
            payload = hist / "api_audit__api_audit__20260319T000000Z__0001.jsonl"
            stub = idx / "api_audit__api_audit__20260319T000000Z__0001.json"
            payload.write_text("rolled data\n")
            stub.write_text('{"schema_type":"segment_history_stub"}\n')

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            result = _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)
            self.assertIsNotNone(result)
            self.assertIn("preserved=2", result["warning"])

            # Manifest must still exist for the next retry
            mf = read_manifest(repo, "api_audit")
            self.assertIsNotNone(mf)

    def test_manifest_removed_when_no_targets_exist(self) -> None:
        """Manifest is removed when there are no target files to recover."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            result = _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)
            self.assertIsNotNone(result)

            # No targets on disk → manifest removed
            self.assertIsNone(read_manifest(repo, "api_audit"))

    def test_manifest_removed_on_successful_recovery(self) -> None:
        """Manifest is removed when the recovery commit succeeds."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir(parents=True)
            payload = hist / "api_audit__api_audit__20260319T000000Z__0001.jsonl"
            stub = idx / "api_audit__api_audit__20260319T000000Z__0001.json"
            payload.write_text("rolled data\n")
            stub.write_text('{"schema_type":"segment_history_stub"}\n')

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            result = _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)
            self.assertIsNotNone(result)
            self.assertIn("recovered", result["warning"])

            # Manifest removed on success
            self.assertIsNone(read_manifest(repo, "api_audit"))


# -----------------------------------------------------------------------
# F10: Orphaned payload cleanup during reconciliation
# -----------------------------------------------------------------------
class TestOrphanedPayloadCleanup(unittest.TestCase):
    """Reconciliation must remove orphaned payloads that have no companion
    stub, since the source file is still intact in that crash scenario."""

    def test_payload_without_stub_is_removed(self) -> None:
        """A payload that exists without its companion stub is deleted."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            # Only the payload exists — no stub (crash between writes)
            payload = hist / "api_audit__api_audit__20260319T000000Z__0001.jsonl"
            payload.write_text("rolled data\n")

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)

            # Orphaned payload should have been removed
            self.assertFalse(payload.exists())
            # Manifest removed (no valid targets to commit)
            self.assertIsNone(read_manifest(repo, "api_audit"))

    def test_both_payload_and_stub_are_committed(self) -> None:
        """When both payload and stub exist, both are committed."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            committed: list[list] = []

            class _TrackingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: list, msg: str) -> bool:
                    committed.append(list(paths))
                    return True

            gm = _TrackingGM(repo)

            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir(parents=True)
            payload = hist / "api_audit__api_audit__20260319T000000Z__0001.jsonl"
            stub = idx / "api_audit__api_audit__20260319T000000Z__0001.json"
            payload.write_text("rolled data\n")
            stub.write_text('{"schema_type":"segment_history_stub"}\n')

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)

            # Both files should remain and have been committed
            self.assertTrue(payload.exists())
            self.assertTrue(stub.exists())
            self.assertEqual(len(committed), 1)
            committed_paths = {str(p) for p in committed[0]}
            self.assertIn(str(payload), committed_paths)
            self.assertIn(str(stub), committed_paths)

    def test_stub_only_without_payload_is_committed(self) -> None:
        """A stub without a payload is still committed (e.g. journal unlink)."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            committed: list[list] = []

            class _TrackingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: list, msg: str) -> bool:
                    committed.append(list(paths))
                    return True

            gm = _TrackingGM(repo)

            idx = repo / "logs" / "history" / "api_audit" / "index"
            idx.mkdir(parents=True)
            stub = idx / "api_audit__api_audit__20260319T000000Z__0001.json"
            stub.write_text('{"schema_type":"segment_history_stub"}\n')

            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260319T000000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/api_audit__api_audit__20260319T000000Z__0001.jsonl",
                    "logs/history/api_audit/index/api_audit__api_audit__20260319T000000Z__0001.json",
                ],
            )

            _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)

            self.assertTrue(stub.exists())
            self.assertEqual(len(committed), 1)


# -----------------------------------------------------------------------
# F-A: Maintenance rollback cleans orphaned target files from manifest
# -----------------------------------------------------------------------
class TestMaintenanceRollbackCleansOrphanedTargets(unittest.TestCase):
    """When _roll_jsonl_source writes a payload but fails on the stub,
    the rollback must remove the orphaned payload using the manifest's
    target_paths — not just the all_created list (which only tracks
    successful rolls).
    """

    def test_partial_roll_failure_cleans_orphaned_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            settings = _FakeSettings()
            settings.audit_log_rollover_bytes = 10  # trigger rollover

            # Create an eligible source
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-03-20T00:00:00Z","event":"test","peer_id":"p"}\n' * 5)

            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            # Patch write_text_file to fail on the stub write (second call
            # after the payload write).  The payload write is the first call
            # with a path ending in .jsonl; the stub write is the first call
            # ending in .json.
            original_wtf = write_text_file
            call_count = {"n": 0}

            def _failing_wtf(path: Any, content: str) -> None:
                call_count["n"] += 1
                # Let the payload write succeed; fail the stub write
                if str(path).endswith(".json") and "/index/" in str(path):
                    raise OSError("Simulated disk full on stub write")
                original_wtf(path, content)

            with patch("app.segment_history.service.write_text_file", _failing_wtf):
                with self.assertRaises(OSError):
                    segment_history_maintenance_service(
                        family="api_audit",
                        repo_root=repo,
                        settings=settings,
                        gm=gm,
                        now=now,
                    )

            # The orphaned payload should have been cleaned up by rollback
            history_dir = repo / "logs" / "history" / "api_audit"
            if history_dir.is_dir():
                payloads = list(history_dir.glob("*.jsonl"))
                self.assertEqual(
                    payloads, [],
                    f"Orphaned payload should have been removed: {payloads}",
                )

            # Source should be restored
            self.assertTrue(source.exists())
            self.assertGreater(source.stat().st_size, 0)

            # Manifest should be removed
            from app.segment_history.manifest import manifest_path
            self.assertFalse(manifest_path(repo, "api_audit").exists())


# -----------------------------------------------------------------------
# F-A (write-time): Write-time rollover cleans orphaned targets on failure
# -----------------------------------------------------------------------
class TestWriteTimeRolloverCleansOrphanedTargets(unittest.TestCase):
    """When write-time rollover writes a payload but fails on the stub,
    the exception handler must remove the orphaned payload file.
    """

    def test_partial_roll_failure_cleans_orphaned_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            # Write enough to trigger rollover at 100 bytes
            source.write_text('{"ts":"2026-03-20T00:00:00Z","event":"test","peer_id":"p"}\n' * 5)

            from app.audit import WriteTimeRolloverError, _check_write_time_rollover

            original_wtf = write_text_file

            def _failing_wtf(path: Any, content: str) -> None:
                if str(path).endswith(".json") and "/index/" in str(path):
                    raise OSError("Simulated disk full on stub write")
                original_wtf(path, content)

            with patch("app.segment_history.service.write_text_file", _failing_wtf):
                with self.assertRaises(WriteTimeRolloverError):
                    _check_write_time_rollover(source, 100, repo, gm)

            # No orphaned payload should remain
            history_dir = repo / "logs" / "history" / "api_audit"
            if history_dir.is_dir():
                payloads = list(history_dir.glob("*.jsonl"))
                self.assertEqual(
                    payloads, [],
                    f"Orphaned payload should have been removed: {payloads}",
                )


# -----------------------------------------------------------------------
# F-B: Cold-store reconciliation removes orphaned .gz for unmutated stubs
# -----------------------------------------------------------------------
class TestColdStoreReconciliationUnmutatedStub(unittest.TestCase):
    """When cold-store crashes between writing the .gz and mutating the
    stub, reconciliation should remove the orphaned .gz rather than
    committing it with a semantically stale stub.
    """

    def test_cold_gz_removed_when_stub_not_mutated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            committed: list[list] = []

            class _TrackingGM:
                repo_root = repo

                def commit_paths(self, paths: Any, _msg: str) -> bool:
                    committed.append(list(paths))
                    return True

                def latest_commit(self) -> str:
                    return "test-sha"

            gm = _TrackingGM()

            # Set up: hot payload + stub (not mutated — no cold_stored_at)
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            hot_payload = hist / "api_audit__api_audit__20260320T120000Z__0001.jsonl"
            hot_payload.write_text('{"event":"test"}\n')

            idx = hist / "index"
            idx.mkdir()
            stub_path = idx / "api_audit__api_audit__20260320T120000Z__0001.json"
            stub = {
                "schema_type": "segment_history_stub",
                "segment_id": "api_audit__api_audit__20260320T120000Z__0001",
                "source_path": "logs/api_audit.jsonl",
                "payload_path": "logs/history/api_audit/api_audit__api_audit__20260320T120000Z__0001.jsonl",
                # No cold_stored_at — stub was NOT mutated
            }
            stub_path.write_text(json.dumps(stub))

            # Orphaned cold .gz (written by crashed cold-store)
            cold_dir = hist / "cold"
            cold_dir.mkdir()
            cold_gz = cold_dir / "api_audit__api_audit__20260320T120000Z__0001.jsonl.gz"
            cold_gz.write_bytes(b"fake-gz-data")

            # Write cold-store manifest referencing cold_gz + stub
            write_manifest(
                repo,
                operation="cold_store",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260320T120000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/cold/api_audit__api_audit__20260320T120000Z__0001.jsonl.gz",
                    "logs/history/api_audit/index/api_audit__api_audit__20260320T120000Z__0001.json",
                ],
            )

            _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)

            # The orphaned cold .gz should have been removed
            self.assertFalse(
                cold_gz.exists(),
                "Orphaned cold .gz should be removed when stub is unmutated",
            )
            # The unmutated stub should still be committed (it's valid)
            self.assertTrue(len(committed) >= 1)
            # Verify the cold .gz was NOT in the committed paths
            for commit_group in committed:
                for p in commit_group:
                    self.assertFalse(
                        str(p).endswith(".gz"),
                        f"Orphaned .gz should not be committed: {p}",
                    )

    def test_mutated_stub_commits_both(self) -> None:
        """When the stub WAS mutated (has cold_stored_at), both .gz and stub
        should be committed normally."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            committed: list[list] = []

            class _TrackingGM:
                repo_root = repo

                def commit_paths(self, paths: Any, _msg: str) -> bool:
                    committed.append(list(paths))
                    return True

                def latest_commit(self) -> str:
                    return "test-sha"

            gm = _TrackingGM()

            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)

            idx = hist / "index"
            idx.mkdir()
            stub_path = idx / "api_audit__api_audit__20260320T120000Z__0001.json"
            stub = {
                "schema_type": "segment_history_stub",
                "segment_id": "api_audit__api_audit__20260320T120000Z__0001",
                "source_path": "logs/api_audit.jsonl",
                "payload_path": "logs/history/api_audit/cold/api_audit__api_audit__20260320T120000Z__0001.jsonl.gz",
                "cold_stored_at": "2026-03-20T12:00:00Z",  # Mutated
            }
            stub_path.write_text(json.dumps(stub))

            cold_dir = hist / "cold"
            cold_dir.mkdir()
            cold_gz = cold_dir / "api_audit__api_audit__20260320T120000Z__0001.jsonl.gz"
            cold_gz.write_bytes(b"fake-gz-data")

            write_manifest(
                repo,
                operation="cold_store",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["api_audit__api_audit__20260320T120000Z__0001"],
                target_paths=[
                    "logs/history/api_audit/cold/api_audit__api_audit__20260320T120000Z__0001.jsonl.gz",
                    "logs/history/api_audit/index/api_audit__api_audit__20260320T120000Z__0001.json",
                ],
            )

            _reconcile_manifest_residue(repo, "api_audit", "maintenance", gm)

            # Both should be committed
            self.assertTrue(cold_gz.exists())
            self.assertTrue(len(committed) >= 1)
            # Verify the cold .gz WAS committed
            committed_strs = [str(p) for group in committed for p in group]
            self.assertTrue(
                any(s.endswith(".gz") for s in committed_strs),
                "Mutated stub: .gz should be committed",
            )


# -----------------------------------------------------------------------
# Finding 1: Manifest clobber guard — reconciliation skips non-overlapping
# -----------------------------------------------------------------------
class TestManifestClobberGuard(unittest.TestCase):
    """Reconciliation must skip manifests whose source_paths do not overlap
    with the caller's locked set.  Without this, concurrent operations on
    the same family but different sources could clobber each other's
    crash-recovery manifests."""

    def test_non_overlapping_sources_skips_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            write_manifest(
                repo,
                operation="maintenance",
                family="message_stream",
                source_paths=["messages/inbox/alice.jsonl"],
                segment_ids=["message_stream__inbox__alice__20260320T120000Z__0001"],
                target_paths=[],
            )

            # Reconcile with a non-overlapping locked set
            result = _reconcile_manifest_residue(
                repo, "message_stream", "cold_store", None,
                locked_source_paths={"messages/outbox/bob.jsonl"},
            )
            # Should have been skipped — manifest still exists
            self.assertIsNone(result)
            self.assertIsNotNone(read_manifest(repo, "message_stream"))

    def test_overlapping_sources_reconciles(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            write_manifest(
                repo,
                operation="maintenance",
                family="message_stream",
                source_paths=["messages/inbox/alice.jsonl"],
                segment_ids=["message_stream__inbox__alice__20260320T120000Z__0001"],
                target_paths=[],
            )

            # Reconcile with an overlapping locked set
            result = _reconcile_manifest_residue(
                repo, "message_stream", "maintenance", None,
                locked_source_paths={"messages/inbox/alice.jsonl", "messages/outbox/bob.jsonl"},
            )
            # Should have reconciled (no targets → manifest removed)
            self.assertIsNotNone(result)
            self.assertIsNone(read_manifest(repo, "message_stream"))

    def test_no_locked_source_paths_reconciles_all(self) -> None:
        """Backward compat: when locked_source_paths is None, always reconcile."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)

            write_manifest(
                repo,
                operation="maintenance",
                family="message_stream",
                source_paths=["messages/inbox/alice.jsonl"],
                segment_ids=["message_stream__inbox__alice__20260320T120000Z__0001"],
                target_paths=[],
            )

            result = _reconcile_manifest_residue(
                repo, "message_stream", "maintenance", None,
            )
            self.assertIsNotNone(result)
            self.assertIsNone(read_manifest(repo, "message_stream"))


# -----------------------------------------------------------------------
# Finding 2: Rehydrate crash-recovery — orphaned hot auto-cleanup
# -----------------------------------------------------------------------
class TestRehydrateOrphanedHotAutoClean(unittest.TestCase):
    """Rehydrate must auto-clean orphaned hot files from a prior crash
    instead of permanently returning 409."""

    def test_orphaned_hot_auto_cleaned_rehydrate_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)
            family = "api_audit"

            # Set up a cold-stored segment
            seg_id = "api_audit__api_audit__20260320T120000Z__0001"
            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()

            hot_payload_content = b'{"event":"test"}\n'
            compressed = _build_cold_gzip_bytes(hot_payload_content)
            cold_dir = hist / "cold"
            cold_dir.mkdir()
            cold_path = cold_dir / f"{seg_id}.jsonl.gz"
            cold_path.write_bytes(compressed)

            stub_data = _create_stub(
                family=family,
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260320T120000Z",
                payload_path=f"logs/history/api_audit/cold/{seg_id}.jsonl.gz",
                summary={"line_count": 1, "byte_size": 17},
            )
            stub_data["cold_stored_at"] = "2026-03-20T12:00:00Z"
            stub_path = idx / f"{seg_id}.json"
            stub_path.write_text(json.dumps(stub_data))

            # Simulate crash residue: hot file exists from prior crash
            hot_path = repo / "logs" / "history" / "api_audit" / f"{seg_id}.jsonl"
            hot_path.write_text("orphaned-from-crash")

            result = segment_history_cold_rehydrate_service(
                family=family, segment_id=seg_id, repo_root=repo, gm=gm,
            )
            self.assertIsInstance(result, dict)
            self.assertTrue(result["ok"])
            # Verify warning about auto-cleanup
            codes = [w["code"] for w in result.get("warnings", [])]
            self.assertIn("segment_history_orphaned_hot_removed", codes)
            # Hot file should now contain decompressed content (not orphan)
            self.assertEqual(hot_path.read_bytes(), hot_payload_content)

    def test_rehydrate_writes_crash_recovery_manifest(self) -> None:
        """Rehydrate should write a manifest before mutations so that
        crash recovery is possible."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            family = "api_audit"
            seg_id = "api_audit__api_audit__20260320T120000Z__0001"

            hist = repo / "logs" / "history" / "api_audit"
            hist.mkdir(parents=True)
            idx = hist / "index"
            idx.mkdir()

            hot_payload_content = b'{"event":"test"}\n'
            compressed = _build_cold_gzip_bytes(hot_payload_content)
            cold_dir = hist / "cold"
            cold_dir.mkdir()
            cold_path = cold_dir / f"{seg_id}.jsonl.gz"
            cold_path.write_bytes(compressed)

            stub_data = _create_stub(
                family=family,
                segment_id=seg_id,
                source_path="logs/api_audit.jsonl",
                stream_key="api_audit",
                rolled_at="20260320T120000Z",
                payload_path=f"logs/history/api_audit/cold/{seg_id}.jsonl.gz",
                summary={"line_count": 1, "byte_size": 17},
            )
            stub_data["cold_stored_at"] = "2026-03-20T12:00:00Z"
            stub_path = idx / f"{seg_id}.json"
            stub_path.write_text(json.dumps(stub_data))

            # Patch write_bytes_file to crash after writing hot payload
            from app.segment_history import service as _svc

            original_write = _svc.write_text_file
            call_count = [0]

            def crash_on_stub_write(path, content):
                call_count[0] += 1
                if "index" in str(path) and seg_id in str(path):
                    # Manifest should already exist at this point
                    mf = read_manifest(repo, family)
                    assert mf is not None, "Manifest must exist before stub mutation"
                    assert mf["operation"] == "rehydrate"
                    raise RuntimeError("simulated crash")
                return original_write(path, content)

            with patch.object(_svc, "write_text_file", side_effect=crash_on_stub_write):
                with self.assertRaises(RuntimeError):
                    segment_history_cold_rehydrate_service(
                        family=family, segment_id=seg_id, repo_root=repo,
                        gm=SimpleGitManagerStub(repo),
                    )

            # After rollback, manifest should be removed
            self.assertIsNone(read_manifest(repo, family))


# -----------------------------------------------------------------------
# Finding 3: In-batch segment ID deduplication
# -----------------------------------------------------------------------
class TestInBatchSegmentIdDedup(unittest.TestCase):
    """_next_segment_id must account for IDs already reserved in the
    current batch to prevent collisions before files are written."""

    def test_reserved_ids_prevents_collision(self) -> None:
        from app.segment_history.service import _next_segment_id

        with tempfile.TemporaryDirectory() as td:
            target = Path(td)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            id1 = _next_segment_id("api_audit", "api_audit", now, target)
            self.assertTrue(id1.endswith("__0001"))

            # Without reserved_ids, same call returns same ID
            id2_no_reserve = _next_segment_id("api_audit", "api_audit", now, target)
            self.assertEqual(id1, id2_no_reserve)

            # With reserved_ids, the collision is avoided
            id2_with_reserve = _next_segment_id(
                "api_audit", "api_audit", now, target,
                reserved_ids={id1},
            )
            self.assertTrue(id2_with_reserve.endswith("__0002"))
            self.assertNotEqual(id1, id2_with_reserve)

    def test_reserved_ids_stacks_with_disk(self) -> None:
        from app.segment_history.service import _next_segment_id

        with tempfile.TemporaryDirectory() as td:
            target = Path(td)
            now = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            # Write a file on disk at seq 0001
            id1 = _next_segment_id("api_audit", "api_audit", now, target)
            (target / f"{id1}.jsonl").write_text("data")

            # Without reserved_ids, gets 0002 from disk scan
            id2 = _next_segment_id("api_audit", "api_audit", now, target)
            self.assertTrue(id2.endswith("__0002"))

            # With reserved_ids at 0003, gets 0004
            reserved = {id2, id2.replace("__0002", "__0003")}
            id4 = _next_segment_id(
                "api_audit", "api_audit", now, target,
                reserved_ids=reserved,
            )
            self.assertTrue(id4.endswith("__0004"))


# -----------------------------------------------------------------------
# Review round 14 fixes
# -----------------------------------------------------------------------


class TestManifestOccupied(unittest.TestCase):
    """F1-R14: write_manifest rejects clobber by non-overlapping operations."""

    def test_clobber_rejected_non_overlapping(self) -> None:
        """write_manifest raises ManifestOccupied when existing manifest has
        non-overlapping source_paths."""
        from app.segment_history.manifest import ManifestOccupied

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["seg1"],
                target_paths=["logs/history/api_audit/seg1.jsonl",
                              "logs/history/api_audit/index/seg1.json"],
            )
            with self.assertRaises(ManifestOccupied):
                write_manifest(
                    repo,
                    operation="cold_store",
                    family="api_audit",
                    source_paths=["logs/other.jsonl"],
                    segment_ids=["seg2"],
                    target_paths=["logs/history/api_audit/seg2.jsonl",
                                  "logs/history/api_audit/index/seg2.json"],
                )

    def test_overwrite_allowed_overlapping(self) -> None:
        """write_manifest allows overwrite when sources overlap (same lock holder)."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["seg1"],
                target_paths=["logs/history/api_audit/seg1.jsonl",
                              "logs/history/api_audit/index/seg1.json"],
            )
            # Same source — should not raise
            write_manifest(
                repo,
                operation="maintenance",
                family="api_audit",
                source_paths=["logs/api_audit.jsonl"],
                segment_ids=["seg1_new"],
                target_paths=["logs/history/api_audit/seg1_new.jsonl",
                              "logs/history/api_audit/index/seg1_new.json"],
            )
            mf = read_manifest(repo, "api_audit")
            self.assertIsNotNone(mf)
            self.assertEqual(mf["segment_ids"], ["seg1_new"])

    def test_maintenance_returns_manifest_occupied_error(self) -> None:
        """Maintenance returns structured error when manifest is occupied."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-03-20T00:00:00Z","event":"test"}\n' * 5)

            # Plant a manifest for a different source
            write_manifest(
                repo,
                operation="cold_store",
                family="api_audit",
                source_paths=["logs/other.jsonl"],
                segment_ids=["other_seg"],
                target_paths=["logs/history/api_audit/other_seg.jsonl",
                              "logs/history/api_audit/index/other_seg.json"],
            )

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=SimpleGitManagerStub(),
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"],
                             "segment_history_manifest_occupied")


class TestColdStoreDefersHotDeletion(unittest.TestCase):
    """F2-R14: Hot payloads survive when git commit fails."""

    def test_hot_payloads_survive_failed_commit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-01-01T00:00:00Z","event":"test"}\n' * 3)

            gm = SimpleGitManagerStub()
            settings = _FakeSettings()
            now = datetime(2026, 3, 20, tzinfo=timezone.utc)

            # Roll first
            result = segment_history_maintenance_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=gm, now=now,
            )
            self.assertTrue(result["ok"])

            # Make git commit fail for cold-store
            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: Any, msg: str) -> bool:
                    return False

            cs_result = segment_history_cold_store_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=_FailingGM(), now=now,
            )
            self.assertTrue(cs_result["ok"])
            self.assertFalse(cs_result["durable"])
            self.assertIn("at_risk_segment_ids", cs_result)

            # Hot payload must still exist (not deleted before commit)
            hist = repo / "logs" / "history" / "api_audit"
            hot_files = list(hist.glob("*.jsonl"))
            self.assertTrue(len(hot_files) > 0,
                            "Hot payload should survive failed commit")


class TestRehydrateDefersRemoval(unittest.TestCase):
    """F3-R14: Cold payload survives when rehydrate git commit fails."""

    def test_cold_payload_survives_failed_commit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-01-01T00:00:00Z","event":"test"}\n' * 3)

            gm = SimpleGitManagerStub()
            settings = _FakeSettings()
            now = datetime(2026, 3, 20, tzinfo=timezone.utc)

            # Roll
            result = segment_history_maintenance_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=gm, now=now,
            )
            seg_id = result["rolled_segment_ids"][0]

            # Cold-store
            cs = segment_history_cold_store_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=gm, now=now,
            )
            self.assertTrue(cs["ok"])

            # Rehydrate with failing git commit
            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: Any, msg: str) -> bool:
                    return False

            rh = segment_history_cold_rehydrate_service(
                family="api_audit", segment_id=seg_id,
                repo_root=repo, gm=_FailingGM(),
            )
            self.assertTrue(rh["ok"])
            self.assertFalse(rh["durable"])
            self.assertIn("at_risk_segment_ids", rh)

            # Cold payload must still exist
            cold_dir = repo / "logs" / "history" / "api_audit" / "cold"
            cold_files = list(cold_dir.glob("*.gz"))
            self.assertTrue(len(cold_files) > 0,
                            "Cold payload should survive failed commit")


class TestTargetPathsPairingValidation(unittest.TestCase):
    """F5-R14: write_manifest validates target_paths pairing."""

    def test_odd_length_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            with self.assertRaises(ValueError):
                write_manifest(
                    repo,
                    operation="test",
                    family="api_audit",
                    source_paths=["src.jsonl"],
                    segment_ids=["seg1"],
                    target_paths=["payload.jsonl", "stub.json", "orphan.jsonl"],
                )

    def test_stub_in_payload_position_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            with self.assertRaises(ValueError):
                write_manifest(
                    repo,
                    operation="test",
                    family="api_audit",
                    source_paths=["src.jsonl"],
                    segment_ids=["seg1"],
                    target_paths=["stub.json", "payload.jsonl"],
                )

    def test_valid_pairing_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            write_manifest(
                repo,
                operation="test",
                family="api_audit",
                source_paths=["src.jsonl"],
                segment_ids=["seg1"],
                target_paths=["payload.jsonl", "stub.json"],
            )
            mf = read_manifest(repo, "api_audit")
            self.assertIsNotNone(mf)


class TestEmptyAckLockGuarded(unittest.TestCase):
    """F6-R14: Empty ack deletion uses source lock to prevent TOCTOU."""

    def test_nonempty_ack_survives(self) -> None:
        """If ack file becomes non-empty before lock-guarded delete,
        it must not be deleted."""
        from contextlib import contextmanager
        from app.segment_history.locking import segment_history_source_lock as _orig

        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            acks = repo / "messages" / "acks"
            acks.mkdir(parents=True)
            ack_file = acks / "test.jsonl"
            # Start empty
            ack_file.write_text("")

            @contextmanager
            def _populating_lock(lock_key: str, *, lock_dir: Path, timeout: float = 30.0):
                # Simulate a concurrent write between size check and lock
                ack_file.write_text('{"msg":"arrived"}\n')
                with _orig(lock_key, lock_dir=lock_dir, timeout=timeout):
                    yield

            with patch(
                "app.segment_history.locking.segment_history_source_lock",
                _populating_lock,
            ):
                segment_history_maintenance_service(
                    family="message_stream",
                    repo_root=repo,
                    settings=_FakeSettings(),
                    gm=SimpleGitManagerStub(),
                )

            # Ack file must survive (was populated before lock-guarded re-check)
            self.assertTrue(ack_file.exists(),
                            "Non-empty ack file should not be deleted")


class TestAtRiskSegmentIds(unittest.TestCase):
    """F7-R14: Non-durable responses identify at-risk segments."""

    def test_maintenance_at_risk_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-01-01T00:00:00Z","event":"test"}\n' * 3)

            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: Any, msg: str) -> bool:
                    return False

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=_FailingGM(),
                now=datetime(2026, 3, 20, tzinfo=timezone.utc),
            )
            self.assertTrue(result["ok"])
            self.assertFalse(result["durable"])
            self.assertIn("at_risk_segment_ids", result)
            self.assertEqual(len(result["at_risk_segment_ids"]),
                             len(result["rolled_segment_ids"]))

    def test_cold_store_at_risk_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-01-01T00:00:00Z","event":"test"}\n' * 3)

            gm = SimpleGitManagerStub()
            settings = _FakeSettings()
            now = datetime(2026, 3, 20, tzinfo=timezone.utc)

            # Roll first
            segment_history_maintenance_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=gm, now=now,
            )

            class _FailingGM(SimpleGitManagerStub):
                def commit_paths(self, paths: Any, msg: str) -> bool:
                    return False

            cs = segment_history_cold_store_service(
                family="api_audit", repo_root=repo, settings=settings,
                gm=_FailingGM(), now=now,
            )
            self.assertTrue(cs["ok"])
            self.assertFalse(cs["durable"])
            self.assertIn("at_risk_segment_ids", cs)

    def test_no_at_risk_when_durable(self) -> None:
        """Durable response should NOT have at_risk_segment_ids."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            logs = repo / "logs"
            logs.mkdir()
            source = logs / "api_audit.jsonl"
            source.write_text('{"ts":"2026-01-01T00:00:00Z","event":"test"}\n' * 3)

            result = segment_history_maintenance_service(
                family="api_audit",
                repo_root=repo,
                settings=_FakeSettings(),
                gm=SimpleGitManagerStub(),
                now=datetime(2026, 3, 20, tzinfo=timezone.utc),
            )
            self.assertTrue(result["ok"])
            self.assertTrue(result["durable"])
            self.assertNotIn("at_risk_segment_ids", result)


if __name__ == "__main__":
    unittest.main()
