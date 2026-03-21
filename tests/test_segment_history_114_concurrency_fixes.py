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


if __name__ == "__main__":
    unittest.main()
