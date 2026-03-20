"""Tests for segment-history restore-test validation (issue #114, Phase 10)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.maintenance.service import _validate_segment_history


class TestValidateSegmentHistory(unittest.TestCase):
    def test_empty_restore_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = _validate_segment_history(Path(td))
            self.assertTrue(result["ok"])
            self.assertEqual(result["total_stubs"], 0)

    def test_valid_hot_stub_and_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            # Journal uses year-based index dirs
            stub_dir = restore / "journal" / "history" / "2026" / "index"
            stub_dir.mkdir(parents=True)
            history_dir = restore / "journal" / "history" / "2026"

            # Create a hot payload (.md for journal)
            payload = history_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.md"
            payload.write_text("entry\n")

            # Create a valid stub (new schema: no cold_stored_at when hot, has created_at)
            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "journal",
                "segment_id": "journal__2026__2026-03-19__20260320T000000Z__0001",
                "source_path": "journal/2026/2026-03-19.md",
                "stream_key": "2026__2026-03-19",
                "rolled_at": "20260320T000000Z",
                "created_at": "20260320T000000Z",
                "payload_path": "journal/history/2026/journal__2026__2026-03-19__20260320T000000Z__0001.md",
                "summary": {"day": "2026-03-19"},
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertTrue(result["ok"])
            self.assertEqual(result["total_stubs"], 1)
            self.assertEqual(result["hot_payloads"], 1)

    def test_missing_hot_payload_reported(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "journal" / "history" / "2026" / "index"
            stub_dir.mkdir(parents=True)

            # Stub points to non-existent payload
            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "journal",
                "segment_id": "journal__2026__2026-03-19__20260320T000000Z__0001",
                "source_path": "journal/2026/2026-03-19.md",
                "stream_key": "2026__2026-03-19",
                "rolled_at": "20260320T000000Z",
                "created_at": "20260320T000000Z",
                "payload_path": "journal/history/2026/journal__2026__2026-03-19__20260320T000000Z__0001.md",
                "summary": {},
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["missing_hot_payloads"]), 1)

    def test_cold_stub_with_missing_cold_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "journal" / "history" / "2026" / "index"
            stub_dir.mkdir(parents=True)

            # Cold stub: payload_path points to cold location, cold_stored_at present
            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "journal",
                "segment_id": "journal__2026__2026-03-19__20260320T000000Z__0001",
                "source_path": "journal/2026/2026-03-19.md",
                "stream_key": "2026__2026-03-19",
                "rolled_at": "20260320T000000Z",
                "created_at": "20260320T000000Z",
                "payload_path": "journal/history/2026/cold/journal__2026__2026-03-19__20260320T000000Z__0001.md.gz",
                "summary": {},
                "cold_stored_at": "2026-03-21T00:00:00Z",
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["missing_cold_payloads"]), 1)

    def test_invalid_stub_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            # Use api_audit index dir for non-journal test
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)
            (stub_dir / "bad.json").write_text("not json")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_stubs"]), 1)

    def test_wrong_family_stub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            # Place api_audit stub in api_audit index dir but with wrong family
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "journal",  # Wrong family for this dir
                "segment_id": "journal__20260320T000000Z__0001",
                "source_path": "journal/2026/2026-03-19.md",
                "stream_key": "2026__2026-03-19",
                "rolled_at": "20260320T000000Z",
                "created_at": "20260320T000000Z",
                "payload_path": "journal/history/2026/journal__20260320T000000Z__0001.md",
                "summary": {},
            }
            (stub_dir / "wrong.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            self.assertGreater(len(result["invalid_stubs"]), 0)


if __name__ == "__main__":
    unittest.main()
