"""Tests for segment-history restore-test validation (issue #114, Phase 10)."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from app.maintenance.service import _validate_segment_history


def _family_result(result: dict, family: str) -> dict | None:
    """Extract the per-family row from the validation result."""
    for f in result.get("families", []):
        if f["family"] == family:
            return f
    return None


class TestValidateSegmentHistory(unittest.TestCase):
    def test_empty_restore_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = _validate_segment_history(Path(td))
            self.assertTrue(result["ok"])
            self.assertEqual(result["families_checked"], 6)
            for fam in result["families"]:
                self.assertEqual(fam["hot_stubs_checked"], 0)
                self.assertEqual(fam["cold_stubs_checked"], 0)

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
                "summary": {"day": "2026-03-19", "line_count": 1, "byte_size": 6},
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertTrue(result["ok"])
            journal = _family_result(result, "journal")
            self.assertIsNotNone(journal)
            self.assertEqual(journal["hot_stubs_checked"], 1)
            self.assertEqual(journal["rolled_payloads_checked"], 1)

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
                "summary": {"day": "2026-03-19"},
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            journal = _family_result(result, "journal")
            codes = [w["code"] for w in journal["warnings"]]
            self.assertIn("segment_history_missing_hot_payload", codes)

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
                "summary": {"day": "2026-03-19"},
                "cold_stored_at": "2026-03-21T00:00:00Z",
            }
            (stub_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.json").write_text(
                json.dumps(stub), encoding="utf-8"
            )

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            journal = _family_result(result, "journal")
            codes = [w["code"] for w in journal["warnings"]]
            self.assertIn("segment_history_missing_cold_payload", codes)

    def test_invalid_stub_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            # Use api_audit index dir for non-journal test
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)
            (stub_dir / "bad.json").write_text("not json")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_invalid_stub", codes)

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
            api = _family_result(result, "api_audit")
            self.assertGreater(len(api["warnings"]), 0)

    def test_stub_missing_segment_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "source_path": "logs/api_audit.jsonl",
                "payload_path": "logs/history/api_audit/x.jsonl",
                "summary": {},
            }
            (stub_dir / "no_id.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_stub_missing_segment_id", codes)

    def test_stub_missing_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "segment_id": "api_audit__api_audit__20260320T120000Z__0001",
                "payload_path": "logs/history/api_audit/x.jsonl",
                "summary": {},
            }
            (stub_dir / "no_src.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_stub_missing_source_path", codes)

    def test_stub_missing_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "segment_id": "api_audit__api_audit__20260320T120000Z__0001",
                "source_path": "logs/api_audit.jsonl",
                "payload_path": "logs/history/api_audit/x.jsonl",
            }
            (stub_dir / "no_summary.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_stub_missing_summary", codes)

    def test_cold_payload_decompression_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)
            cold_dir = restore / "logs" / "history" / "api_audit" / "cold"
            cold_dir.mkdir(parents=True)

            seg_id = "api_audit__api_audit__20260320T120000Z__0001"
            cold_payload = cold_dir / f"{seg_id}.jsonl.gz"
            cold_payload.write_bytes(b"not-gzip-data")

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "segment_id": seg_id,
                "source_path": "logs/api_audit.jsonl",
                "payload_path": f"logs/history/api_audit/cold/{seg_id}.jsonl.gz",
                "summary": {"byte_size": 100},
                "cold_stored_at": "2026-03-21T00:00:00Z",
            }
            (stub_dir / f"{seg_id}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_cold_payload_corrupt", codes)

    def test_cold_payload_byte_size_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)
            cold_dir = restore / "logs" / "history" / "api_audit" / "cold"
            cold_dir.mkdir(parents=True)

            seg_id = "api_audit__api_audit__20260320T120000Z__0001"
            original = b'{"ts":"2026-03-20T12:00:00Z","event":"test"}\n'
            cold_payload = cold_dir / f"{seg_id}.jsonl.gz"
            cold_payload.write_bytes(gzip.compress(original))

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "segment_id": seg_id,
                "source_path": "logs/api_audit.jsonl",
                "payload_path": f"logs/history/api_audit/cold/{seg_id}.jsonl.gz",
                "summary": {"byte_size": 9999},  # Wrong size
                "cold_stored_at": "2026-03-21T00:00:00Z",
            }
            (stub_dir / f"{seg_id}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertFalse(result["ok"])
            api = _family_result(result, "api_audit")
            codes = [w["code"] for w in api["warnings"]]
            self.assertIn("segment_history_cold_byte_size_mismatch", codes)

    def test_valid_cold_payload_passes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restore = Path(td)
            stub_dir = restore / "logs" / "history" / "api_audit" / "index"
            stub_dir.mkdir(parents=True)
            cold_dir = restore / "logs" / "history" / "api_audit" / "cold"
            cold_dir.mkdir(parents=True)

            seg_id = "api_audit__api_audit__20260320T120000Z__0001"
            original = b'{"ts":"2026-03-20T12:00:00Z","event":"test"}\n'
            cold_payload = cold_dir / f"{seg_id}.jsonl.gz"
            cold_payload.write_bytes(gzip.compress(original))

            stub = {
                "schema_type": "segment_history_stub",
                "schema_version": "1.0",
                "family": "api_audit",
                "segment_id": seg_id,
                "source_path": "logs/api_audit.jsonl",
                "payload_path": f"logs/history/api_audit/cold/{seg_id}.jsonl.gz",
                "summary": {"byte_size": len(original)},
                "cold_stored_at": "2026-03-21T00:00:00Z",
            }
            (stub_dir / f"{seg_id}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_segment_history(restore)
            self.assertTrue(result["ok"])
            api = _family_result(result, "api_audit")
            self.assertEqual(api["cold_stubs_checked"], 1)
            self.assertEqual(api["cold_payloads_checked"], 1)

    def test_response_shape_per_family(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = _validate_segment_history(Path(td))
            self.assertIn("families_checked", result)
            self.assertIn("families", result)
            self.assertEqual(len(result["families"]), 6)
            for fam in result["families"]:
                self.assertIn("family", fam)
                self.assertIn("active_sources_checked", fam)
                self.assertIn("hot_stubs_checked", fam)
                self.assertIn("cold_stubs_checked", fam)
                self.assertIn("rolled_payloads_checked", fam)
                self.assertIn("cold_payloads_checked", fam)
                self.assertIn("warnings", fam)


if __name__ == "__main__":
    unittest.main()
