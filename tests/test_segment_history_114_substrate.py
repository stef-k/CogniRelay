"""Tests for segment-history shared substrate (issue #114, Phase 3)."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.segment_history.service import (
    _build_cold_gzip_bytes,
    _capture_rollback_state,
    _cold_payload_path,
    _count_lines,
    _create_stub,
    _decompress_cold_payload,
    _derive_stream_key,
    _first_last_json_field,
    _first_nonempty_line_preview,
    _json_field_counts,
    _mutate_stub_cold,
    _mutate_stub_rehydrate,
    _next_segment_id,
    _rehydrate_target_path,
    _remove_created_paths,
    _restore_rollback_state,
    _roll_jsonl_source,
    _roll_journal_source,
    _sample_json_field,
    _segment_timestamp_str,
    _validate_segment_id,
)


class TestSegmentTimestamp(unittest.TestCase):
    def test_format(self) -> None:
        dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(_segment_timestamp_str(dt), "20260320T120000Z")


class TestDeriveStreamKey(unittest.TestCase):
    def test_journal(self) -> None:
        # journal/2026/2026-03-19.md -> strip "journal/", remove ext, replace /
        key = _derive_stream_key("journal", "journal/2026/2026-03-19.md")
        self.assertEqual(key, "2026__2026-03-19")

    def test_api_audit(self) -> None:
        # logs/api_audit.jsonl -> strip "logs/", remove ext
        key = _derive_stream_key("api_audit", "logs/api_audit.jsonl")
        self.assertEqual(key, "api_audit")

    def test_message_stream(self) -> None:
        key = _derive_stream_key("message_stream", "messages/inbox/alice.jsonl")
        self.assertEqual(key, "inbox__alice")

    def test_episodic(self) -> None:
        key = _derive_stream_key("episodic", "memory/episodic/observations.jsonl")
        self.assertEqual(key, "observations")

    def test_message_thread(self) -> None:
        key = _derive_stream_key("message_thread", "messages/threads/t1.jsonl")
        self.assertEqual(key, "t1")


class TestNextSegmentId(unittest.TestCase):
    def test_first_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "history"
            target.mkdir()
            dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            # stream_key is now embedded in the segment ID
            sid = _next_segment_id("journal", "2026__2026-03-19", dt, target)
            self.assertEqual(sid, "journal__2026__2026-03-19__20260320T120000Z__0001")

    def test_increments_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "history"
            target.mkdir()
            dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            # Create an existing segment file with the new 4-component format
            (target / "journal__2026__2026-03-19__20260320T120000Z__0001.md").write_text("data")
            sid = _next_segment_id("journal", "2026__2026-03-19", dt, target)
            self.assertEqual(sid, "journal__2026__2026-03-19__20260320T120000Z__0002")


class TestValidateSegmentId(unittest.TestCase):
    def test_valid_with_stream_key(self) -> None:
        result = _validate_segment_id(
            "journal", "journal__2026__2026-03-19__20260320T120000Z__0001"
        )
        self.assertIsNotNone(result)
        family, stream_key, ts, seq = result
        self.assertEqual(family, "journal")
        self.assertEqual(stream_key, "2026__2026-03-19")
        self.assertEqual(ts, "20260320T120000Z")
        self.assertEqual(seq, 1)

    def test_invalid_without_stream_key(self) -> None:
        # Segment ID with no stream_key component is rejected
        result = _validate_segment_id("journal", "journal__20260320T120000Z__0001")
        self.assertIsNone(result)

    def test_wrong_family(self) -> None:
        result = _validate_segment_id("api_audit", "journal__20260320T120000Z__0001")
        self.assertIsNone(result)

    def test_invalid_format(self) -> None:
        result = _validate_segment_id("journal", "not-a-segment-id")
        self.assertIsNone(result)


class TestStubOperations(unittest.TestCase):
    def _make_stub(self) -> dict:
        return _create_stub(
            family="api_audit",
            segment_id="api_audit__api_audit__20260320T120000Z__0001",
            source_path="logs/api_audit.jsonl",
            stream_key="api_audit",
            rolled_at="20260320T120000Z",
            payload_path="logs/history/api_audit/api_audit__api_audit__20260320T120000Z__0001.jsonl",
            summary={"line_count": 100, "byte_size": 50000},
        )

    def test_create_stub_schema(self) -> None:
        stub = self._make_stub()
        self.assertEqual(stub["schema_type"], "segment_history_stub")
        self.assertEqual(stub["schema_version"], "1.0")
        self.assertIn("created_at", stub)
        # cold_stored_at should be absent (key not present) while hot
        self.assertNotIn("cold_stored_at", stub)

    def test_mutate_cold(self) -> None:
        stub = self._make_stub()
        cold = _mutate_stub_cold(stub, "logs/history/api_audit/cold/x.jsonl.gz", "2026-03-21T00:00:00Z")
        self.assertEqual(cold["cold_stored_at"], "2026-03-21T00:00:00Z")
        # payload_path should now point to cold location
        self.assertEqual(cold["payload_path"], "logs/history/api_audit/cold/x.jsonl.gz")
        # Original unchanged
        self.assertNotIn("cold_stored_at", stub)

    def test_mutate_rehydrate(self) -> None:
        stub = self._make_stub()
        cold = _mutate_stub_cold(stub, "cold/x.gz", "2026-03-21T00:00:00Z")
        rehydrated = _mutate_stub_rehydrate(cold, "hot/x.jsonl")
        # cold_stored_at should be removed (key absent)
        self.assertNotIn("cold_stored_at", rehydrated)
        self.assertEqual(rehydrated["payload_path"], "hot/x.jsonl")


class TestGzipPrimitives(unittest.TestCase):
    def test_round_trip(self) -> None:
        original = b"line 1\nline 2\nline 3\n"
        compressed = _build_cold_gzip_bytes(original)
        self.assertGreater(len(compressed), 0)
        decompressed = _decompress_cold_payload(compressed)
        self.assertEqual(decompressed, original)

    def test_corrupt_raises(self) -> None:
        with self.assertRaises(ValueError):
            _decompress_cold_payload(b"not valid gzip data")


class TestRollJsonlSource(unittest.TestCase):
    def test_roll_with_carry_forward(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            source = repo / "logs" / "api_audit.jsonl"
            source.parent.mkdir(parents=True)
            # Three complete lines + one partial
            source.write_text('{"a":1}\n{"a":2}\n{"a":3}\npartial', encoding="utf-8")

            payload_dir = repo / "logs" / "history" / "api_audit"
            stub_dir = repo / "logs" / "history" / "api_audit" / "index"
            payload_path = payload_dir / "api_audit__api_audit__20260320T120000Z__0001.jsonl"
            dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            stub, paths = _roll_jsonl_source(
                source_path=source,
                payload_path=payload_path,
                family="api_audit",
                segment_id="api_audit__api_audit__20260320T120000Z__0001",
                stream_key="api_audit",
                rolled_at=dt,
                stub_dir=stub_dir,
                summary={"line_count": 3},
                repo_root=repo,
            )

            # Payload contains complete lines
            payload_content = payload_path.read_text(encoding="utf-8")
            self.assertIn('{"a":1}', payload_content)
            self.assertNotIn("partial", payload_content)

            # Source now contains only the carry-forward
            source_content = source.read_text(encoding="utf-8")
            self.assertEqual(source_content, "partial")

            # Stub written
            self.assertEqual(stub["family"], "api_audit")
            self.assertEqual(len(paths), 2)

    def test_roll_complete_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            source = repo / "logs" / "test.jsonl"
            source.parent.mkdir(parents=True)
            source.write_text('{"x":1}\n{"x":2}\n', encoding="utf-8")

            payload_dir = repo / "logs" / "history" / "test"
            stub_dir = payload_dir / "index"
            payload_path = payload_dir / "test__test__20260320T120000Z__0001.jsonl"
            dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

            _roll_jsonl_source(
                source_path=source,
                payload_path=payload_path,
                family="test",
                segment_id="test__test__20260320T120000Z__0001",
                stream_key="test",
                rolled_at=dt,
                stub_dir=stub_dir,
                summary={},
                repo_root=repo,
            )

            # Source replaced with empty
            self.assertEqual(source.read_text(encoding="utf-8"), "")


class TestRollJournalSource(unittest.TestCase):
    def test_roll_preserves_source_for_deferred_deletion(self) -> None:
        """Journal source is NOT deleted by _roll_journal_source — caller defers deletion."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            source = repo / "journal" / "2026" / "2026-03-19.md"
            source.parent.mkdir(parents=True)
            source.write_text("entry 1\nentry 2\n", encoding="utf-8")

            payload_dir = repo / "journal" / "history" / "2026"
            stub_dir = payload_dir / "index"
            payload_path = payload_dir / "journal__2026__2026-03-19__20260320T000000Z__0001.md"
            dt = datetime(2026, 3, 20, 0, 0, 0, tzinfo=timezone.utc)

            stub, paths = _roll_journal_source(
                source_path=source,
                payload_path=payload_path,
                family="journal",
                segment_id="journal__2026__2026-03-19__20260320T000000Z__0001",
                stream_key="2026__2026-03-19",
                rolled_at=dt,
                stub_dir=stub_dir,
                summary={"day": "2026-03-19"},
                repo_root=repo,
            )

            # Source preserved for deferred deletion after commit
            self.assertTrue(source.exists())
            # Payload preserved exact bytes
            self.assertEqual(payload_path.read_text(encoding="utf-8"), "entry 1\nentry 2\n")
            self.assertEqual(stub["family"], "journal")


class TestRollbackHelpers(unittest.TestCase):
    def test_capture_and_restore(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "test.txt"
            f.write_text("original", encoding="utf-8")
            state = _capture_rollback_state([f])
            f.write_text("modified", encoding="utf-8")
            _restore_rollback_state(state)
            self.assertEqual(f.read_text(encoding="utf-8"), "original")

    def test_capture_nonexistent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "missing.txt"
            state = _capture_rollback_state([f])
            self.assertEqual(state[0][1], None)

    def test_remove_created_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "created.txt"
            f.write_text("data", encoding="utf-8")
            _remove_created_paths([f])
            self.assertFalse(f.exists())


class TestColdPayloadPaths(unittest.TestCase):
    def test_cold_path(self) -> None:
        hot = Path("/repo/logs/history/api_audit/api_audit__api_audit__20260320T120000Z__0001.jsonl")
        cold = _cold_payload_path(hot)
        self.assertEqual(cold.name, "api_audit__api_audit__20260320T120000Z__0001.jsonl.gz")
        self.assertEqual(cold.parent.name, "cold")

    def test_rehydrate_path(self) -> None:
        cold = Path("/repo/logs/history/api_audit/cold/api_audit__api_audit__20260320T120000Z__0001.jsonl.gz")
        hot = _rehydrate_target_path(cold)
        self.assertEqual(hot.name, "api_audit__api_audit__20260320T120000Z__0001.jsonl")
        self.assertNotEqual(hot.parent.name, "cold")


class TestSummaryHelpers(unittest.TestCase):
    def test_count_lines(self) -> None:
        # _count_lines counts newline-terminated lines (number of \n chars)
        self.assertEqual(_count_lines("a\nb\nc\n"), 3)
        self.assertEqual(_count_lines(""), 0)
        self.assertEqual(_count_lines("\n\n"), 2)

    def test_first_nonempty_line(self) -> None:
        self.assertEqual(_first_nonempty_line_preview("\n\nhello\nworld"), "hello")
        self.assertEqual(_first_nonempty_line_preview("\n\n"), "")

    def test_first_nonempty_line_max_200(self) -> None:
        long_line = "x" * 300
        result = _first_nonempty_line_preview(long_line)
        self.assertEqual(len(result), 200)

    def test_sample_json_field(self) -> None:
        content = '{"id":"a"}\n{"id":"b"}\n{"id":"a"}\n{"id":"c"}\n'
        result = _sample_json_field(content, "id", 2)
        self.assertEqual(result, ["a", "b"])

    def test_first_last_json_field(self) -> None:
        content = '{"ts":"2026-01-01"}\n{"ts":"2026-02-01"}\n{"ts":"2026-03-01"}\n'
        first, last = _first_last_json_field(content, "ts")
        self.assertEqual(first, "2026-01-01")
        self.assertEqual(last, "2026-03-01")

    def test_json_field_counts(self) -> None:
        content = '{"job":"a"}\n{"job":"b"}\n{"job":"a"}\n{"job":"c"}\n{"job":"a"}\n'
        counts = _json_field_counts(content, "job", 2)
        self.assertEqual(counts, {"a": 3, "b": 1})


if __name__ == "__main__":
    unittest.main()
