"""Tests for append_jsonl crash safety (issue #52)."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.storage import append_jsonl


class TestAppendJsonl(unittest.TestCase):
    """Verify append_jsonl writes durable, newline-terminated JSON lines."""

    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.tmpdir = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_basic_append_creates_valid_jsonl(self):
        """A single append produces one valid JSON line."""
        target = self.tmpdir / "log.jsonl"
        append_jsonl(target, {"key": "value"})
        line = target.read_text(encoding="utf-8").strip()
        self.assertEqual(json.loads(line), {"key": "value"})

    def test_multiple_appends_produce_multiple_lines(self):
        """Each append adds exactly one newline-terminated line."""
        target = self.tmpdir / "log.jsonl"
        append_jsonl(target, {"n": 1})
        append_jsonl(target, {"n": 2})
        append_jsonl(target, {"n": 3})
        lines = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 3)
        self.assertEqual([json.loads(line) for line in lines], [{"n": 1}, {"n": 2}, {"n": 3}])

    def test_creates_parent_directories(self):
        """Parent directories are created when they do not exist."""
        target = self.tmpdir / "deep" / "nested" / "log.jsonl"
        append_jsonl(target, {"ok": True})
        self.assertTrue(target.exists())
        self.assertEqual(json.loads(target.read_text(encoding="utf-8").strip()), {"ok": True})

    def test_append_to_existing_file_preserves_content(self):
        """Appending to a pre-existing file must not truncate prior content."""
        target = self.tmpdir / "log.jsonl"
        target.write_text('{"existing": true}\n', encoding="utf-8")
        append_jsonl(target, {"new": True})
        lines = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"existing": True})
        self.assertEqual(json.loads(lines[1]), {"new": True})

    def test_unicode_content_roundtrips(self):
        """Non-ASCII content must survive the encode/decode pipeline."""
        target = self.tmpdir / "log.jsonl"
        record = {"emoji": "\U0001f525", "text": "\u65e5\u672c\u8a9e"}
        append_jsonl(target, record)
        line = target.read_text(encoding="utf-8").strip()
        self.assertEqual(json.loads(line), record)

    def test_flush_precedes_fsync(self):
        """flush() must be called before fsync() for durability.

        Structural assertion: verifies call ordering in the source since
        TextIOWrapper.flush cannot be patched at the C level.
        """
        import inspect

        source = inspect.getsource(append_jsonl)
        # Match the method calls; resilient to variable renames as long as
        # the pattern .flush() / os.fsync( appears in the expected order.
        flush_pos = source.index(".flush()")
        fsync_pos = source.index("os.fsync(")
        self.assertLess(flush_pos, fsync_pos, "flush() must appear before fsync()")

    def test_fsync_oserror_propagates_and_record_written(self):
        """An OSError from fsync must propagate; the record should still be on disk."""
        target = self.tmpdir / "log.jsonl"
        with self.assertLogs("root", level="ERROR") as log_cm:
            with patch("app.storage.os.fsync", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    append_jsonl(target, {"fragile": True})
        # Record was written before fsync failed
        content = target.read_text(encoding="utf-8").strip()
        self.assertEqual(json.loads(content), {"fragile": True})
        # logging.error was called
        self.assertTrue(any("record may not be durable" in msg for msg in log_cm.output))

    def test_non_serializable_raises_typeerror(self):
        """A non-JSON-serializable record must raise TypeError before any I/O."""
        target = self.tmpdir / "log.jsonl"
        with self.assertRaises(TypeError):
            append_jsonl(target, {"bad": object()})
        # File must not have been created
        self.assertFalse(target.exists())

    def test_each_line_ends_with_newline(self):
        """Every appended record must produce a newline-terminated line."""
        target = self.tmpdir / "log.jsonl"
        append_jsonl(target, {"a": 1})
        append_jsonl(target, {"b": 2})
        raw = target.read_text(encoding="utf-8")
        self.assertTrue(raw.endswith("\n"))
        for line in raw.splitlines():
            self.assertTrue(len(line) > 0, "no empty lines expected")

    def test_write_oserror_propagates_and_logs(self):
        """An OSError from f.write() must propagate with logging."""
        target = self.tmpdir / "log.jsonl"
        # Patch the file's write method to fail after the file is opened
        original_open = Path.open

        def failing_open(self_path, *args, **kwargs):
            f = original_open(self_path, *args, **kwargs)
            f.write = lambda _data: (_ for _ in ()).throw(OSError("disk full"))
            return f

        with self.assertLogs("root", level="ERROR") as log_cm:
            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl(target, {"data": 1})
        self.assertTrue(any("record may not be durable" in msg for msg in log_cm.output))


if __name__ == "__main__":
    unittest.main()
