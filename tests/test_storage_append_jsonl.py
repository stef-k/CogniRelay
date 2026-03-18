"""Tests for append_jsonl crash safety (issue #52)."""

import json
import os
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

    def test_fsync_called_after_write(self):
        """os.fsync must be called to ensure durability."""
        target = self.tmpdir / "log.jsonl"
        fsync_calls = []
        original_fsync = os.fsync

        def tracking_fsync(fd):
            fsync_calls.append(fd)
            return original_fsync(fd)

        with patch("app.storage.os.fsync", side_effect=tracking_fsync):
            append_jsonl(target, {"durable": True})

        self.assertEqual(len(fsync_calls), 1)


if __name__ == "__main__":
    unittest.main()
