"""Tests for atomic write_text_file (issue #44)."""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.storage import write_text_file


class TestAtomicWriteTextFile(unittest.TestCase):
    """Verify write_text_file uses atomic write-to-temp-then-rename."""

    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.tmpdir = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_basic_write(self):
        """A normal write produces the expected file content."""
        target = self.tmpdir / "subdir" / "file.json"
        write_text_file(target, '{"key": "value"}')
        self.assertEqual(target.read_text(encoding="utf-8"), '{"key": "value"}')

    def test_no_temp_file_after_success(self):
        """No leftover .tmp files after a successful write."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "hello")
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_overwrite_preserves_atomicity(self):
        """Overwriting an existing file should work correctly."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "first")
        write_text_file(target, "second")
        self.assertEqual(target.read_text(encoding="utf-8"), "second")

    def test_temp_file_cleaned_on_write_failure(self):
        """If the write fails, the temp file is removed and the original is untouched."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "original")

        with patch("app.storage.os.fdopen", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                write_text_file(target, "should not appear")

        # Original content must survive
        self.assertEqual(target.read_text(encoding="utf-8"), "original")
        # No orphaned temp files
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_no_partial_file_on_simulated_crash(self):
        """Simulate a crash during write — target must not contain partial content."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "safe content")

        original_fdopen = os.fdopen

        def crashing_fdopen(fd, *args, **kwargs):
            f = original_fdopen(fd, *args, **kwargs)
            # Write partial content then raise to simulate crash
            f.write("PARTIAL")
            raise OSError("simulated crash")

        with patch("app.storage.os.fdopen", side_effect=crashing_fdopen):
            with self.assertRaises(OSError):
                write_text_file(target, "new content")

        # Original must be intact — never partially overwritten
        self.assertEqual(target.read_text(encoding="utf-8"), "safe content")
        # No orphaned temp files
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_fsync_called_before_rename(self):
        """Verify os.fsync is called before os.rename for durability."""
        target = self.tmpdir / "file.json"
        call_order = []

        original_fsync = os.fsync
        original_rename = os.rename

        def tracking_fsync(fd):
            call_order.append("fsync")
            return original_fsync(fd)

        def tracking_rename(src, dst):
            call_order.append("rename")
            return original_rename(src, dst)

        with patch("app.storage.os.fsync", side_effect=tracking_fsync), \
             patch("app.storage.os.rename", side_effect=tracking_rename):
            write_text_file(target, "durable content")

        self.assertEqual(call_order, ["fsync", "rename"])


if __name__ == "__main__":
    unittest.main()
