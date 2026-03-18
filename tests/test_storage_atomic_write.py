"""Tests for atomic write_text_file and write_bytes_file (issues #44, #51)."""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.storage import write_bytes_file, write_text_file


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

    def test_overwrite_produces_correct_content(self):
        """Overwriting an existing file should produce the new content."""
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

    def test_replace_failure_cleans_temp_and_preserves_original(self):
        """If os.replace fails after write+fsync, temp is cleaned and original is untouched."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "original")

        with patch("app.storage.os.replace", side_effect=OSError("cross-device")):
            with self.assertRaises(OSError):
                write_text_file(target, "new content")

        self.assertEqual(target.read_text(encoding="utf-8"), "original")
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_no_partial_file_on_simulated_crash(self):
        """Simulate a crash during write — target must not contain partial content."""
        target = self.tmpdir / "file.json"
        write_text_file(target, "safe content")

        original_fdopen = os.fdopen

        def crashing_fdopen(fd, *args, **kwargs):
            f = original_fdopen(fd, *args, **kwargs)
            f.write("PARTIAL")
            f.close()
            raise OSError("simulated crash")

        with patch("app.storage.os.fdopen", side_effect=crashing_fdopen):
            with self.assertRaises(OSError):
                write_text_file(target, "new content")

        # Original must be intact — never partially overwritten
        self.assertEqual(target.read_text(encoding="utf-8"), "safe content")
        # No orphaned temp files
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_fsync_called_before_replace(self):
        """Verify os.fsync is called before os.replace for durability."""
        target = self.tmpdir / "file.json"
        call_order = []

        original_fsync = os.fsync
        original_replace = os.replace

        def tracking_fsync(fd):
            call_order.append("fsync")
            return original_fsync(fd)

        def tracking_replace(src, dst):
            call_order.append("replace")
            return original_replace(src, dst)

        with patch("app.storage.os.fsync", side_effect=tracking_fsync), \
             patch("app.storage.os.replace", side_effect=tracking_replace):
            write_text_file(target, "durable content")

        self.assertEqual(call_order, ["fsync", "replace"])

    def test_fd_closed_on_fdopen_failure(self):
        """If os.fdopen fails, the raw fd must be closed to prevent leaks."""
        target = self.tmpdir / "file.json"
        closed_fds = []
        original_close = os.close

        def tracking_close(fd):
            closed_fds.append(fd)
            return original_close(fd)

        with patch("app.storage.os.fdopen", side_effect=OSError("encoding error")), \
             patch("app.storage.os.close", side_effect=tracking_close):
            with self.assertRaises(OSError):
                write_text_file(target, "content")

        self.assertEqual(len(closed_fds), 1, "fd should be closed exactly once")


class TestAtomicWriteBytesFile(unittest.TestCase):
    """Verify write_bytes_file uses atomic write-to-temp-then-rename."""

    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.tmpdir = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_basic_write(self):
        """A normal write produces the expected file content."""
        target = self.tmpdir / "subdir" / "file.bin"
        write_bytes_file(target, b"\x00\x01\x02\xff")
        self.assertEqual(target.read_bytes(), b"\x00\x01\x02\xff")

    def test_no_temp_file_after_success(self):
        """No leftover .tmp files after a successful write."""
        target = self.tmpdir / "file.bin"
        write_bytes_file(target, b"hello")
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_overwrite_produces_correct_content(self):
        """Overwriting an existing file should produce the new content."""
        target = self.tmpdir / "file.bin"
        write_bytes_file(target, b"first")
        write_bytes_file(target, b"second")
        self.assertEqual(target.read_bytes(), b"second")

    def test_temp_file_cleaned_on_write_failure(self):
        """If the write fails, the temp file is removed and the original is untouched."""
        target = self.tmpdir / "file.bin"
        write_bytes_file(target, b"original")

        with patch("app.storage.os.fdopen", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                write_bytes_file(target, b"should not appear")

        self.assertEqual(target.read_bytes(), b"original")
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_no_partial_file_on_simulated_crash(self):
        """Simulate a crash during write — target must not contain partial content."""
        target = self.tmpdir / "file.bin"
        write_bytes_file(target, b"safe content")

        original_fdopen = os.fdopen

        def crashing_fdopen(fd, *args, **kwargs):
            f = original_fdopen(fd, *args, **kwargs)
            f.write(b"PARTIAL")
            f.close()
            raise OSError("simulated crash")

        with patch("app.storage.os.fdopen", side_effect=crashing_fdopen):
            with self.assertRaises(OSError):
                write_bytes_file(target, b"new content")

        # Original must be intact — never partially overwritten
        self.assertEqual(target.read_bytes(), b"safe content")
        # No orphaned temp files
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_replace_failure_cleans_temp_and_preserves_original(self):
        """If os.replace fails after write+fsync, temp is cleaned and original is untouched."""
        target = self.tmpdir / "file.bin"
        write_bytes_file(target, b"original")

        with patch("app.storage.os.replace", side_effect=OSError("cross-device")):
            with self.assertRaises(OSError):
                write_bytes_file(target, b"new content")

        self.assertEqual(target.read_bytes(), b"original")
        tmp_files = [f for f in self.tmpdir.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(tmp_files, [])

    def test_fsync_called_before_replace(self):
        """Verify os.fsync is called before os.replace for durability."""
        target = self.tmpdir / "file.bin"
        call_order = []

        original_fsync = os.fsync
        original_replace = os.replace

        def tracking_fsync(fd):
            call_order.append("fsync")
            return original_fsync(fd)

        def tracking_replace(src, dst):
            call_order.append("replace")
            return original_replace(src, dst)

        with patch("app.storage.os.fsync", side_effect=tracking_fsync), \
             patch("app.storage.os.replace", side_effect=tracking_replace):
            write_bytes_file(target, b"durable content")

        self.assertEqual(call_order, ["fsync", "replace"])

    def test_fd_closed_on_fdopen_failure(self):
        """If os.fdopen fails, the raw fd must be closed to prevent leaks."""
        target = self.tmpdir / "file.bin"
        closed_fds = []
        original_close = os.close

        def tracking_close(fd):
            closed_fds.append(fd)
            return original_close(fd)

        with patch("app.storage.os.fdopen", side_effect=OSError("open error")), \
             patch("app.storage.os.close", side_effect=tracking_close):
            with self.assertRaises(OSError):
                write_bytes_file(target, b"content")

        self.assertEqual(len(closed_fds), 1, "fd should be closed exactly once")


if __name__ == "__main__":
    unittest.main()
