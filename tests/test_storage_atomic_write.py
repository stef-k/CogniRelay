"""Tests for atomic write_text_file and write_bytes_file (issues #44, #51, #53)."""

import logging
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.storage import _fsync_directory, write_bytes_file, write_text_file


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

    def test_fsync_ordering(self):
        """Verify file fsync before replace, then directory fsync after."""
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

        # file fsync → replace → directory fsync
        self.assertEqual(call_order, ["fsync", "replace", "fsync"])

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

    def test_fsync_ordering(self):
        """Verify file fsync before replace, then directory fsync after."""
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

        # file fsync → replace → directory fsync
        self.assertEqual(call_order, ["fsync", "replace", "fsync"])

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


class TestDirectoryFsync(unittest.TestCase):
    """Verify directory fsync after atomic rename (issue #53)."""

    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.tmpdir = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    # -- shared helpers -----------------------------------------------

    def _assert_dir_fsync_order(self, write_fn, target, content):
        """Verify: file fsync → replace → dir open → dir fsync → dir close."""
        call_order = []
        dir_fd = None

        original_fsync = os.fsync
        original_replace = os.replace
        original_open = os.open
        original_close = os.close
        dir_path_str = str(target.parent)

        def tracking_fsync(fd):
            call_order.append("fsync")
            return original_fsync(fd)

        def tracking_replace(src, dst):
            call_order.append("replace")
            return original_replace(src, dst)

        def tracking_open(path, flags, *args, **kwargs):
            nonlocal dir_fd
            fd = original_open(path, flags, *args, **kwargs)
            if path == dir_path_str:
                dir_fd = fd
                call_order.append("open_dir")
            return fd

        def tracking_close(fd):
            if fd == dir_fd:
                call_order.append("close_dir")
            return original_close(fd)

        with patch("app.storage.os.fsync", side_effect=tracking_fsync), \
             patch("app.storage.os.replace", side_effect=tracking_replace), \
             patch("app.storage.os.open", side_effect=tracking_open), \
             patch("app.storage.os.close", side_effect=tracking_close):
            write_fn(target, content)

        self.assertEqual(
            call_order,
            ["fsync", "replace", "open_dir", "fsync", "close_dir"],
        )

    # -- ordering tests ---------------------------------------------

    def test_dir_fsync_called_after_replace_text(self):
        """write_text_file fsyncs the parent directory after os.replace."""
        self._assert_dir_fsync_order(
            write_text_file, self.tmpdir / "file.json", "durable content",
        )

    def test_dir_fsync_called_after_replace_bytes(self):
        """write_bytes_file fsyncs the parent directory after os.replace."""
        self._assert_dir_fsync_order(
            write_bytes_file, self.tmpdir / "file.bin", b"durable content",
        )

    # -- Windows skip -----------------------------------------------

    def test_dir_fsync_skipped_on_windows(self):
        """_fsync_directory is a no-op when os.name is 'nt'."""
        with patch("app.storage.os.name", "nt"), \
             patch("app.storage.os.open") as mock_open:
            _fsync_directory(self.tmpdir)
            mock_open.assert_not_called()

    # -- dir fsync failure is warning, not exception ------------------

    def _dir_fsync_failure_helper(self, write_fn, target, content, read_fn):
        """Dir fsync failure logs a warning; file content survives."""
        original_fsync = os.fsync
        call_count = 0

        def fsync_fail_on_dir(fd):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Second fsync is the directory fsync
                raise OSError("directory fsync failed")
            return original_fsync(fd)

        with self.assertLogs("root", level=logging.WARNING) as cm, \
             patch("app.storage.os.fsync", side_effect=fsync_fail_on_dir):
            write_fn(target, content)

        self.assertTrue(
            any("Directory fsync failed" in msg for msg in cm.output),
            f"Expected warning about directory fsync failure, got: {cm.output}",
        )
        self.assertEqual(read_fn(target), content)

    def test_dir_fsync_failure_warns_text(self):
        """write_text_file: dir fsync failure logs warning, file content survives."""
        self._dir_fsync_failure_helper(
            write_text_file,
            self.tmpdir / "file.json",
            "content survives",
            lambda p: p.read_text(encoding="utf-8"),
        )

    def test_dir_fsync_failure_warns_bytes(self):
        """write_bytes_file: dir fsync failure logs warning, file content survives."""
        self._dir_fsync_failure_helper(
            write_bytes_file,
            self.tmpdir / "file.bin",
            b"content survives",
            lambda p: p.read_bytes(),
        )

    # -- os.open failure on directory --------------------------------

    def _dir_open_failure_helper(self, write_fn, target, content, read_fn):
        """Dir open failure logs a warning; file content survives."""
        original_open = os.open

        def open_fail_on_dir(path, flags, *args, **kwargs):
            if os.O_DIRECTORY & flags:
                raise PermissionError("dir open denied")
            return original_open(path, flags, *args, **kwargs)

        with self.assertLogs("root", level=logging.WARNING) as cm, \
             patch("app.storage.os.open", side_effect=open_fail_on_dir):
            write_fn(target, content)

        self.assertTrue(
            any("Directory fsync failed" in msg for msg in cm.output),
        )
        self.assertEqual(read_fn(target), content)

    def test_dir_open_failure_warns_text(self):
        """write_text_file: if os.open on dir fails, file survives and warning is logged."""
        self._dir_open_failure_helper(
            write_text_file,
            self.tmpdir / "file.json",
            "survives dir open fail",
            lambda p: p.read_text(encoding="utf-8"),
        )

    def test_dir_open_failure_warns_bytes(self):
        """write_bytes_file: if os.open on dir fails, file survives and warning is logged."""
        self._dir_open_failure_helper(
            write_bytes_file,
            self.tmpdir / "file.bin",
            b"survives dir open fail",
            lambda p: p.read_bytes(),
        )

    # -- fd cleanup on fsync failure --------------------------------

    def test_dir_fsync_fd_closed_on_fsync_failure(self):
        """If os.fsync fails on the directory fd, the fd is still closed."""
        dir_path = self.tmpdir
        closed_fds = []
        dir_fd = None
        original_open = os.open
        original_close = os.close

        def tracking_open(path, flags, *args, **kwargs):
            nonlocal dir_fd
            fd = original_open(path, flags, *args, **kwargs)
            if path == str(dir_path):
                dir_fd = fd
            return fd

        def tracking_close(fd):
            if fd == dir_fd:
                closed_fds.append(fd)
            return original_close(fd)

        with patch("app.storage.os.open", side_effect=tracking_open), \
             patch("app.storage.os.close", side_effect=tracking_close), \
             patch("app.storage.os.fsync", side_effect=OSError("dir fsync failed")):
            with self.assertRaises(OSError):
                _fsync_directory(dir_path)

        self.assertEqual(len(closed_fds), 1, "directory fd should be closed even on fsync failure")


if __name__ == "__main__":
    unittest.main()
