"""Tests for append_jsonl_multi multi-file append with rollback."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.storage import _AppendTarget, _rollback_appends, append_jsonl_multi


class TestAppendJsonlMulti(unittest.TestCase):
    """Validate multi-file append and rollback behavior."""

    def test_happy_path_all_files_written(self) -> None:
        """All files receive the record when no I/O error occurs."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [root / "a.jsonl", root / "b.jsonl", root / "c.jsonl"]
            record = {"key": "value", "n": 1}

            append_jsonl_multi(paths, record)

            for p in paths:
                lines = p.read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(lines), 1)
                self.assertEqual(json.loads(lines[0]), record)

    def test_failure_on_second_file_rolls_back_first(self) -> None:
        """If the second file fails, the first is truncated back."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"
            p2 = root / "b.jsonl"
            record = {"x": 1}

            original_open = Path.open

            def failing_open(self_path, *args, **kwargs):
                if self_path == p2 and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl_multi([p1, p2], record)

            # p1 should not exist (was new, so rollback deletes it)
            self.assertFalse(p1.exists())

    def test_failure_on_first_file_no_modification(self) -> None:
        """If the first file fails, nothing is modified."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"
            p2 = root / "b.jsonl"

            original_open = Path.open

            def failing_open(self_path, *args, **kwargs):
                if self_path == p1 and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl_multi([p1, p2], {"x": 1})

            self.assertFalse(p1.exists())
            self.assertFalse(p2.exists())

    def test_pre_existing_content_preserved_on_rollback(self) -> None:
        """Existing file content is restored when a later append fails."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"
            p2 = root / "b.jsonl"

            existing_line = json.dumps({"old": True}) + "\n"
            p1.write_text(existing_line, encoding="utf-8")

            original_open = Path.open

            def failing_open(self_path, *args, **kwargs):
                if self_path == p2 and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl_multi([p1, p2], {"new": True})

            content = p1.read_text(encoding="utf-8")
            self.assertEqual(content, existing_line)

    def test_new_files_deleted_on_rollback(self) -> None:
        """Files that didn't exist before are deleted during rollback."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "new1.jsonl"
            p2 = root / "new2.jsonl"
            p3 = root / "new3.jsonl"

            original_open = Path.open

            def failing_open(self_path, *args, **kwargs):
                if self_path == p3 and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl_multi([p1, p2, p3], {"x": 1})

            self.assertFalse(p1.exists())
            self.assertFalse(p2.exists())
            self.assertFalse(p3.exists())

    def test_non_serializable_record_raises_before_io(self) -> None:
        """A non-serializable record raises TypeError without touching files."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"

            with self.assertRaises(TypeError):
                append_jsonl_multi([p1], {"bad": object()})

            self.assertFalse(p1.exists())

    def test_empty_paths_is_noop(self) -> None:
        """Calling with an empty paths list does nothing."""
        append_jsonl_multi([], {"key": "value"})

    def test_single_path_equivalent_to_append_jsonl(self) -> None:
        """A single-path call behaves like append_jsonl."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p = root / "single.jsonl"
            record = {"solo": True}

            append_jsonl_multi([p], record)

            lines = p.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), record)

    def test_multiple_appends_accumulate(self) -> None:
        """Successive calls append additional lines to all files."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [root / "a.jsonl", root / "b.jsonl"]

            append_jsonl_multi(paths, {"n": 1})
            append_jsonl_multi(paths, {"n": 2})

            for p in paths:
                lines = p.read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(lines), 2)
                self.assertEqual(json.loads(lines[0])["n"], 1)
                self.assertEqual(json.loads(lines[1])["n"], 2)

    def test_rollback_failure_logs_error_and_continues(self) -> None:
        """When rollback itself fails, errors are logged and the original OSError still propagates."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"
            p2 = root / "b.jsonl"

            # Pre-populate p1 so rollback will attempt truncation
            existing = json.dumps({"old": True}) + "\n"
            p1.write_text(existing, encoding="utf-8")

            original_open = Path.open
            call_count = {"rollback_open": 0}

            def failing_open(self_path, *args, **kwargs):
                # Fail when p2 is opened for append (trigger rollback)
                if self_path == p2 and "a" in args:
                    raise OSError("disk full")
                # Fail when p1 is opened for rollback (r+b mode)
                if self_path == p1 and "r+b" in args:
                    call_count["rollback_open"] += 1
                    raise OSError("permission denied during rollback")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertLogs("root", level="ERROR") as cm:
                    with self.assertRaises(OSError, msg="disk full"):
                        append_jsonl_multi([p1, p2], {"new": True})

            # Rollback was attempted
            self.assertGreaterEqual(call_count["rollback_open"], 1)
            # Rollback failure was logged at ERROR level
            rollback_logs = [m for m in cm.output if "rollback failed" in m]
            self.assertTrue(rollback_logs, "Expected 'rollback failed' error log")

    def test_failure_on_third_file_preserves_existing_content(self) -> None:
        """When file 3 fails, files 1-2 with pre-existing content are restored."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "a.jsonl"
            p2 = root / "b.jsonl"
            p3 = root / "c.jsonl"

            line1 = json.dumps({"file": 1}) + "\n"
            line2 = json.dumps({"file": 2}) + "\n"
            p1.write_text(line1, encoding="utf-8")
            p2.write_text(line2, encoding="utf-8")

            original_open = Path.open

            def failing_open(self_path, *args, **kwargs):
                if self_path == p3 and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch.object(Path, "open", failing_open):
                with self.assertRaises(OSError):
                    append_jsonl_multi([p1, p2, p3], {"new": True})

            self.assertEqual(p1.read_text(encoding="utf-8"), line1)
            self.assertEqual(p2.read_text(encoding="utf-8"), line2)
            self.assertFalse(p3.exists())

    def test_duplicate_paths_deduplicated(self) -> None:
        """When paths resolve to the same file, the record is appended only once."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p = root / "same.jsonl"

            append_jsonl_multi([p, p, p], {"x": 1})

            lines = p.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1, "Record should be appended only once for duplicate paths")

    def test_duplicate_paths_via_symlink_deduplicated(self) -> None:
        """Paths that resolve to the same file via symlinks are deduplicated."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            real = root / "real.jsonl"
            link = root / "link.jsonl"
            # Create the real file so the symlink target exists for resolve()
            real.touch()
            os.symlink(real, link)

            append_jsonl_multi([real, link], {"x": 1})

            lines = real.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)

    def test_subdirectories_created_automatically(self) -> None:
        """Parent directories are created for paths in subdirectories."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = root / "sub" / "dir1" / "a.jsonl"
            p2 = root / "sub" / "dir2" / "b.jsonl"

            append_jsonl_multi([p1, p2], {"nested": True})

            for p in [p1, p2]:
                lines = p.read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(lines), 1)
                self.assertEqual(json.loads(lines[0]), {"nested": True})


class TestRollbackAppendsDirect(unittest.TestCase):
    """Direct tests for _rollback_appends helper."""

    def test_rollback_truncates_existing_and_deletes_new(self) -> None:
        """Rollback truncates existing files and deletes new ones."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            existing = root / "existing.jsonl"
            new_file = root / "new.jsonl"

            original_content = '{"old":true}\n'
            existing.write_text(original_content + '{"appended":true}\n', encoding="utf-8")
            new_file.write_text('{"new":true}\n', encoding="utf-8")

            targets = [
                _AppendTarget(path=existing, prior_size=len(original_content.encode()), is_new=False),
                _AppendTarget(path=new_file, prior_size=0, is_new=True),
            ]

            _rollback_appends(targets)

            self.assertEqual(existing.read_text(encoding="utf-8"), original_content)
            self.assertFalse(new_file.exists())

    def test_rollback_logs_error_on_failure(self) -> None:
        """Rollback logs at ERROR level when truncation fails."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p = root / "nonexistent_for_truncate.jsonl"
            # Don't create the file — truncation will fail

            targets = [_AppendTarget(path=p, prior_size=0, is_new=False)]

            with self.assertLogs("root", level="ERROR") as cm:
                _rollback_appends(targets)

            error_logs = [m for m in cm.output if "rollback failed" in m]
            self.assertTrue(error_logs)


if __name__ == "__main__":
    unittest.main()
