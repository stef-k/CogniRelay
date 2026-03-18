"""Tests for append_jsonl_multi atomic multi-file append with rollback."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.storage import append_jsonl_multi


class TestAppendJsonlMulti(unittest.TestCase):
    """Validate atomic multi-file append and rollback behavior."""

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

            # Write pre-existing content to p1
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

            # p1 should still have only the original line
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
        # Should not raise
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


if __name__ == "__main__":
    unittest.main()
