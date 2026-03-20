"""Tests for segment-history source locking (issue #114, Phase 2)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.segment_history.locking import (
    _safe_lock_filename,
    acquire_sorted_source_locks,
    segment_history_source_lock,
)


class TestSafeLockFilename(unittest.TestCase):
    """Verify lock filename derivation from arbitrary keys."""

    def test_deterministic(self) -> None:
        a = _safe_lock_filename("segment_history:journal:logs/journal/2026-03-20.jsonl")
        b = _safe_lock_filename("segment_history:journal:logs/journal/2026-03-20.jsonl")
        self.assertEqual(a, b)

    def test_different_keys_differ(self) -> None:
        a = _safe_lock_filename("key_a")
        b = _safe_lock_filename("key_b")
        self.assertNotEqual(a, b)

    def test_ends_with_lock(self) -> None:
        name = _safe_lock_filename("any:key")
        self.assertTrue(name.endswith(".lock"))

    def test_no_path_traversal(self) -> None:
        name = _safe_lock_filename("../../etc/passwd")
        self.assertNotIn("/", name)
        self.assertNotIn("..", name)


class TestSourceLock(unittest.TestCase):
    """Verify single source lock acquisition and release."""

    def test_acquire_and_release(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            with segment_history_source_lock(
                "segment_history:journal:test", lock_dir=lock_dir
            ):
                self.assertTrue(lock_dir.is_dir())
            # Lock released — should be re-acquirable
            with segment_history_source_lock(
                "segment_history:journal:test", lock_dir=lock_dir
            ):
                pass

    def test_colon_key_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            with segment_history_source_lock(
                "segment_history:api_audit:logs/api_audit.jsonl",
                lock_dir=lock_dir,
            ):
                pass


class TestAcquireSortedSourceLocks(unittest.TestCase):
    """Verify batch lock acquisition in sorted order."""

    def test_empty_keys(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            with acquire_sorted_source_locks([], lock_dir=lock_dir):
                pass

    def test_multiple_keys(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            keys = ["b:key", "a:key", "c:key"]
            with acquire_sorted_source_locks(keys, lock_dir=lock_dir):
                self.assertTrue(lock_dir.is_dir())

    def test_duplicate_keys_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            keys = ["same:key", "same:key", "same:key"]
            with acquire_sorted_source_locks(keys, lock_dir=lock_dir):
                pass


if __name__ == "__main__":
    unittest.main()
