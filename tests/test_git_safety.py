"""Tests for app.git_safety commit-safe helpers."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

from fastapi import HTTPException

from app.git_safety import safe_commit_new_file, safe_commit_updated_file, try_commit_file


class TestSafeCommitNewFile(unittest.TestCase):
    """Tests for safe_commit_new_file."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.gm = MagicMock()

    def tearDown(self):
        self.tmp.cleanup()

    def test_success_returns_committed_bool(self):
        """On successful commit, the return value from gm.commit_file is propagated."""
        path = self.dir / "new.json"
        path.write_text("{}")
        self.gm.commit_file.return_value = True
        result = safe_commit_new_file(
            path=path, gm=self.gm,
            commit_message="test commit",
            error_detail="should not see this",
        )
        self.assertTrue(result)
        self.gm.commit_file.assert_called_once_with(path, "test commit")

    def test_success_returns_false_when_no_changes(self):
        """When commit_file returns False (no diff), the helper propagates that."""
        path = self.dir / "new.json"
        path.write_text("{}")
        self.gm.commit_file.return_value = False
        result = safe_commit_new_file(
            path=path, gm=self.gm,
            commit_message="test commit",
            error_detail="should not see this",
        )
        self.assertFalse(result)

    def test_failure_deletes_file_and_raises(self):
        """On commit failure, the orphaned file is deleted and HTTPException raised."""
        path = self.dir / "new.json"
        path.write_text('{"orphan": true}')
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        with self.assertRaises(HTTPException) as ctx:
            safe_commit_new_file(
                path=path, gm=self.gm,
                commit_message="test commit",
                error_detail="commit failed for new file",
            )
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("commit failed for new file", ctx.exception.detail)
        self.assertFalse(path.exists(), "Orphaned file should be deleted on failure")

    def test_failure_when_file_already_gone(self):
        """Rollback handles the case where the file was already removed."""
        path = self.dir / "gone.json"
        # File doesn't exist — rollback should not crash
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        with self.assertRaises(HTTPException):
            safe_commit_new_file(
                path=path, gm=self.gm,
                commit_message="test commit",
                error_detail="commit failed",
            )


class TestSafeCommitUpdatedFile(unittest.TestCase):
    """Tests for safe_commit_updated_file."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.gm = MagicMock()

    def tearDown(self):
        self.tmp.cleanup()

    def test_success_returns_committed_bool(self):
        """On successful commit, the return value is propagated."""
        path = self.dir / "data.json"
        path.write_text('{"new": true}')
        self.gm.commit_file.return_value = True
        result = safe_commit_updated_file(
            path=path, gm=self.gm,
            commit_message="update commit",
            error_detail="should not see this",
            old_bytes=b'{"old": true}',
        )
        self.assertTrue(result)

    def test_failure_restores_old_bytes(self):
        """On commit failure, the file is restored to old_bytes."""
        path = self.dir / "data.json"
        old_content = b'{"version": 1}'
        path.write_text('{"version": 2}')
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        with self.assertRaises(HTTPException) as ctx:
            safe_commit_updated_file(
                path=path, gm=self.gm,
                commit_message="update commit",
                error_detail="commit failed for update",
                old_bytes=old_content,
            )
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertEqual(path.read_bytes(), old_content, "File should be restored to old content")

    def test_failure_deletes_when_old_bytes_none(self):
        """When old_bytes is None (new file), failure deletes the file."""
        path = self.dir / "brand_new.json"
        path.write_text('{"created": true}')
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        with self.assertRaises(HTTPException):
            safe_commit_updated_file(
                path=path, gm=self.gm,
                commit_message="update commit",
                error_detail="commit failed",
                old_bytes=None,
            )
        self.assertFalse(path.exists(), "File should be deleted when old_bytes is None")

    def test_failure_restores_deleted_file(self):
        """When used after a file deletion, failure restores the deleted file."""
        path = self.dir / "to_delete.json"
        old_content = b'{"important": true}'
        # Simulate: caller deleted the file, then commit fails
        # File doesn't exist on disk anymore
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        with self.assertRaises(HTTPException):
            safe_commit_updated_file(
                path=path, gm=self.gm,
                commit_message="delete commit",
                error_detail="commit failed",
                old_bytes=old_content,
            )
        self.assertTrue(path.exists(), "Deleted file should be restored on failure")
        self.assertEqual(path.read_bytes(), old_content)


class TestTryCommitFile(unittest.TestCase):
    """Tests for try_commit_file."""

    def setUp(self):
        self.gm = MagicMock()

    def test_success_returns_true(self):
        """Successful commit returns True."""
        self.gm.commit_file.return_value = True
        result = try_commit_file(
            path=Path("/fake/path.json"), gm=self.gm,
            commit_message="index update",
        )
        self.assertTrue(result)

    def test_failure_returns_false_and_does_not_raise(self):
        """On failure, returns False instead of raising."""
        self.gm.commit_file.side_effect = RuntimeError("git broke")
        result = try_commit_file(
            path=Path("/fake/path.json"), gm=self.gm,
            commit_message="index update",
        )
        self.assertFalse(result)

    def test_no_changes_returns_false(self):
        """When commit_file returns False (no changes), propagates that."""
        self.gm.commit_file.return_value = False
        result = try_commit_file(
            path=Path("/fake/path.json"), gm=self.gm,
            commit_message="index update",
        )
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
