"""Tests for segment-history manifest infrastructure (issue #114, Phase 2)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.segment_history.manifest import (
    manifest_path,
    read_manifest,
    remove_manifest,
    write_manifest,
)


class TestManifestWriteRead(unittest.TestCase):
    """Verify manifest write/read round-trip."""

    def test_write_and_read(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            path = write_manifest(
                repo,
                operation="maintenance",
                family="journal",
                source_paths=["logs/journal/2026-03-20.jsonl"],
                segment_ids=["journal__20260320T120000Z__0001"],
                started_at="2026-03-20T12:00:00+00:00",
            )
            self.assertTrue(path.is_file())
            data = read_manifest(repo, "journal")
            self.assertIsNotNone(data)
            self.assertEqual(data["schema_type"], "segment_history_manifest")
            self.assertEqual(data["schema_version"], "1.0")
            self.assertEqual(data["operation"], "maintenance")
            self.assertEqual(data["family"], "journal")
            self.assertEqual(len(data["source_paths"]), 1)
            self.assertEqual(len(data["segment_ids"]), 1)

    def test_read_absent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self.assertIsNone(read_manifest(Path(td), "journal"))

    def test_read_corrupt_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            path = manifest_path(repo, "journal")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                read_manifest(repo, "journal")


class TestManifestRemove(unittest.TestCase):
    """Verify manifest removal."""

    def test_remove_existing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            write_manifest(
                repo,
                operation="cold_store",
                family="api_audit",
                source_paths=[],
                segment_ids=[],
            )
            self.assertTrue(remove_manifest(repo, "api_audit"))
            self.assertIsNone(read_manifest(repo, "api_audit"))

    def test_remove_absent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            self.assertFalse(remove_manifest(Path(td), "journal"))


class TestManifestGitignore(unittest.TestCase):
    """Verify .gitignore is created in the manifest directory."""

    def test_gitignore_created(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            write_manifest(
                repo,
                operation="maintenance",
                family="journal",
                source_paths=[],
                segment_ids=[],
            )
            gitignore = repo / ".cognirelay" / "segment-history" / ".gitignore"
            self.assertTrue(gitignore.is_file())
            self.assertEqual(gitignore.read_text(encoding="utf-8").strip(), "*")


if __name__ == "__main__":
    unittest.main()
