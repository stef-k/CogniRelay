"""Tests for low-level indexer helpers."""

import tempfile
import unittest
from pathlib import Path

from app.indexer import _iter_text_files


class TestIndexer(unittest.TestCase):
    """Validate indexer helper behavior that is easy to regress."""

    def test_iter_text_files_skips_index_directory(self) -> None:
        """Indexer file iteration should ignore derived index outputs."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "index").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)

            (repo_root / "index" / "files_index.json").write_text("{}", encoding="utf-8")
            (repo_root / "memory" / "core" / "values.md").write_text("# values", encoding="utf-8")

            files = [str(p.relative_to(repo_root)) for p in _iter_text_files(repo_root)]
            self.assertIn("memory/core/values.md", files)
            self.assertNotIn("index/files_index.json", files)


if __name__ == "__main__":
    unittest.main()
