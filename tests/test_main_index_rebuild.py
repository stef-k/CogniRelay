import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import index_rebuild


class _AuthStub:
    def require(self, scope: str) -> None:
        if scope != "read:index":
            raise AssertionError(f"unexpected scope: {scope}")


class _GitManagerStub:
    def __init__(self) -> None:
        self.committed: list[str] = []

    def commit_file(self, path: Path, _message: str) -> bool:
        self.committed.append(str(path))
        return True

    def latest_commit(self) -> str:
        return "test-sha"


class TestIndexRebuildEndpoint(unittest.TestCase):
    def test_full_rebuild_commits_all_derived_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = Settings(
                repo_root=repo_root,
                auto_init_git=False,
                git_author_name="n/a",
                git_author_email="n/a",
                tokens={},
                audit_log_enabled=False,
            )

            def _fake_rebuild(root: Path) -> dict:
                self.assertEqual(root, repo_root)
                index_dir = root / "index"
                index_dir.mkdir(parents=True, exist_ok=True)
                for rel in (
                    "files_index.json",
                    "tags_index.json",
                    "words_index.json",
                    "types_index.json",
                    "index_state.json",
                    "search.db",
                ):
                    (index_dir / rel).write_text("{}", encoding="utf-8")
                return {"file_count": 1}

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.main.rebuild_index", side_effect=_fake_rebuild):
                    result = index_rebuild(auth=_AuthStub())

            expected = {
                "index/files_index.json",
                "index/tags_index.json",
                "index/words_index.json",
                "index/types_index.json",
                "index/index_state.json",
                "index/search.db",
            }
            self.assertEqual(set(result["committed_files"]), expected)


if __name__ == "__main__":
    unittest.main()
