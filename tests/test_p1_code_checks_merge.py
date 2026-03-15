"""Tests for phase-1 code check execution and merge guard behavior."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.git_manager import GitManager
from app.main import code_checks_run, code_merge
from app.models import CodeCheckRunRequest, CodeMergeRequest


class _AuthStub:
    """Auth stub that permits the scopes used by code-check tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_write_path(self, _path: str) -> None:
        """Accept any requested write path for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None


class _GitManagerStub:
    """Git manager stub that pretends every file commit succeeds."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestP1CodeChecksAndMerge(unittest.TestCase):
    """Validate code-check artifact generation and merge gating."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build a settings object rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _init_git_repo(self, repo_root: Path) -> None:
        """Initialize a temporary git repository with test author metadata."""
        subprocess.run(["git", "init"], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.name", "tester"], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.email", "tester@example.local"], cwd=repo_root, check=True, capture_output=True, text=True)

    def _seed_repo(self, repo_root: Path) -> None:
        """Seed the repository with an initial tracked source file and commit."""
        (repo_root / "projects").mkdir(parents=True, exist_ok=True)
        (repo_root / "projects" / "seed.py").write_text("print('ok')\n", encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "seed"], cwd=repo_root, check=True, capture_output=True, text=True)

    def test_code_checks_run_writes_artifact(self) -> None:
        """Code checks should persist an artifact describing the executed profile."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._init_git_repo(repo_root)
            self._seed_repo(repo_root)
            settings = self._settings(repo_root)
            gm = GitManager(repo_root=repo_root, author_name="tester", author_email="tester@example.local")

            with patch("app.main._services", return_value=(settings, gm)):
                out = code_checks_run(req=CodeCheckRunRequest(ref="HEAD", profile="lint"), auth=_AuthStub())

            run = out["run"]
            path = repo_root / out["path"]
            self.assertTrue(out["ok"])
            self.assertEqual(run["profile"], "lint")
            self.assertEqual(run["status"], "passed")
            self.assertTrue(path.exists())

    def test_code_merge_blocks_when_required_check_is_missing(self) -> None:
        """Merges should fail when the required check artifact is missing."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._init_git_repo(repo_root)
            self._seed_repo(repo_root)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as err:
                    code_merge(
                        req=CodeMergeRequest(source_ref="HEAD", target_ref="HEAD", required_checks=["test"]),
                        auth=_AuthStub(),
                    )

            self.assertEqual(err.exception.status_code, 409)
            self.assertIn("Required checks not passed", str(err.exception.detail))

    def test_code_merge_allows_when_required_check_passed(self) -> None:
        """Merges should succeed when the required check artifact has passed."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._init_git_repo(repo_root)
            self._seed_repo(repo_root)
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

            checks_dir = repo_root / "runs" / "checks"
            checks_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = checks_dir / "run-ok.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "run_id": "run-ok",
                        "profile": "test",
                        "ref_resolved": head,
                        "status": "passed",
                    }
                ),
                encoding="utf-8",
            )
            subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "add check artifact"], cwd=repo_root, check=True, capture_output=True, text=True)

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = code_merge(
                    req=CodeMergeRequest(source_ref=head, target_ref="HEAD", required_checks=["test"]),
                    auth=_AuthStub(),
                )

            self.assertTrue(out["ok"])
            self.assertEqual(out["source_ref"], head)
            self.assertEqual(out["target_ref"], "HEAD")


if __name__ == "__main__":
    unittest.main()
