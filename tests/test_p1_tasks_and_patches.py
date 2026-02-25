import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.git_manager import GitManager
from app.main import docs_patch_apply, docs_patch_propose, tasks_create, tasks_query, tasks_update
from app.models import PatchApplyRequest, PatchProposeRequest, TaskCreateRequest, TaskUpdateRequest


class _AuthStub:
    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def commit_file(self, _path: Path, _message: str) -> bool:
        return True

    def latest_commit(self) -> str:
        return "test-sha"


class TestP1TasksAndPatches(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _init_git_repo(self, repo_root: Path) -> None:
        subprocess.run(["git", "init"], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.name", "tester"], cwd=repo_root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.email", "tester@example.local"], cwd=repo_root, check=True, capture_output=True, text=True)

    def test_tasks_lifecycle_query_and_transition_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                created = tasks_create(
                    req=TaskCreateRequest(
                        task_id="task-1",
                        title="Draft intro",
                        owner_peer="peer-alpha",
                        collaborators=["peer-beta"],
                        thread_id="thread-a",
                    ),
                    auth=_AuthStub(),
                )
                updated = tasks_update(
                    task_id="task-1",
                    req=TaskUpdateRequest(status="in_progress"),
                    auth=_AuthStub(),
                )
                done = tasks_update(
                    task_id="task-1",
                    req=TaskUpdateRequest(status="done"),
                    auth=_AuthStub(),
                )
                listing = tasks_query(status="done", limit=50, auth=_AuthStub())

                with self.assertRaises(HTTPException) as err:
                    tasks_update(
                        task_id="task-1",
                        req=TaskUpdateRequest(status="open"),
                        auth=_AuthStub(),
                    )

            self.assertTrue(created["ok"])
            self.assertEqual(updated["task"]["status"], "in_progress")
            self.assertEqual(done["task"]["status"], "done")
            self.assertEqual(listing["count"], 1)
            self.assertEqual(listing["tasks"][0]["task_id"], "task-1")
            self.assertEqual(err.exception.status_code, 409)

    def test_docs_patch_propose_and_apply_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._init_git_repo(repo_root)
            target_rel = "projects/doc.md"
            target_abs = repo_root / target_rel
            target_abs.parent.mkdir(parents=True, exist_ok=True)
            target_abs.write_text("line1\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed"], cwd=repo_root, check=True, capture_output=True, text=True)

            diff = (
                "diff --git a/projects/doc.md b/projects/doc.md\n"
                "--- a/projects/doc.md\n"
                "+++ b/projects/doc.md\n"
                "@@ -1 +1 @@\n"
                "-line1\n"
                "+line2\n"
            )

            settings = self._settings(repo_root)
            gm = GitManager(repo_root=repo_root, author_name="tester", author_email="tester@example.local")
            with patch("app.main._services", return_value=(settings, gm)):
                proposed = docs_patch_propose(
                    req=PatchProposeRequest(
                        patch_id="patch-ok",
                        target_path=target_rel,
                        base_ref="HEAD",
                        diff=diff,
                        reason="update wording",
                    ),
                    auth=_AuthStub(),
                )
                applied = docs_patch_apply(
                    req=PatchApplyRequest(patch_id="patch-ok"),
                    auth=_AuthStub(),
                )

            proposal_payload = json.loads((repo_root / "patches" / "proposals" / "patch-ok.json").read_text(encoding="utf-8"))
            applied_payload = json.loads((repo_root / "patches" / "applied" / "patch-ok.json").read_text(encoding="utf-8"))
            self.assertTrue(proposed["ok"])
            self.assertTrue(applied["ok"])
            self.assertEqual(target_abs.read_text(encoding="utf-8"), "line2\n")
            self.assertEqual(proposal_payload["status"], "applied")
            self.assertEqual(applied_payload["status"], "applied")

    def test_docs_patch_apply_fails_on_base_ref_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._init_git_repo(repo_root)
            target_rel = "projects/doc.md"
            target_abs = repo_root / target_rel
            target_abs.parent.mkdir(parents=True, exist_ok=True)
            target_abs.write_text("line1\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed"], cwd=repo_root, check=True, capture_output=True, text=True)

            diff = (
                "diff --git a/projects/doc.md b/projects/doc.md\n"
                "--- a/projects/doc.md\n"
                "+++ b/projects/doc.md\n"
                "@@ -1 +1 @@\n"
                "-line1\n"
                "+line2\n"
            )

            settings = self._settings(repo_root)
            gm = GitManager(repo_root=repo_root, author_name="tester", author_email="tester@example.local")
            with patch("app.main._services", return_value=(settings, gm)):
                docs_patch_propose(
                    req=PatchProposeRequest(
                        patch_id="patch-mismatch",
                        target_path=target_rel,
                        base_ref="HEAD",
                        diff=diff,
                    ),
                    auth=_AuthStub(),
                )

                # Move target file state after proposal so apply deterministically fails on base_ref mismatch.
                target_abs.write_text("changed-after-proposal\n", encoding="utf-8")
                subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
                subprocess.run(["git", "commit", "-m", "advance head"], cwd=repo_root, check=True, capture_output=True, text=True)

                with self.assertRaises(HTTPException) as err:
                    docs_patch_apply(
                        req=PatchApplyRequest(patch_id="patch-mismatch"),
                        auth=_AuthStub(),
                    )

            self.assertEqual(err.exception.status_code, 409)
            self.assertIn("base_ref mismatch", str(err.exception.detail))


if __name__ == "__main__":
    unittest.main()
