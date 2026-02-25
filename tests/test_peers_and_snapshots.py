import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.indexer import rebuild_index
from app.main import (
    context_snapshot_create,
    context_snapshot_get,
    peer_manifest,
    peers_list,
    peers_register,
)
from app.models import ContextSnapshotRequest, PeerRegisterRequest


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


class _FakeHTTPResponse:
    def __init__(self, payload: dict):
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class TestPeersAndSnapshots(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_peer_register_and_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = PeerRegisterRequest(
                peer_id="peer-beta",
                base_url="https://peer-beta.example.net",
                public_key="ed25519:peer-beta-key",
                trust_level="trusted",
                allowed_scopes=["read:files", "search"],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                reg = peers_register(req=req, auth=_AuthStub())
                listing = peers_list(auth=_AuthStub())

            self.assertTrue(reg["ok"])
            self.assertEqual(listing["count"], 1)
            self.assertEqual(listing["peers"][0]["peer_id"], "peer-beta")

    def test_peer_manifest_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = PeerRegisterRequest(peer_id="peer-beta", base_url="https://peer-beta.example.net")
            with patch("app.main._services", return_value=(settings, gm)):
                peers_register(req=req, auth=_AuthStub())
                with patch("app.main.urlopen", return_value=_FakeHTTPResponse({"service": "peer-beta", "ok": True})):
                    out = peer_manifest(peer_id="peer-beta", auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["manifest"]["service"], "peer-beta")

    def test_context_snapshot_working_tree_create_and_get(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text(
                "---\ntype: core_memory\nimportance: 1.0\n---\n# Identity\nAgent profile.",
                encoding="utf-8",
            )
            (repo_root / "journal" / "2026" / "2026-02-25.md").write_text(
                "---\ntype: journal_entry\n---\nQuestion? answer.",
                encoding="utf-8",
            )
            rebuild_index(repo_root)

            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = ContextSnapshotRequest(task="identity question", limit=5)
            with patch("app.main._services", return_value=(settings, gm)):
                created = context_snapshot_create(req=req, auth=_AuthStub())
                loaded = context_snapshot_get(snapshot_id=created["snapshot_id"], auth=_AuthStub())

            self.assertTrue(created["ok"])
            self.assertEqual(created["as_of"]["mode"], "working_tree")
            self.assertTrue(loaded["ok"])
            self.assertEqual(loaded["snapshot"]["snapshot_id"], created["snapshot_id"])

    def test_context_snapshot_commit_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            subprocess.run(["git", "init"], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "tester"], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "tester@example.local"], cwd=repo_root, check=True, capture_output=True, text=True)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            p = repo_root / "memory" / "core" / "values.md"
            p.write_text("---\ntype: core_memory\nimportance: 1.0\n---\n# Values\nStay deterministic.", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed"], cwd=repo_root, check=True, capture_output=True, text=True)

            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = ContextSnapshotRequest(task="values", as_of={"mode": "commit", "value": "HEAD"}, limit=5)
            with patch("app.main._services", return_value=(settings, gm)):
                created = context_snapshot_create(req=req, auth=_AuthStub())

            self.assertTrue(created["ok"])
            self.assertEqual(created["as_of"]["mode"], "commit")
            self.assertTrue(created["as_of"]["value"])


if __name__ == "__main__":
    unittest.main()
