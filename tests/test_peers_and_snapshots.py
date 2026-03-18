"""Tests for peer registry operations and context snapshot behavior."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
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
    """Auth stub that permits the scopes used by peer and snapshot tests."""

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

    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful commit without touching git."""
        return True

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        """Report a successful multi-path commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class _FailingCommitPathsGitManagerStub(_GitManagerStub):
    """Git manager stub that fails grouped commits."""

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        raise OSError("git commit failed")


class _FakeHTTPResponse:
    """Minimal HTTP response stub for peer manifest fetches."""

    def __init__(self, payload: dict):
        """Serialize the provided JSON payload into the fake response body."""
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        """Return the serialized response payload."""
        return self._raw

    def __enter__(self):
        """Support use as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Propagate exceptions raised inside the response context."""
        return False


class TestPeersAndSnapshots(unittest.TestCase):
    """Validate peer operations and context snapshot creation modes."""

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

    def test_peer_register_and_list(self) -> None:
        """Registered peers should appear in peer listing results."""
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
        """Peer manifest fetch should proxy the remote manifest payload."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            req = PeerRegisterRequest(peer_id="peer-beta", base_url="https://peer-beta.example.net")
            with patch("app.main._services", return_value=(settings, gm)):
                peers_register(req=req, auth=_AuthStub())
                with patch("app.peers.service.urlopen", return_value=_FakeHTTPResponse({"service": "peer-beta", "ok": True})):
                    out = peer_manifest(peer_id="peer-beta", auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["manifest"]["service"], "peer-beta")

    def test_peer_register_rolls_back_on_commit_failure(self) -> None:
        """Peer registration should restore both files when the grouped commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _FailingCommitPathsGitManagerStub(repo_root)
            req = PeerRegisterRequest(peer_id="peer-beta", base_url="https://peer-beta.example.net")

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException):
                    peers_register(req=req, auth=_AuthStub())

            self.assertFalse((repo_root / "peers" / "registry.json").exists())
            self.assertFalse((repo_root / "peers" / "trust_policies.json").exists())

    def test_context_snapshot_working_tree_create_and_get(self) -> None:
        """Working-tree snapshots should be creatable and reloadable by id."""
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
        """Commit-mode snapshots should resolve and record the requested commit ref."""
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
