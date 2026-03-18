"""Tests for go-live hardening features added around the API surface."""

import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.main import (
    backup_create,
    backup_restore_test,
    contracts,
    discovery,
    governance_policy,
    messages_send,
    messages_verify,
    peers_register,
    peers_trust_transition,
    security_keys_rotate,
    well_known_mcp,
)
from app.models import (
    BackupCreateRequest,
    BackupRestoreTestRequest,
    MessageSendRequest,
    MessageVerifyRequest,
    PeerRegisterRequest,
    PeerTrustTransitionRequest,
    SecurityKeysRotateRequest,
)


class _AuthStub:
    """Auth stub that permits the scopes used by hardening tests."""

    peer_id = "peer-admin"

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
        """Report a successful single-file commit without touching git."""
        return True

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        """Report a successful multi-path commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestGoLiveHardening(unittest.TestCase):
    """Validate hardening behaviors that should remain stable across releases."""

    def _settings(self, repo_root: Path, **overrides) -> Settings:
        """Build a settings object rooted at the temporary repository."""
        payload = {
            "repo_root": repo_root,
            "auto_init_git": False,
            "git_author_name": "n/a",
            "git_author_email": "n/a",
            "tokens": {},
            "audit_log_enabled": False,
        }
        payload.update(overrides)
        return Settings(**payload)

    def test_contract_version_is_exposed_consistently(self) -> None:
        """Contract version should stay aligned across the exposed metadata endpoints."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td), contract_version="2026-03-01")
            with patch("app.main.get_settings", return_value=settings):
                c = contracts()
                d = discovery()
                m = well_known_mcp()

        self.assertEqual(c["contract_version"], "2026-03-01")
        self.assertEqual(d["protocol"]["version"], "2026-03-01")
        self.assertEqual(m["contract_version"], "2026-03-01")
        self.assertEqual(len(c["tool_catalog_hash"]), 64)

    def test_governance_policy_default_is_available(self) -> None:
        """Governance policy endpoint should expose the default policy payload."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = governance_policy()

        self.assertTrue(out["ok"])
        self.assertEqual(out["policy"]["authority_model"]["issuer"], "hosting_agent")
        self.assertIn("scope_templates", out["policy"])

    def test_trust_transition_flow_enforces_policy(self) -> None:
        """Trust transitions should follow the configured peer trust policy."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            gm = _GitManagerStub()
            register_req = PeerRegisterRequest(
                peer_id="peer-beta",
                base_url="https://peer-beta.example.net",
                public_key="ed25519:peer-beta-key",
                trust_level="restricted",
                allowed_scopes=["read:files", "search"],
            )
            with patch("app.main._services", return_value=(settings, gm)):
                created = peers_register(req=register_req, auth=_AuthStub())
                promoted = peers_trust_transition(
                    peer_id="peer-beta",
                    req=PeerTrustTransitionRequest(trust_level="trusted", reason="manual review complete"),
                    auth=_AuthStub(),
                )

                with self.assertRaises(HTTPException) as err:
                    peers_trust_transition(
                        peer_id="peer-beta",
                        req=PeerTrustTransitionRequest(trust_level="untrusted", reason="direct demotion"),
                        auth=_AuthStub(),
                    )

        self.assertTrue(created["ok"])
        self.assertEqual(promoted["peer"]["trust_level"], "trusted")
        self.assertGreaterEqual(len(promoted["peer"]["trust_history"]), 1)
        self.assertEqual(err.exception.status_code, 409)

    def test_backup_create_and_restore_validation(self) -> None:
        """Backup creation and restore validation should succeed on a valid archive."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                created = backup_create(req=BackupCreateRequest(include_prefixes=["memory"], note="drill"), auth=_AuthStub())
                restore = backup_restore_test(
                    req=BackupRestoreTestRequest(backup_path=created["backup_path"], verify_index_rebuild=True),
                    auth=_AuthStub(),
                )

        self.assertTrue(created["ok"])
        self.assertTrue(restore["ok"])
        self.assertGreater(restore["extracted_files"], 0)
        self.assertIn("memory", restore["extracted_prefixes"])
        self.assertTrue(restore["index_validation"]["ok"])

    def test_backup_restore_rejects_unsafe_archive_paths(self) -> None:
        """Restore tests should reject backup archives with unsafe extraction paths."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "backups").mkdir(parents=True, exist_ok=True)
            bad_rel = "backups/bad.tar.gz"
            bad_path = repo_root / bad_rel
            with tarfile.open(bad_path, mode="w:gz") as tf:
                payload = b"x"
                entry = tarfile.TarInfo(name="../escape.txt")
                entry.size = len(payload)
                tf.addfile(entry, io.BytesIO(payload))

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as err:
                    backup_restore_test(req=BackupRestoreTestRequest(backup_path=bad_rel), auth=_AuthStub())

        self.assertEqual(err.exception.status_code, 400)
        self.assertIn("unsafe path", str(err.exception.detail).lower())

    def test_external_key_store_keeps_secret_out_of_repo_file(self) -> None:
        """External key-store mode should keep raw secrets out of repo-tracked files."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            key_store_path = repo_root / "external" / "security_keys.json"
            settings = self._settings(repo_root, use_external_key_store=True, key_store_path=key_store_path)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = security_keys_rotate(
                    req=SecurityKeysRotateRequest(key_id="key-ext", secret="secret-ext", return_secret=False),
                    auth=_AuthStub(),
                )

            repo_keys = json.loads((repo_root / "config" / "security_keys.json").read_text(encoding="utf-8"))
            external = json.loads(key_store_path.read_text(encoding="utf-8"))

        self.assertEqual(out["key"]["storage_mode"], "external")
        self.assertNotIn("secret", out["key"])
        self.assertNotIn("secret", repo_keys["keys"]["key-ext"])
        self.assertEqual(repo_keys["keys"]["key-ext"]["secret_ref"], "external:key-ext")
        self.assertEqual(external["keys"]["key-ext"]["secret"], "secret-ext")

    def test_verification_failure_throttle_is_enforced(self) -> None:
        """Repeated signature failures should trigger the verification throttle."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td), verify_failure_limit=1, verify_failure_window_seconds=600)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                security_keys_rotate(req=SecurityKeysRotateRequest(key_id="key-a", secret="secret-a"), auth=_AuthStub())
                first = messages_verify(
                    req=MessageVerifyRequest(
                        payload={"x": 1},
                        key_id="key-a",
                        nonce="nonce-1",
                        signature="bad-signature",
                        consume_nonce=True,
                    ),
                    auth=_AuthStub(),
                )
                second = messages_verify(
                    req=MessageVerifyRequest(
                        payload={"x": 1},
                        key_id="key-a",
                        nonce="nonce-2",
                        signature="bad-signature",
                        consume_nonce=True,
                    ),
                    auth=_AuthStub(),
                )

        self.assertFalse(first["valid"])
        self.assertEqual(first["reason"], "invalid_signature")
        self.assertFalse(second["valid"])
        self.assertEqual(second["reason"], "verification_throttled")

    def test_rate_limit_and_payload_limit_are_enforced(self) -> None:
        """Rate limiting and payload limits should reject abusive requests."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings_rate = self._settings(repo_root, token_rate_limit_per_minute=1, ip_rate_limit_per_minute=100)
            gm = _GitManagerStub()
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
            )
            with patch("app.main._services", return_value=(settings_rate, gm)):
                messages_send(req=req, auth=_AuthStub())
                with self.assertRaises(HTTPException) as err_rate:
                    messages_send(req=req, auth=_AuthStub())

            settings_payload = self._settings(repo_root, max_payload_bytes=180)
            big_req = MessageSendRequest(
                thread_id="thread-2",
                sender="peer-a",
                recipient="peer-b",
                subject="big",
                body_md="x" * 1000,
            )
            with patch("app.main._services", return_value=(settings_payload, gm)):
                with self.assertRaises(HTTPException) as err_payload:
                    messages_send(req=big_req, auth=_AuthStub())

        self.assertEqual(err_rate.exception.status_code, 429)
        self.assertEqual(err_payload.exception.status_code, 413)


if __name__ == "__main__":
    unittest.main()
