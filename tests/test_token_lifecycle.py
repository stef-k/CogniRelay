"""Tests for token issuance, revocation, rotation, and auth transport handling."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.auth import require_auth
from app.config import Settings
from app.main import security_tokens_issue, security_tokens_list, security_tokens_revoke, security_tokens_rotate
from app.models import SecurityTokenIssueRequest, SecurityTokenRevokeRequest, SecurityTokenRotateRequest


class _AuthStub:
    """Auth stub that permits the scopes used by token lifecycle tests."""

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

    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class _RequestStub:
    """Minimal request object used to exercise auth transport IP handling."""

    class _Client:
        """Simple client holder exposing only a host field."""

        def __init__(self, host: str) -> None:
            """Store the client host used by the request stub."""
            self.host = host

    def __init__(self, host: str) -> None:
        """Construct the request stub with the desired client host."""
        self.client = self._Client(host)


class TestTokenLifecycle(unittest.TestCase):
    """Validate token lifecycle behavior and request-IP selection rules."""

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

    def test_issue_and_revoke_token(self) -> None:
        """Issued tokens should authenticate until revoked."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                issued = security_tokens_issue(
                    req=SecurityTokenIssueRequest(
                        peer_id="peer-beta",
                        scopes=["read:files", "write:messages"],
                        read_namespaces=["memory", "messages"],
                        write_namespaces=["messages"],
                        description="beta peer token",
                    ),
                    auth=_AuthStub(),
                )

            token = issued["token"]
            token_id = issued["token_meta"]["token_id"]

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                ctx = require_auth(f"Bearer {token}")
                self.assertEqual(ctx.peer_id, "peer-beta")

            with patch("app.main._services", return_value=(settings, gm)):
                revoked = security_tokens_revoke(
                    req=SecurityTokenRevokeRequest(token_id=token_id, reason="decommission"),
                    auth=_AuthStub(),
                )

            self.assertTrue(revoked["ok"])
            self.assertEqual(revoked["revoked"], 1)

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                with self.assertRaises(HTTPException) as err:
                    require_auth(f"Bearer {token}")
            self.assertEqual(err.exception.status_code, 401)
            self.assertIn("revoked", str(err.exception.detail).lower())

    def test_rotate_token_reissues_and_invalidates_previous(self) -> None:
        """Token rotation should revoke the old token and issue a working replacement."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                issued = security_tokens_issue(
                    req=SecurityTokenIssueRequest(
                        peer_id="peer-beta",
                        scopes=["read:files", "write:messages"],
                        read_namespaces=["memory", "messages"],
                        write_namespaces=["messages"],
                        description="beta token",
                    ),
                    auth=_AuthStub(),
                )

            old_token = issued["token"]
            old_token_id = issued["token_meta"]["token_id"]
            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                self.assertEqual(require_auth(f"Bearer {old_token}").peer_id, "peer-beta")

            with patch("app.main._services", return_value=(settings, gm)):
                rotated = security_tokens_rotate(
                    req=SecurityTokenRotateRequest(token_id=old_token_id, ttl_seconds=3600, reason="routine_rotation"),
                    auth=_AuthStub(),
                )

            self.assertTrue(rotated["ok"])
            self.assertEqual(rotated["from_token"]["effective_status"], "revoked")
            self.assertEqual(rotated["token_meta"]["effective_status"], "active")
            self.assertEqual(rotated["token_meta"]["peer_id"], "peer-beta")
            self.assertEqual(sorted(rotated["token_meta"]["scopes"]), ["read:files", "write:messages"])
            self.assertEqual(sorted(rotated["token_meta"]["read_namespaces"]), ["memory", "messages"])
            self.assertEqual(rotated["token_meta"]["write_namespaces"], ["messages"])

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                with self.assertRaises(HTTPException) as err:
                    require_auth(f"Bearer {old_token}")
            self.assertEqual(err.exception.status_code, 401)
            self.assertIn("revoked", str(err.exception.detail).lower())

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                self.assertEqual(require_auth(f"Bearer {rotated['token']}").peer_id, "peer-beta")

    def test_expired_token_is_rejected(self) -> None:
        """Expired tokens should be rejected by auth resolution."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                issued = security_tokens_issue(
                    req=SecurityTokenIssueRequest(
                        peer_id="peer-expired",
                        expires_at="2000-01-01T00:00:00+00:00",
                    ),
                    auth=_AuthStub(),
                )

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                with self.assertRaises(HTTPException) as err:
                    require_auth(f"Bearer {issued['token']}")
            self.assertEqual(err.exception.status_code, 401)
            self.assertIn("expired", str(err.exception.detail).lower())

    def test_tokens_list_filters_inactive(self) -> None:
        """Token listing should optionally hide inactive tokens."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                a = security_tokens_issue(req=SecurityTokenIssueRequest(peer_id="peer-a"), auth=_AuthStub())
                security_tokens_issue(req=SecurityTokenIssueRequest(peer_id="peer-b"), auth=_AuthStub())
                security_tokens_revoke(req=SecurityTokenRevokeRequest(token_id=a["token_meta"]["token_id"]), auth=_AuthStub())
                active_only = security_tokens_list(include_inactive=False, auth=_AuthStub())
                all_tokens = security_tokens_list(include_inactive=True, auth=_AuthStub())

            self.assertTrue(active_only["ok"])
            self.assertEqual(active_only["count"], 1)
            self.assertEqual(active_only["tokens"][0]["peer_id"], "peer-b")
            self.assertEqual(all_tokens["count"], 2)

    def test_require_auth_client_ip_prefers_transport_and_proxy_safety(self) -> None:
        """Auth should prefer transport IPs unless a trusted local proxy forwards them."""
        with tempfile.TemporaryDirectory() as td:
            env = {
                "COGNIRELAY_REPO_ROOT": td,
                "COGNIRELAY_TOKENS": "token-auth:admin:peers|read:files",
            }
            with patch.dict(os.environ, env, clear=True):
                # Spoofed forwarded header should not override non-local transport source.
                remote_ctx = require_auth(
                    "Bearer token-auth",
                    x_forwarded_for="127.0.0.1",
                    request=_RequestStub("10.20.30.40"),
                )
                # Local proxy transport should honor non-local forwarded source.
                proxied_ctx = require_auth(
                    "Bearer token-auth",
                    x_forwarded_for="10.20.30.41",
                    request=_RequestStub("127.0.0.1"),
                )
                # Fully local source remains local.
                local_ctx = require_auth(
                    "Bearer token-auth",
                    x_forwarded_for="127.0.0.1",
                    request=_RequestStub("127.0.0.1"),
                )

        self.assertEqual(remote_ctx.client_ip, "10.20.30.40")
        self.assertEqual(proxied_ctx.client_ip, "10.20.30.41")
        self.assertEqual(local_ctx.client_ip, "127.0.0.1")


if __name__ == "__main__":
    unittest.main()
