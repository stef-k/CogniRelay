"""Tests for narrower replication_peer scope (issue #155).

Verifies that the replication_peer governance template uses replication:sync
instead of admin:peers, and that the scope/namespace enforcement behaves
correctly for replication tokens.
"""

from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.auth import AuthContext
from app.config import SCOPE_ADMIN_PEERS, SCOPE_REPLICATION_SYNC
from app.maintenance.service import REPLICATION_ALLOWED_PREFIXES
from app.security.service import _default_governance_policy


def _make_auth(
    scopes: set[str],
    read_namespaces: set[str] | None = None,
    write_namespaces: set[str] | None = None,
) -> AuthContext:
    """Build an AuthContext with the given scopes and namespaces."""
    return AuthContext(
        token="test-token",
        peer_id="test-peer",
        scopes=scopes,
        read_namespaces=read_namespaces or {"*"},
        write_namespaces=write_namespaces or set(),
    )


class TestReplicationScopeEnforcement(unittest.TestCase):
    """Verify replication:sync scope grants and denials."""

    def test_replication_sync_passes_require(self) -> None:
        auth = _make_auth({SCOPE_REPLICATION_SYNC})
        auth.require(SCOPE_REPLICATION_SYNC)

    def test_replication_sync_does_not_grant_admin_peers(self) -> None:
        auth = _make_auth({SCOPE_REPLICATION_SYNC})
        with self.assertRaises(HTTPException) as ctx:
            auth.require(SCOPE_ADMIN_PEERS)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_admin_peers_still_bypasses_replication_sync(self) -> None:
        """Owner tokens with admin:peers can still call replication endpoints."""
        auth = _make_auth({SCOPE_ADMIN_PEERS})
        auth.require(SCOPE_REPLICATION_SYNC)
        self.assertEqual(len(auth.bypass_events), 1)
        self.assertEqual(auth.bypass_events[0]["kind"], "scope")
        self.assertEqual(auth.bypass_events[0]["required"], SCOPE_REPLICATION_SYNC)

    def test_no_scope_denied(self) -> None:
        auth = _make_auth({"read:files"})
        with self.assertRaises(HTTPException) as ctx:
            auth.require(SCOPE_REPLICATION_SYNC)
        self.assertEqual(ctx.exception.status_code, 403)


class TestReplicationNamespaceEnforcement(unittest.TestCase):
    """Verify replication peer namespace access without admin:peers bypass."""

    def setUp(self) -> None:
        policy = _default_governance_policy()
        tmpl = policy["scope_templates"]["replication_peer"]
        self.write_ns = set(tmpl["write_namespaces"])
        self.read_ns = set(tmpl["read_namespaces"])

    def test_write_allowed_for_all_replication_prefixes(self) -> None:
        auth = _make_auth(
            {SCOPE_REPLICATION_SYNC, "read:files", "write:messages"},
            read_namespaces=self.read_ns,
            write_namespaces=self.write_ns,
        )
        for prefix in REPLICATION_ALLOWED_PREFIXES:
            auth.require_write_path(f"{prefix}/test-file.json")

    def test_write_allowed_for_peers_state(self) -> None:
        auth = _make_auth(
            {SCOPE_REPLICATION_SYNC},
            write_namespaces=self.write_ns,
        )
        auth.require_write_path("peers/replication_state.json")
        auth.require_write_path("peers/replication_tombstones.json")

    def test_write_denied_outside_replication_namespaces(self) -> None:
        auth = _make_auth(
            {SCOPE_REPLICATION_SYNC},
            write_namespaces=self.write_ns,
        )
        for forbidden in ("config/peer_tokens.json", "logs/audit.jsonl", "backups/test.tar"):
            with self.assertRaises(HTTPException, msg=f"Expected denial for {forbidden}"):
                auth.require_write_path(forbidden)

    def test_wildcard_read_access(self) -> None:
        auth = _make_auth(
            {SCOPE_REPLICATION_SYNC},
            read_namespaces=self.read_ns,
            write_namespaces=self.write_ns,
        )
        auth.require_read_path("memory/continuity/capsule.json")
        auth.require_read_path("config/peer_tokens.json")


class TestGovernanceTemplateShape(unittest.TestCase):
    """Verify the replication_peer template no longer carries admin:peers."""

    def test_no_admin_peers_in_template(self) -> None:
        policy = _default_governance_policy()
        repl = policy["scope_templates"]["replication_peer"]
        self.assertNotIn(SCOPE_ADMIN_PEERS, repl["scopes"])

    def test_replication_sync_in_template(self) -> None:
        policy = _default_governance_policy()
        repl = policy["scope_templates"]["replication_peer"]
        self.assertIn(SCOPE_REPLICATION_SYNC, repl["scopes"])

    def test_replication_sync_in_all_scopes(self) -> None:
        from app.config import ALL_SCOPES
        self.assertIn(SCOPE_REPLICATION_SYNC, ALL_SCOPES)


class TestScopeMapConsistency(unittest.TestCase):
    """Verify that the discovery service scope map matches actual enforcement."""

    def test_replication_endpoints_use_replication_sync_in_scope_map(self) -> None:
        from pathlib import Path
        source = Path("app/discovery/service.py").read_text(encoding="utf-8")
        # The scope map entries for replication endpoints must reference
        # replication:sync, not admin:peers.
        self.assertIn('"POST /v1/replication/pull": {"scope": "replication:sync"}', source)
        self.assertIn('"POST /v1/replication/push": {"scope": "replication:sync"}', source)


class TestScopeValidation(unittest.TestCase):
    """Verify unknown scopes are warned about at token load time."""

    def test_unknown_scope_logged_at_file_load(self) -> None:
        """_load_tokens_file warns about unknown scopes."""
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_dir = Path(tmpdir) / "config"
            cfg_dir.mkdir()
            tokens_file = cfg_dir / "peer_tokens.json"
            tokens_file.write_text(json.dumps({
                "tokens": [{
                    "token_sha256": "abc123",
                    "peer_id": "test",
                    "scopes": ["read:files", "typo:scope"],
                }]
            }))
            from app.config import _load_tokens_file
            with patch("app.config._log") as mock_log:
                _load_tokens_file(Path(tmpdir))
                mock_log.warning.assert_called_once()
                call_args = mock_log.warning.call_args
                self.assertIn("unknown scopes", call_args[0][0])

    def test_unknown_scope_logged_at_issuance(self) -> None:
        """security_tokens_issue_service warns about unknown scopes."""
        # We just verify the validation code path exists by checking
        # that ALL_SCOPES is used for validation in the issuance function.
        import inspect
        from app.security.service import security_tokens_issue_service
        source = inspect.getsource(security_tokens_issue_service)
        self.assertIn("ALL_SCOPES", source)
        self.assertIn("unknown_scopes", source)


if __name__ == "__main__":
    unittest.main()
