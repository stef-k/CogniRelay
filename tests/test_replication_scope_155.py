"""Tests for narrower replication_peer scope (issue #155).

Verifies that the replication_peer governance template uses replication:sync
instead of admin:peers, and that the scope/namespace enforcement behaves
correctly for replication tokens.
"""

from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.auth import AuthContext
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
        auth = _make_auth({"replication:sync"})
        auth.require("replication:sync")

    def test_replication_sync_does_not_grant_admin_peers(self) -> None:
        auth = _make_auth({"replication:sync"})
        with self.assertRaises(HTTPException) as ctx:
            auth.require("admin:peers")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_admin_peers_still_bypasses_replication_sync(self) -> None:
        """Owner tokens with admin:peers can still call replication endpoints."""
        auth = _make_auth({"admin:peers"})
        auth.require("replication:sync")
        self.assertEqual(len(auth.bypass_events), 1)
        self.assertEqual(auth.bypass_events[0]["kind"], "scope")
        self.assertEqual(auth.bypass_events[0]["required"], "replication:sync")

    def test_no_scope_denied(self) -> None:
        auth = _make_auth({"read:files"})
        with self.assertRaises(HTTPException) as ctx:
            auth.require("replication:sync")
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
            {"replication:sync", "read:files", "write:messages"},
            read_namespaces=self.read_ns,
            write_namespaces=self.write_ns,
        )
        for prefix in REPLICATION_ALLOWED_PREFIXES:
            auth.require_write_path(f"{prefix}/test-file.json")

    def test_write_allowed_for_peers_state(self) -> None:
        auth = _make_auth(
            {"replication:sync"},
            write_namespaces=self.write_ns,
        )
        auth.require_write_path("peers/replication_state.json")

    def test_write_denied_outside_replication_namespaces(self) -> None:
        auth = _make_auth(
            {"replication:sync"},
            write_namespaces=self.write_ns,
        )
        for forbidden in ("config/peer_tokens.json", "logs/audit.jsonl", "backups/test.tar"):
            with self.assertRaises(HTTPException, msg=f"Expected denial for {forbidden}"):
                auth.require_write_path(forbidden)

    def test_wildcard_read_access(self) -> None:
        auth = _make_auth(
            {"replication:sync"},
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
        self.assertNotIn("admin:peers", repl["scopes"])

    def test_replication_sync_in_template(self) -> None:
        policy = _default_governance_policy()
        repl = policy["scope_templates"]["replication_peer"]
        self.assertIn("replication:sync", repl["scopes"])

    def test_replication_sync_in_all_scopes(self) -> None:
        from app.config import ALL_SCOPES
        self.assertIn("replication:sync", ALL_SCOPES)


if __name__ == "__main__":
    unittest.main()
