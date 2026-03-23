"""Tests for sub-directory namespace granularity in auth model (issue #149)."""

from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.auth import AuthContext


def _ctx(
    *,
    scopes: set[str] | None = None,
    read_namespaces: set[str] | None = None,
    write_namespaces: set[str] | None = None,
) -> AuthContext:
    """Build a minimal AuthContext for namespace testing."""
    return AuthContext(
        token="test-token",
        peer_id="peer-test",
        scopes=scopes or set(),
        read_namespaces=read_namespaces or set(),
        write_namespaces=write_namespaces or set(),
    )


class TestSubDirectoryReadNamespace(unittest.TestCase):
    """Verify sub-directory granularity in read namespace checks."""

    def test_subdirectory_allows_nested_path(self) -> None:
        """read_namespaces: {'memory/coordination'} allows memory/coordination/handoffs/foo.json."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        ctx.require_read_path("memory/coordination/handoffs/foo.json")

    def test_subdirectory_denies_sibling_continuity(self) -> None:
        """read_namespaces: {'memory/coordination'} denies memory/continuity/task-foo.json."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/continuity/task-foo.json")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subdirectory_denies_sibling_core(self) -> None:
        """read_namespaces: {'memory/coordination'} denies memory/core/identity.md."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/core/identity.md")
        self.assertEqual(cm.exception.status_code, 403)

    def test_top_level_backward_compat(self) -> None:
        """read_namespaces: {'memory'} still allows memory/continuity/task-foo.json."""
        ctx = _ctx(read_namespaces={"memory"})
        ctx.require_read_path("memory/continuity/task-foo.json")

    def test_top_level_allows_deeply_nested(self) -> None:
        """read_namespaces: {'memory'} allows memory/coordination/handoffs/foo.json."""
        ctx = _ctx(read_namespaces={"memory"})
        ctx.require_read_path("memory/coordination/handoffs/foo.json")

    def test_wildcard_allows_everything(self) -> None:
        """read_namespaces: {'*'} allows any path."""
        ctx = _ctx(read_namespaces={"*"})
        ctx.require_read_path("memory/continuity/task-foo.json")
        ctx.require_read_path("messages/inbox/peer-a.jsonl")
        ctx.require_read_path("journal/2026-03-23.md")

    def test_admin_peers_bypass(self) -> None:
        """admin:peers scope bypasses all namespace checks."""
        ctx = _ctx(scopes={"admin:peers"}, read_namespaces=set())
        ctx.require_read_path("memory/continuity/task-foo.json")
        ctx.require_read_path("messages/inbox/peer-a.jsonl")

    def test_prefix_boundary_safety(self) -> None:
        """read_namespaces: {'memory/co'} must NOT match memory/coordination/foo.json."""
        ctx = _ctx(read_namespaces={"memory/co"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/coordination/foo.json")
        self.assertEqual(cm.exception.status_code, 403)

    def test_prefix_boundary_no_partial_directory(self) -> None:
        """read_namespaces: {'msg'} must NOT match messages/inbox/foo.jsonl."""
        ctx = _ctx(read_namespaces={"msg"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("messages/inbox/foo.jsonl")
        self.assertEqual(cm.exception.status_code, 403)

    def test_multiple_sub_namespaces(self) -> None:
        """Multiple sub-namespaces allow their subtrees, deny others."""
        ctx = _ctx(read_namespaces={"memory/coordination", "memory/summaries"})
        ctx.require_read_path("memory/coordination/handoffs/foo.json")
        ctx.require_read_path("memory/summaries/weekly.md")
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/continuity/task-foo.json")

    def test_empty_path_denied(self) -> None:
        """Empty relative_path is denied."""
        ctx = _ctx(read_namespaces={"memory"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("")
        self.assertEqual(cm.exception.status_code, 403)

    def test_exact_namespace_path_match(self) -> None:
        """read_namespaces: {'memory/coordination'} allows exact path memory/coordination."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        ctx.require_read_path("memory/coordination")

    def test_exact_top_level_match(self) -> None:
        """read_namespaces: {'messages'} allows exact path messages."""
        ctx = _ctx(read_namespaces={"messages"})
        ctx.require_read_path("messages")


class TestSubDirectoryWriteNamespace(unittest.TestCase):
    """Verify sub-directory granularity in write namespace checks."""

    def test_write_top_level_allows_nested(self) -> None:
        """write_namespaces: {'messages'} allows messages/inbox/foo.jsonl."""
        ctx = _ctx(write_namespaces={"messages"})
        ctx.require_write_path("messages/inbox/foo.jsonl")

    def test_write_subdirectory_allows_nested(self) -> None:
        """write_namespaces: {'messages/inbox'} allows messages/inbox/foo.jsonl."""
        ctx = _ctx(write_namespaces={"messages/inbox"})
        ctx.require_write_path("messages/inbox/foo.jsonl")

    def test_write_subdirectory_denies_sibling(self) -> None:
        """write_namespaces: {'messages/inbox'} denies messages/state/delivery.json."""
        ctx = _ctx(write_namespaces={"messages/inbox"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_write_path("messages/state/delivery.json")
        self.assertEqual(cm.exception.status_code, 403)

    def test_write_wildcard(self) -> None:
        """write_namespaces: {'*'} allows any write path."""
        ctx = _ctx(write_namespaces={"*"})
        ctx.require_write_path("memory/continuity/task-foo.json")

    def test_write_admin_bypass(self) -> None:
        """admin:peers scope bypasses write namespace checks."""
        ctx = _ctx(scopes={"admin:peers"}, write_namespaces=set())
        ctx.require_write_path("memory/continuity/task-foo.json")


class TestErrorDetailContainsFullPath(unittest.TestCase):
    """Verify the error detail includes the full path for debugging."""

    def test_read_error_shows_full_path(self) -> None:
        """Denied read should include full relative_path in error detail."""
        ctx = _ctx(read_namespaces={"messages"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/continuity/task-foo.json")
        self.assertIn("memory/continuity/task-foo.json", cm.exception.detail)

    def test_write_error_shows_full_path(self) -> None:
        """Denied write should include full relative_path in error detail."""
        ctx = _ctx(write_namespaces={"messages"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_write_path("memory/continuity/task-foo.json")
        self.assertIn("memory/continuity/task-foo.json", cm.exception.detail)


class TestGovernanceTemplateUpdate(unittest.TestCase):
    """Verify the collaboration_peer governance template was updated."""

    def test_collaboration_peer_read_namespaces(self) -> None:
        """collaboration_peer template should use memory/coordination, not memory."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        collab = policy["scope_templates"]["collaboration_peer"]
        self.assertIn("memory/coordination", collab["read_namespaces"])
        self.assertNotIn("memory", collab["read_namespaces"])
        self.assertIn("messages", collab["read_namespaces"])

    def test_replication_peer_unchanged(self) -> None:
        """replication_peer template should remain with wildcard read access."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        repl = policy["scope_templates"]["replication_peer"]
        self.assertIn("*", repl["read_namespaces"])


if __name__ == "__main__":
    unittest.main()
