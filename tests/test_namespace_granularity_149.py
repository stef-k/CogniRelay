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


class TestPathTraversalBlocked(unittest.TestCase):
    """Verify path traversal via .. cannot bypass namespace restrictions."""

    def test_dotdot_escapes_subdirectory_namespace(self) -> None:
        """memory/coordination/../../memory/continuity/secret.json must be denied."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/coordination/../../memory/continuity/secret.json")
        self.assertEqual(cm.exception.status_code, 403)

    def test_dotdot_one_level_escape(self) -> None:
        """memory/coordination/../continuity/task.json must be denied."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("memory/coordination/../continuity/task.json")
        self.assertEqual(cm.exception.status_code, 403)

    def test_dotdot_at_start_denied(self) -> None:
        """../etc/passwd must be denied."""
        ctx = _ctx(read_namespaces={"memory"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("../etc/passwd")
        self.assertEqual(cm.exception.status_code, 403)

    def test_double_slash_normalized(self) -> None:
        """memory//coordination/foo.json normalizes and matches."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        ctx.require_read_path("memory//coordination/foo.json")

    def test_trailing_slash_path_matches(self) -> None:
        """memory/coordination/ (trailing slash) matches namespace memory/coordination."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        ctx.require_read_path("memory/coordination/")

    def test_absolute_path_denied(self) -> None:
        """/etc/passwd must be denied."""
        ctx = _ctx(read_namespaces={"*"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("/etc/passwd")
        self.assertEqual(cm.exception.status_code, 403)

    def test_write_traversal_denied(self) -> None:
        """Write path traversal must also be blocked."""
        ctx = _ctx(write_namespaces={"messages"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_write_path("messages/../memory/continuity/foo.json")
        self.assertEqual(cm.exception.status_code, 403)


class TestCollaborationPeerIntegration(unittest.TestCase):
    """End-to-end test using collaboration_peer template values.

    Template: scopes=[read:files, search, write:messages, write:projects],
    read_namespaces=[memory/coordination, messages, tasks],
    write_namespaces=[memory/coordination, messages, tasks].
    """

    _COLLAB_SCOPES = {"read:files", "search", "write:messages", "write:projects"}
    _COLLAB_READ_NS = {"memory/coordination", "messages", "tasks"}
    _COLLAB_WRITE_NS = {
        "memory/coordination/handoffs",
        "memory/coordination/shared",
        "memory/coordination/reconciliations",
        "messages",
        "tasks",
    }

    def _collab_ctx(self) -> AuthContext:
        return _ctx(
            scopes=self._COLLAB_SCOPES,
            read_namespaces=self._COLLAB_READ_NS,
            write_namespaces=self._COLLAB_WRITE_NS,
        )

    # --- Continuity privacy: denied ---

    def test_denies_continuity_read(self) -> None:
        """Collaborator cannot read continuity capsules."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/continuity/task-foo.json")

    def test_denies_continuity_write(self) -> None:
        """Collaborator cannot write continuity capsules."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_write_path("memory/continuity/task-foo.json")

    def test_denies_core_memory_read(self) -> None:
        """Collaborator cannot read core memory."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/core/identity.md")

    def test_denies_episodic_read(self) -> None:
        """Collaborator cannot read episodic logs."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/episodic/2026-03-23.jsonl")

    def test_denies_summaries_read(self) -> None:
        """Collaborator cannot read summaries."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/summaries/weekly.md")

    # --- Arbitrary coordination write: denied ---

    def test_denies_arbitrary_coordination_write(self) -> None:
        """Collaborator cannot write to arbitrary paths under memory/coordination/."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_write_path("memory/coordination/malicious/payload.json")

    # --- Admin/owner-private surfaces: denied ---

    def test_denies_journal_write(self) -> None:
        """Collaborator cannot write to owner's journal."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_write_path("journal/2026-03-23.md")

    def test_denies_config_write(self) -> None:
        """Collaborator cannot write to config."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_write_path("config/peer_tokens.json")

    def test_denies_peers_write(self) -> None:
        """Collaborator cannot write to peers registry."""
        ctx = self._collab_ctx()
        with self.assertRaises(HTTPException):
            ctx.require_write_path("peers/registry.json")

    # --- Coordination: full read + write ---

    def test_allows_handoff_read(self) -> None:
        """Collaborator can read handoff artifacts."""
        ctx = self._collab_ctx()
        ctx.require_read_path("memory/coordination/handoffs/handoff_abc123.json")

    def test_allows_handoff_write(self) -> None:
        """Collaborator can write handoff artifacts (create)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("memory/coordination/handoffs/handoff_abc123.json")

    def test_allows_shared_read(self) -> None:
        """Collaborator can read shared coordination artifacts."""
        ctx = self._collab_ctx()
        ctx.require_read_path("memory/coordination/shared/shared_abc123.json")

    def test_allows_shared_write(self) -> None:
        """Collaborator can write shared coordination artifacts (create)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("memory/coordination/shared/shared_abc123.json")

    def test_allows_reconciliation_read(self) -> None:
        """Collaborator can read reconciliation records."""
        ctx = self._collab_ctx()
        ctx.require_read_path("memory/coordination/reconciliations/recon_abc123.json")

    def test_allows_reconciliation_write(self) -> None:
        """Collaborator can write reconciliation records (open/resolve)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("memory/coordination/reconciliations/recon_abc123.json")

    # --- Messages: full read + write ---

    def test_allows_message_read(self) -> None:
        """Collaborator can read messages."""
        ctx = self._collab_ctx()
        ctx.require_read_path("messages/inbox/peer-a.jsonl")

    def test_allows_message_write(self) -> None:
        """Collaborator can write messages (send)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("messages/inbox/peer-a.jsonl")

    def test_allows_delivery_state_write(self) -> None:
        """Collaborator can write delivery state (ack)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("messages/state/delivery_index.json")

    # --- Tasks: full read + write ---

    def test_allows_task_read(self) -> None:
        """Collaborator can read tasks."""
        ctx = self._collab_ctx()
        ctx.require_read_path("tasks/open/task_abc123.json")
        ctx.require_read_path("tasks/done/task_abc123.json")

    def test_allows_task_write(self) -> None:
        """Collaborator can write tasks (create/update)."""
        ctx = self._collab_ctx()
        ctx.require_write_path("tasks/open/task_abc123.json")
        ctx.require_write_path("tasks/done/task_abc123.json")


class TestEdgeCasePathFormats(unittest.TestCase):
    """Edge cases for path formatting and case sensitivity."""

    def test_case_sensitive_matching(self) -> None:
        """Namespace matching must be case-sensitive."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException):
            ctx.require_read_path("Memory/Coordination/foo.json")

    def test_namespace_with_trailing_slash_silently_fails(self) -> None:
        """Namespace entry with trailing slash should still match after normpath on path side."""
        ctx = _ctx(read_namespaces={"memory/coordination/"})
        # This namespace has a trailing slash, so it matches "memory/coordination//" prefix
        # but not the normalized path "memory/coordination/handoffs/foo.json".
        # This is a known limitation — namespace values should not have trailing slashes.
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory/coordination/handoffs/foo.json")

    def test_backslash_path_not_matched(self) -> None:
        """Backslash-separated paths do not match forward-slash namespaces."""
        ctx = _ctx(read_namespaces={"memory/coordination"})
        with self.assertRaises(HTTPException):
            ctx.require_read_path("memory\\coordination\\file.json")

    def test_write_prefix_boundary_safety(self) -> None:
        """write_namespaces: {'messages/in'} must NOT match messages/inbox/foo.jsonl."""
        ctx = _ctx(write_namespaces={"messages/in"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_write_path("messages/inbox/foo.jsonl")
        self.assertEqual(cm.exception.status_code, 403)


class TestGovernanceTemplateUpdate(unittest.TestCase):
    """Verify the collaboration_peer governance template matches intended role model."""

    def test_collaboration_peer_scopes(self) -> None:
        """collaboration_peer has read, search, write:messages, and write:projects."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        collab = policy["scope_templates"]["collaboration_peer"]
        scopes = set(collab["scopes"])
        self.assertEqual(scopes, {"read:files", "search", "write:messages", "write:projects"})
        self.assertNotIn("admin:peers", scopes)

    def test_collaboration_peer_read_namespaces(self) -> None:
        """collaboration_peer reads coordination, messages, and tasks — not continuity."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        collab = policy["scope_templates"]["collaboration_peer"]
        read_ns = set(collab["read_namespaces"])
        self.assertEqual(read_ns, {"memory/coordination", "messages", "tasks"})
        self.assertNotIn("memory", read_ns)

    def test_collaboration_peer_write_namespaces(self) -> None:
        """collaboration_peer writes to specific coordination subdirs, messages, and tasks."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        collab = policy["scope_templates"]["collaboration_peer"]
        write_ns = set(collab["write_namespaces"])
        self.assertEqual(write_ns, {
            "memory/coordination/handoffs",
            "memory/coordination/shared",
            "memory/coordination/reconciliations",
            "messages",
            "tasks",
        })

    def test_replication_peer_unchanged(self) -> None:
        """replication_peer template should remain with wildcard read access."""
        from app.security.service import _default_governance_policy

        policy = _default_governance_policy()
        repl = policy["scope_templates"]["replication_peer"]
        self.assertIn("*", repl["read_namespaces"])


if __name__ == "__main__":
    unittest.main()
