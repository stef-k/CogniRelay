"""Tests for admin:peers bypass audit visibility (issue #153)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.auth import AuthContext
from tests.helpers import AllowAllAuthStub


def _ctx(
    *,
    scopes: set[str] | None = None,
    read_namespaces: set[str] | None = None,
    write_namespaces: set[str] | None = None,
) -> AuthContext:
    """Build a minimal AuthContext for bypass testing."""
    return AuthContext(
        token="test-token",
        peer_id="peer-test",
        scopes=scopes or set(),
        read_namespaces=read_namespaces or set(),
        write_namespaces=write_namespaces or set(),
    )


# ---------------------------------------------------------------------------
# Scope bypass detection
# ---------------------------------------------------------------------------


class TestScopeBypassDetection(unittest.TestCase):
    """Verify that scope bypass via admin:peers is recorded."""

    def test_bypass_recorded_when_specific_scope_missing(self) -> None:
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")
        self.assertEqual(len(ctx.bypass_events), 1)
        self.assertEqual(ctx.bypass_events[0], {"kind": "scope", "required": "write:messages"})

    def test_no_bypass_when_scope_present(self) -> None:
        ctx = _ctx(scopes={"admin:peers", "write:messages"})
        ctx.require("write:messages")
        self.assertEqual(ctx.bypass_events, [])

    def test_no_bypass_when_requiring_admin_peers_directly(self) -> None:
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("admin:peers")
        self.assertEqual(ctx.bypass_events, [])

    def test_multiple_scope_bypasses_accumulate(self) -> None:
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")
        ctx.require("read:files")
        self.assertEqual(len(ctx.bypass_events), 2)
        self.assertEqual(ctx.bypass_events[0]["required"], "write:messages")
        self.assertEqual(ctx.bypass_events[1]["required"], "read:files")


# ---------------------------------------------------------------------------
# Namespace bypass detection
# ---------------------------------------------------------------------------


class TestNamespaceBypassDetection(unittest.TestCase):
    """Verify that namespace bypass via admin:peers is recorded."""

    def test_read_namespace_bypass_recorded(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, read_namespaces=set())
        ctx.require_read_path("memory/foo.json")
        self.assertEqual(len(ctx.bypass_events), 1)
        self.assertEqual(
            ctx.bypass_events[0],
            {"kind": "namespace", "mode": "read", "path": "memory/foo.json"},
        )

    def test_write_namespace_bypass_recorded(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, write_namespaces=set())
        ctx.require_write_path("journal/entry.json")
        self.assertEqual(len(ctx.bypass_events), 1)
        self.assertEqual(
            ctx.bypass_events[0],
            {"kind": "namespace", "mode": "write", "path": "journal/entry.json"},
        )

    def test_no_bypass_when_wildcard_present(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, read_namespaces={"*"})
        ctx.require_read_path("memory/foo.json")
        self.assertEqual(ctx.bypass_events, [])

    def test_no_bypass_when_namespace_matches(self) -> None:
        ctx = _ctx(scopes={"read:files"}, read_namespaces={"memory"})
        ctx.require_read_path("memory/foo.json")
        self.assertEqual(ctx.bypass_events, [])

    def test_mixed_scope_and_namespace_bypass(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, write_namespaces=set())
        ctx.require("write:messages")
        ctx.require_write_path("messages/outbox/peer-b.jsonl")
        self.assertEqual(len(ctx.bypass_events), 2)
        self.assertEqual(ctx.bypass_events[0]["kind"], "scope")
        self.assertEqual(ctx.bypass_events[1]["kind"], "namespace")


# ---------------------------------------------------------------------------
# Audit event integration
# ---------------------------------------------------------------------------


class TestAuditEventBypassInjection(unittest.TestCase):
    """Verify that bypass events are injected into audit log entries."""

    def _make_settings(self) -> MagicMock:
        settings = MagicMock()
        settings.audit_log_enabled = True
        settings.audit_log_rollover_bytes = 0
        settings.repo_root = "/tmp/test-repo"
        return settings

    @patch("app.runtime.service.append_audit")
    def test_audit_includes_admin_bypass(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")

        audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        mock_append.assert_called_once()
        detail = mock_append.call_args[1].get("detail") or mock_append.call_args[0][3]
        self.assertIn("admin_bypass", detail)
        self.assertEqual(len(detail["admin_bypass"]), 1)
        self.assertEqual(detail["admin_bypass"][0]["kind"], "scope")

    @patch("app.runtime.service.append_audit")
    def test_audit_excludes_bypass_when_none(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"write:messages"})
        ctx.require("write:messages")

        audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        mock_append.assert_called_once()
        detail = mock_append.call_args[1].get("detail") or mock_append.call_args[0][3]
        self.assertNotIn("admin_bypass", detail)

    @patch("app.runtime.service.append_audit")
    def test_bypass_events_cleared_after_audit(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")
        self.assertEqual(len(ctx.bypass_events), 1)

        audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        self.assertEqual(ctx.bypass_events, [])

    @patch("app.runtime.service.append_audit")
    def test_original_detail_not_mutated(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")

        original_detail = {"thread_id": "t1"}
        audit_event(settings, ctx, "message_send", original_detail)

        self.assertNotIn("admin_bypass", original_detail)

    @patch("app.runtime.service.append_audit")
    def test_allow_all_auth_stub_does_not_crash(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        stub = AllowAllAuthStub()

        audit_event(settings, stub, "test_event", {"key": "value"})

        mock_append.assert_called_once()
        detail = mock_append.call_args[1].get("detail") or mock_append.call_args[0][3]
        self.assertNotIn("admin_bypass", detail)


# ---------------------------------------------------------------------------
# Default field backward compatibility
# ---------------------------------------------------------------------------


class TestBypassFieldBackwardCompatibility(unittest.TestCase):
    """Verify that the new field does not break existing constructor patterns."""

    def test_default_bypass_events_is_empty_list(self) -> None:
        ctx = _ctx()
        self.assertEqual(ctx.bypass_events, [])

    def test_bypass_events_not_shared_between_instances(self) -> None:
        ctx1 = _ctx(scopes={"admin:peers"})
        ctx2 = _ctx(scopes={"admin:peers"})
        ctx1.require("write:messages")
        self.assertEqual(len(ctx1.bypass_events), 1)
        self.assertEqual(len(ctx2.bypass_events), 0)


if __name__ == "__main__":
    unittest.main()
