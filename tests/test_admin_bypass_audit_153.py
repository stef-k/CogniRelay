"""Tests for admin:peers bypass audit visibility (issue #153)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

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

    def test_no_bypass_on_scope_denial(self) -> None:
        ctx = _ctx(scopes={"read:files"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require("write:messages")
        self.assertEqual(cm.exception.status_code, 403)
        self.assertEqual(ctx.bypass_events, [])


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

    def test_non_admin_non_matching_namespace_denied(self) -> None:
        ctx = _ctx(scopes={"read:files"}, read_namespaces={"memory"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("journal/entry.json")
        self.assertEqual(cm.exception.status_code, 403)
        self.assertEqual(ctx.bypass_events, [])

    def test_exact_namespace_match_no_bypass(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, read_namespaces={"memory"})
        ctx.require_read_path("memory")
        self.assertEqual(ctx.bypass_events, [])

    def test_mixed_scope_and_namespace_bypass(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, write_namespaces=set())
        ctx.require("write:messages")
        ctx.require_write_path("messages/outbox/peer-b.jsonl")
        self.assertEqual(len(ctx.bypass_events), 2)
        self.assertEqual(ctx.bypass_events[0]["kind"], "scope")
        self.assertEqual(ctx.bypass_events[1]["kind"], "namespace")

    def test_no_bypass_when_admin_peers_and_namespace_matches(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, read_namespaces={"memory"})
        ctx.require_read_path("memory/foo.json")
        self.assertEqual(ctx.bypass_events, [])

    def test_no_bypass_on_path_traversal_denial(self) -> None:
        ctx = _ctx(scopes={"admin:peers"})
        with self.assertRaises(HTTPException) as cm:
            ctx.require_read_path("../secret")
        self.assertEqual(cm.exception.status_code, 403)
        self.assertEqual(ctx.bypass_events, [])

    def test_require_path_alias_records_bypass(self) -> None:
        ctx = _ctx(scopes={"admin:peers"}, write_namespaces=set())
        ctx.require_path("journal/entry.json")
        self.assertEqual(len(ctx.bypass_events), 1)
        self.assertEqual(ctx.bypass_events[0]["kind"], "namespace")
        self.assertEqual(ctx.bypass_events[0]["mode"], "write")


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
    def test_bypass_events_persist_across_audit_calls(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")
        self.assertEqual(len(ctx.bypass_events), 1)

        audit_event(settings, ctx, "event_a", {"key": "a"})
        audit_event(settings, ctx, "event_b", {"key": "b"})

        # Both audit calls should carry the same bypass context.
        self.assertEqual(mock_append.call_count, 2)
        detail_a = mock_append.call_args_list[0][1].get("detail") or mock_append.call_args_list[0][0][3]
        detail_b = mock_append.call_args_list[1][1].get("detail") or mock_append.call_args_list[1][0][3]
        self.assertIn("admin_bypass", detail_a)
        self.assertIn("admin_bypass", detail_b)
        # Events remain on the AuthContext (per-request lifecycle).
        self.assertEqual(len(ctx.bypass_events), 1)

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

    @patch("app.runtime.service.append_audit")
    def test_auth_none_does_not_crash(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        audit_event(settings, None, "test_event", {"key": "value"})

        mock_append.assert_called_once()
        detail = mock_append.call_args[1].get("detail") or mock_append.call_args[0][3]
        self.assertNotIn("admin_bypass", detail)

    @patch("app.runtime.service.append_audit")
    def test_bypass_events_preserved_when_audit_disabled(self, _mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        settings.audit_log_enabled = False
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")
        self.assertEqual(len(ctx.bypass_events), 1)

        audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        # Events remain on the AuthContext (per-request lifecycle, GC'd at end).
        self.assertEqual(len(ctx.bypass_events), 1)
        _mock_append.assert_not_called()

    @patch("app.runtime.service.append_audit")
    def test_rollover_callback_carries_same_bypass_context(self, mock_append: MagicMock) -> None:
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")

        # Simulate append_audit triggering its audit callback once (rollover).
        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                cb = kwargs.get("audit")
                if cb is not None:
                    cb("audit_log_rollover", {"segment": "test"})

        mock_append.side_effect = side_effect
        audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        # Two calls: primary event + rollover event.
        self.assertEqual(mock_append.call_count, 2)
        primary_detail = mock_append.call_args_list[0][1].get("detail") or mock_append.call_args_list[0][0][3]
        rollover_detail = mock_append.call_args_list[1][1].get("detail") or mock_append.call_args_list[1][0][3]
        # Both carry bypass context since they share the same request.
        self.assertIn("admin_bypass", primary_detail)
        self.assertIn("admin_bypass", rollover_detail)

    def test_lost_bypass_warning_on_append_failure(self) -> None:
        from app.audit import WriteTimeRolloverError
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"admin:peers"})
        ctx.require("write:messages")

        with patch(
            "app.runtime.service.append_audit",
            side_effect=WriteTimeRolloverError("test", "disk full"),
        ):
            with self.assertLogs("app.runtime.service", level="WARNING") as log_ctx:
                audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        messages = "\n".join(log_ctx.output)
        self.assertIn("Audit append failed", messages)
        self.assertIn("Lost admin:peers bypass events", messages)

    def test_append_failure_without_bypass_no_lost_warning(self) -> None:
        from app.audit import WriteTimeRolloverError
        from app.runtime.service import audit_event

        settings = self._make_settings()
        ctx = _ctx(scopes={"write:messages"})
        ctx.require("write:messages")

        with patch(
            "app.runtime.service.append_audit",
            side_effect=WriteTimeRolloverError("test", "disk full"),
        ):
            with self.assertLogs("app.runtime.service", level="WARNING") as log_ctx:
                audit_event(settings, ctx, "message_send", {"thread_id": "t1"})

        messages = "\n".join(log_ctx.output)
        self.assertIn("Audit append failed", messages)
        self.assertNotIn("Lost admin:peers bypass events", messages)


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
