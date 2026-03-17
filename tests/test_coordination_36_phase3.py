"""Tests for Issue #36 Phase 3 discovery and MCP exposure."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.auth import AuthContext
from app.config import Settings
from app.main import discovery_tools, manifest, mcp_rpc
from tests.helpers import SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub used by MCP coordination tests."""


class _RequestStub:
    """Minimal request object used by MCP auth resolution tests."""

    class _Client:
        """Simple client holder exposing a host field."""

        def __init__(self, host: str) -> None:
            """Store the fake client host."""
            self.host = host

    def __init__(self, host: str) -> None:
        """Construct the request stub with the desired host."""
        self.client = self._Client(host)


class TestCoordination36Phase3(unittest.TestCase):
    """Validate discovery and MCP exposure for the coordination handoff surface."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for coordination discovery tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_discovery_tools_include_coordination_handoff_surface(self) -> None:
        """Discovery tools should expose the bounded handoff endpoints and schemas."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}

        self.assertIn("coordination.handoff_create", by_name)
        self.assertIn("coordination.handoff_read", by_name)
        self.assertIn("coordination.handoffs_query", by_name)
        self.assertIn("coordination.handoff_consume", by_name)

        create_schema = by_name["coordination.handoff_create"]["input_schema"]
        self.assertIn("recipient_peer", create_schema.get("properties", {}))
        self.assertIn("subject_kind", create_schema.get("properties", {}))
        query_schema = by_name["coordination.handoffs_query"]["input_schema"]
        self.assertIn("recipient_peer", query_schema.get("properties", {}))
        self.assertIn("sender_peer", query_schema.get("properties", {}))
        consume_schema = by_name["coordination.handoff_consume"]["input_schema"]
        self.assertIn("handoff_id", consume_schema.get("properties", {}))
        self.assertIn("status", consume_schema.get("properties", {}))

    def test_manifest_exposes_coordination_endpoints(self) -> None:
        """Manifest should list the new handoff endpoints without implying shared-state mutation."""
        endpoints = manifest()["endpoints"]
        self.assertIn("POST /v1/coordination/handoff/create", endpoints)
        self.assertIn("GET /v1/coordination/handoff/{handoff_id}", endpoints)
        self.assertIn("GET /v1/coordination/handoffs/query", endpoints)
        self.assertIn("POST /v1/coordination/handoff/{handoff_id}/consume", endpoints)

    def test_mcp_tools_call_can_read_handoff_artifact(self) -> None:
        """MCP tool dispatch should expose handoff read through the new discovery tool."""
        req = {
            "jsonrpc": "2.0",
            "id": 81,
            "method": "tools/call",
            "params": {
                "name": "coordination.handoff_read",
                "arguments": {"handoff_id": "handoff_1234567890abcdef1234567890abcdef"},
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-beta",
            scopes={"read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            handoff_dir = repo_root / "memory" / "coordination" / "handoffs"
            handoff_dir.mkdir(parents=True, exist_ok=True)
            (handoff_dir / "handoff_1234567890abcdef1234567890abcdef.json").write_text(
                json.dumps(
                    {
                        "schema_type": "continuity_handoff",
                        "schema_version": "1.0",
                        "handoff_id": "handoff_1234567890abcdef1234567890abcdef",
                        "created_at": "2026-03-17T12:00:00Z",
                        "created_by": "peer-alpha",
                        "sender_peer": "peer-alpha",
                        "recipient_peer": "peer-beta",
                        "source_selector": {"subject_kind": "task", "subject_id": "build-phase-5a"},
                        "source_summary": {
                            "path": "memory/continuity/task-build-phase-5a.json",
                            "updated_at": "2026-03-17T10:00:00Z",
                            "verified_at": "2026-03-17T10:00:00Z",
                            "verification_status": "peer_confirmed",
                            "health_status": "healthy",
                        },
                        "task_id": "task-123",
                        "thread_id": "thread-abc",
                        "note": "Advisory handoff only.",
                        "shared_continuity": {
                            "active_constraints": ["Do not weaken durability guarantees."],
                            "drift_signals": ["Pending review."],
                        },
                        "recipient_status": "pending",
                        "recipient_reason": None,
                        "consumed_at": None,
                        "consumed_by": None,
                    }
                ),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                res = mcp_rpc(req, authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))

        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["handoff"]["recipient_peer"], "peer-beta")


if __name__ == "__main__":
    unittest.main()
