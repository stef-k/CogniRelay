"""Tests for Issue #37 Phase 3 discovery and MCP exposure."""

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
    """Git stub used by shared coordination MCP tests."""


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


class TestCoordination37Phase3(unittest.TestCase):
    """Validate discovery and MCP exposure for the shared coordination surface."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for shared coordination discovery tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _artifact(self) -> dict:
        """Return one valid stored shared coordination artifact payload."""
        return {
            "schema_type": "coordination_shared_state",
            "schema_version": "1.0",
            "shared_id": "shared_0123456789abcdef0123456789abcdef",
            "created_at": "2026-03-17T12:00:00Z",
            "updated_at": "2026-03-17T12:00:00Z",
            "created_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Retry slice coordination",
            "summary": "Shared constraints and drift signals.",
            "shared_state": {
                "constraints": ["Do not weaken durability guarantees."],
                "drift_signals": ["External review may invalidate timing assumptions."],
                "coordination_alerts": ["One participant reports missing context."],
            },
            "version": 1,
            "last_updated_by": "peer-alpha",
        }

    def test_discovery_tools_include_shared_coordination_surface(self) -> None:
        """Discovery tools should expose the bounded shared coordination endpoints and schemas."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}

        self.assertIn("coordination.shared_create", by_name)
        self.assertIn("coordination.shared_read", by_name)
        self.assertIn("coordination.shared_query", by_name)
        self.assertIn("coordination.shared_update", by_name)

        create_schema = by_name["coordination.shared_create"]["input_schema"]
        self.assertIn("participant_peers", create_schema.get("properties", {}))
        self.assertIn("constraints", create_schema.get("properties", {}))
        query_schema = by_name["coordination.shared_query"]["input_schema"]
        self.assertIn("owner_peer", query_schema.get("properties", {}))
        self.assertIn("participant_peer", query_schema.get("properties", {}))
        update_schema = by_name["coordination.shared_update"]["input_schema"]
        self.assertIn("shared_id", update_schema.get("properties", {}))
        self.assertIn("expected_version", update_schema.get("properties", {}))
        self.assertNotIn("reconciliation", by_name["coordination.shared_query"]["description"].lower())
        self.assertNotIn("consensus", by_name["coordination.shared_create"]["description"].lower())

    def test_manifest_exposes_shared_coordination_endpoints(self) -> None:
        """Manifest should list the Phase 5B shared coordination endpoints."""
        endpoints = manifest()["endpoints"]
        self.assertIn("POST /v1/coordination/shared/create", endpoints)
        self.assertIn("GET /v1/coordination/shared/{shared_id}", endpoints)
        self.assertIn("GET /v1/coordination/shared/query", endpoints)
        self.assertIn("POST /v1/coordination/shared/{shared_id}/update", endpoints)

    def test_mcp_tools_call_can_query_shared_artifacts(self) -> None:
        """MCP tool dispatch should expose shared query through the discovery tool catalog."""
        req = {
            "jsonrpc": "2.0",
            "id": 91,
            "method": "tools/call",
            "params": {
                "name": "coordination.shared_query",
                "arguments": {
                    "participant_peer": "peer-beta",
                    "offset": 0,
                    "limit": 20,
                },
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
            shared_dir = repo_root / "memory" / "coordination" / "shared"
            shared_dir.mkdir(parents=True, exist_ok=True)
            (shared_dir / "shared_0123456789abcdef0123456789abcdef.json").write_text(
                json.dumps(self._artifact()),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                res = mcp_rpc(req, authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))

        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["count"], 1)
        self.assertEqual(structured["shared_artifacts"][0]["owner_peer"], "peer-alpha")

    def test_mcp_tools_call_can_update_shared_artifact(self) -> None:
        """MCP tool dispatch should expose shared update through the discovery tool catalog."""
        req = {
            "jsonrpc": "2.0",
            "id": 92,
            "method": "tools/call",
            "params": {
                "name": "coordination.shared_update",
                "arguments": {
                    "shared_id": "shared_0123456789abcdef0123456789abcdef",
                    "expected_version": 1,
                    "title": "Retry slice coordination v2",
                    "summary": "Updated shared constraints and alerts.",
                    "constraints": ["Do not weaken durability guarantees.", "Do not bypass rollback safety."],
                    "drift_signals": ["External review may invalidate timing assumptions."],
                    "coordination_alerts": ["Waiting on owner confirmation."],
                },
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-alpha",
            scopes={"write:projects"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            shared_dir = repo_root / "memory" / "coordination" / "shared"
            shared_dir.mkdir(parents=True, exist_ok=True)
            (shared_dir / "shared_0123456789abcdef0123456789abcdef.json").write_text(
                json.dumps(self._artifact()),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                res = mcp_rpc(req, authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))

        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertTrue(structured["updated"])
        self.assertEqual(structured["shared"]["version"], 2)
        self.assertEqual(structured["shared"]["last_updated_by"], "peer-alpha")


if __name__ == "__main__":
    unittest.main()
