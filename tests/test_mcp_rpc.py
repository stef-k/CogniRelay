"""Tests for MCP-compatible RPC handling and tool dispatch behavior."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.responses import Response

from app.auth import AuthContext
from app.config import Settings
from app.main import app, mcp_rpc, well_known_mcp
from app.mcp.service import reset_bootstrap_state
from tests.helpers import SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that pretends every file commit succeeds."""


class _RequestStub:
    """Minimal request object used to exercise request-aware auth paths."""

    class _Client:
        """Simple client holder exposing only a host field."""

        def __init__(self, host: str) -> None:
            """Store the client host used by the request stub."""
            self.host = host

    def __init__(self, host: str) -> None:
        """Construct the request stub with the desired client host."""
        self.client = self._Client(host)


class TestMcpRpcCompatibility(unittest.TestCase):
    """Validate the MCP-compatible JSON-RPC surface and edge cases."""

    def setUp(self) -> None:
        """Reset shared bootstrap state before each test."""
        reset_bootstrap_state()

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

    def _bootstrap(self, *, authorization: str | None = None, http_request=None) -> None:
        """Advance one caller context through the MCP bootstrap flow."""
        init = mcp_rpc(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            authorization=authorization,
            http_request=http_request,
        )
        self.assertIn("result", init)
        notify = mcp_rpc(
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            authorization=authorization,
            http_request=http_request,
        )
        self.assertIsInstance(notify, Response)
        self.assertEqual(notify.status_code, 204)

    def test_well_known_mcp_descriptor(self) -> None:
        """The well-known descriptor should advertise the MCP endpoint surface."""
        payload = well_known_mcp()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["endpoint"], "/v1/mcp")
        self.assertIn("initialize", payload["methods"])
        self.assertIn("notifications/initialized", payload["methods"])
        self.assertIn("tools/list", payload["methods"])
        self.assertIn("tools/call", payload["methods"])

    def test_initialize(self) -> None:
        """Initialize should return the exact slice-2 protocol baseline."""
        req = {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "clientInfo": {"name": "agent-x"}},
        }
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 99)
        self.assertEqual(res["result"]["protocolVersion"], "2025-11-25")
        self.assertEqual(res["result"]["capabilities"], {"tools": {"listChanged": False}})
        self.assertNotIn("instructions", res["result"])

    def test_http_initialize_accepts_jsonrpc_body(self) -> None:
        """The generated HTTP contract should require a JSON request body for MCP initialize."""
        operation = app.openapi()["paths"]["/v1/mcp"]["post"]
        self.assertIn("requestBody", operation)
        self.assertTrue(operation["requestBody"]["required"])
        self.assertIn("application/json", operation["requestBody"]["content"])
        parameters = operation.get("parameters", [])
        payload_query_params = [p for p in parameters if p.get("name") == "payload" and p.get("in") == "query"]
        self.assertEqual(payload_query_params, [])

    def test_notifications_initialized_no_response_body(self) -> None:
        """Initialization notifications should return an empty 204 response."""
        req = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        res = mcp_rpc(req)
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 204)

    def test_ping(self) -> None:
        """Ping should return a basic success payload."""
        req = {"jsonrpc": "2.0", "id": 5, "method": "ping", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 5)
        self.assertTrue(res["result"]["ok"])

    def test_tools_list(self) -> None:
        """Tools list should expose the expected public tool catalog."""
        self._bootstrap()
        req = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 1)
        tools = res["result"]["tools"]
        by_name = {t["name"]: t for t in tools}
        self.assertIn("system.discovery", by_name)
        self.assertIn("memory.write", by_name)
        self.assertIn("recent.list", by_name)
        self.assertIn("continuity.read", by_name)
        self.assertIn("continuity.compare", by_name)
        self.assertIn("continuity.revalidate", by_name)
        self.assertIn("continuity.refresh_plan", by_name)
        self.assertIn("continuity.list", by_name)
        self.assertIn("continuity.archive", by_name)
        self.assertIn("continuity.delete", by_name)
        self.assertIn("peers.list", by_name)
        self.assertIn("context.snapshot_create", by_name)
        self.assertIn("tasks.create", by_name)
        self.assertIn("docs.patch_propose", by_name)
        self.assertIn("code.checks_run", by_name)
        self.assertIn("code.merge", by_name)
        self.assertIn("security.tokens_list", by_name)
        self.assertIn("security.tokens_issue", by_name)
        self.assertIn("security.tokens_revoke", by_name)
        self.assertIn("security.tokens_rotate", by_name)
        self.assertIn("security.keys_rotate", by_name)
        self.assertIn("messages.verify", by_name)
        self.assertIn("metrics.get", by_name)
        self.assertIn("messages.replay", by_name)
        self.assertIn("replication.pull", by_name)
        self.assertIn("replication.push", by_name)
        self.assertIn("system.contracts", by_name)
        self.assertIn("system.governance_policy", by_name)
        self.assertIn("peers.trust_transition", by_name)
        self.assertIn("backup.create", by_name)
        self.assertIn("backup.restore_test", by_name)
        self.assertIn("ops.catalog", by_name)
        self.assertIn("ops.status", by_name)
        self.assertIn("ops.run", by_name)
        self.assertIn("ops.schedule_export", by_name)
        self.assertIn("inputSchema", by_name["memory.write"])

    def test_tools_call_continuity_read_with_auth(self) -> None:
        """Continuity read should be invokable through MCP tool dispatch."""
        req = {
            "jsonrpc": "2.0",
            "id": 74,
            "method": "tools/call",
            "params": {
                "name": "continuity.read",
                "arguments": {"subject_kind": "user", "subject_id": "stef"},
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "subject_kind": "user",
                        "subject_id": "stef",
                        "updated_at": "2026-03-15T14:30:22Z",
                        "verified_at": "2026-03-15T14:30:22Z",
                        "verification_kind": "self_review",
                        "source": {
                            "producer": "handoff-hook",
                            "update_reason": "pre_compaction",
                            "inputs": ["memory/core/identity.md"],
                        },
                        "continuity": {
                            "top_priorities": ["reply"],
                            "active_concerns": ["none"],
                            "active_constraints": ["stay deterministic"],
                            "open_loops": ["follow up"],
                            "stance_summary": "keep context stable",
                            "drift_signals": [],
                        },
                        "confidence": {"continuity": 0.82, "relationship_model": 0.0},
                    }
                ),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["capsule"]["subject_id"], "stef")

    def test_tools_call_continuity_compare_with_auth(self) -> None:
        """Continuity compare should be invokable through MCP tool dispatch."""
        candidate_capsule = {
            "schema_version": "1.0",
            "subject_kind": "user",
            "subject_id": "stef",
            "updated_at": "2026-03-15T14:30:22Z",
            "verified_at": "2026-03-15T14:30:22Z",
            "verification_kind": "self_review",
            "source": {
                "producer": "handoff-hook",
                "update_reason": "pre_compaction",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["reply"],
                "active_concerns": ["none"],
                "active_constraints": ["stay deterministic"],
                "open_loops": ["follow up"],
                "stance_summary": "keep context stable",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.82, "relationship_model": 0.0},
        }
        req = {
            "jsonrpc": "2.0",
            "id": 75,
            "method": "tools/call",
            "params": {
                "name": "continuity.compare",
                "arguments": {
                    "subject_kind": "user",
                    "subject_id": "stef",
                    "candidate_capsule": candidate_capsule,
                    "signals": [
                        {
                            "kind": "system_check",
                            "source_ref": "checks/continuity.json",
                            "observed_at": "2026-03-15T14:30:22Z",
                            "summary": "verification passed",
                        }
                    ],
                },
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(json.dumps(candidate_capsule), encoding="utf-8")
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertTrue(structured["identical"])
        self.assertEqual(structured["recommended_outcome"], "confirm")

    def test_tools_call_continuity_refresh_plan_with_auth(self) -> None:
        """Continuity refresh planning should be invokable through MCP tool dispatch."""
        req = {
            "jsonrpc": "2.0",
            "id": 77,
            "method": "tools/call",
            "params": {
                "name": "continuity.refresh_plan",
                "arguments": {"limit": 5},
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"read:files", "write:projects"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["count"], 0)

    def test_tools_call_continuity_delete_with_auth(self) -> None:
        """Continuity delete should be invokable through MCP tool dispatch."""
        req = {
            "jsonrpc": "2.0",
            "id": 78,
            "method": "tools/call",
            "params": {
                "name": "continuity.delete",
                "arguments": {
                    "subject_kind": "user",
                    "subject_id": "stef",
                    "delete_active": True,
                    "reason": "cleanup",
                },
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"write:projects", "read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "subject_kind": "user",
                        "subject_id": "stef",
                        "updated_at": "2026-03-15T14:30:22Z",
                        "verified_at": "2026-03-15T14:30:22Z",
                        "verification_kind": "self_review",
                        "source": {
                            "producer": "handoff-hook",
                            "update_reason": "pre_compaction",
                            "inputs": ["memory/core/identity.md"],
                        },
                        "continuity": {
                            "top_priorities": ["reply"],
                            "active_concerns": ["none"],
                            "active_constraints": ["stay deterministic"],
                            "open_loops": ["follow up"],
                            "stance_summary": "keep context stable",
                            "drift_signals": [],
                        },
                        "confidence": {"continuity": 0.82, "relationship_model": 0.0},
                    }
                ),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["deleted_paths"], ["memory/continuity/user-stef.json"])

    def test_tools_call_continuity_revalidate_with_auth(self) -> None:
        """Continuity revalidate should be invokable through MCP tool dispatch."""
        req = {
            "jsonrpc": "2.0",
            "id": 76,
            "method": "tools/call",
            "params": {
                "name": "continuity.revalidate",
                "arguments": {
                    "subject_kind": "user",
                    "subject_id": "stef",
                    "outcome": "confirm",
                    "signals": [
                        {
                            "kind": "peer_confirmation",
                            "source_ref": "messages/thread/peer-confirmation.json",
                            "observed_at": "2026-03-15T14:30:22Z",
                            "summary": "peer confirmed context",
                        }
                    ],
                },
            },
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"write:projects", "read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "subject_kind": "user",
                        "subject_id": "stef",
                        "updated_at": "2026-03-15T14:30:22Z",
                        "verified_at": "2026-03-15T14:30:22Z",
                        "verification_kind": "self_review",
                        "source": {
                            "producer": "handoff-hook",
                            "update_reason": "pre_compaction",
                            "inputs": ["memory/core/identity.md"],
                        },
                        "continuity": {
                            "top_priorities": ["reply"],
                            "active_concerns": ["none"],
                            "active_constraints": ["stay deterministic"],
                            "open_loops": ["follow up"],
                            "stance_summary": "keep context stable",
                            "drift_signals": [],
                        },
                        "confidence": {"continuity": 0.82, "relationship_model": 0.0},
                    }
                ),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertEqual(structured["outcome"], "confirm")
        self.assertEqual(structured["verification_state"]["status"], "peer_confirmed")

    def test_tools_call_system_manifest_without_auth(self) -> None:
        """Public tools should be invokable without auth when designed that way."""
        self._bootstrap()
        req = {
            "jsonrpc": "2.0",
            "id": "abc",
            "method": "tools/call",
            "params": {"name": "system.manifest", "arguments": {}},
        }
        res = mcp_rpc(req)
        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertIn("endpoints", structured)
        self.assertEqual(sorted(res["result"].keys()), ["content", "structuredContent"])

    def test_tools_call_protected_tool_requires_auth(self) -> None:
        """Protected tools should fail with an auth error when auth is absent."""
        self._bootstrap()
        req = {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "memory.read", "arguments": {"path": "memory/core/identity.md"}},
        }
        res = mcp_rpc(req)
        self.assertIn("error", res)
        self.assertEqual(res["error"]["code"], -32001)

    def test_tools_call_ops_catalog_forbidden_when_non_local(self) -> None:
        """Local-only ops tools should reject non-local callers."""
        req = {
            "jsonrpc": "2.0",
            "id": 71,
            "method": "tools/call",
            "params": {"name": "ops.catalog", "arguments": {}},
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"admin:peers"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="10.1.2.3",
        )

        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("error", res)
        self.assertEqual(res["error"]["code"], -32002)

    def test_tools_call_ops_catalog_allowed_when_local(self) -> None:
        """Local-only ops tools should work for loopback callers."""
        req = {
            "jsonrpc": "2.0",
            "id": 72,
            "method": "tools/call",
            "params": {"name": "ops.catalog", "arguments": {}},
        }
        auth = AuthContext(
            token="token",
            peer_id="peer-host",
            scopes={"admin:peers"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                self._bootstrap(authorization="Bearer token")
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertTrue(structured["local_only"])

    def test_mcp_rpc_passes_request_to_auth(self) -> None:
        """RPC auth resolution should receive the incoming HTTP request context."""
        req = {
            "jsonrpc": "2.0",
            "id": 73,
            "method": "tools/call",
            "params": {"name": "metrics.get", "arguments": {}},
        }
        seen: dict[str, str | None] = {"host": None}

        def _fake_require_auth(
            authorization: str | None = None,
            x_forwarded_for: str | None = None,
            x_real_ip: str | None = None,
            request=None,
        ) -> AuthContext:
            seen["host"] = request.client.host if request is not None and request.client is not None else None
            return AuthContext(
                token=str(authorization or "token"),
                peer_id="peer-host",
                scopes={"admin:peers", "read:index", "search"},
                read_namespaces={"*"},
                write_namespaces={"*"},
                client_ip="127.0.0.1",
            )

        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", side_effect=_fake_require_auth
            ):
                self._bootstrap(authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))
                res = mcp_rpc(req, authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))

        self.assertIn("result", res)
        self.assertEqual(seen["host"], "127.0.0.1")

    def test_invalid_method(self) -> None:
        """Unknown RPC methods should return the JSON-RPC method-not-found error."""
        req = {"jsonrpc": "2.0", "id": "x", "method": "tools/unknown", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["error"]["code"], -32601)

    def test_batch_request(self) -> None:
        """Batch requests should be rejected in slice 2."""
        req = [
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "system.discovery", "arguments": {}},
            },
        ]
        res = mcp_rpc(req)
        self.assertEqual(res["error"]["code"], -32600)
        self.assertEqual(res["error"]["data"], {"reason": "batch requests are not supported"})

    def test_batch_notifications_only_returns_204(self) -> None:
        """Notification-only batches are still invalid in slice 2."""
        req = [
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        ]
        res = mcp_rpc(req)
        self.assertEqual(res["error"]["code"], -32600)
        self.assertEqual(res["error"]["data"], {"reason": "batch requests are not supported"})

    def test_empty_batch_returns_invalid_request_error(self) -> None:
        """Empty batches should return the JSON-RPC invalid-request error."""
        res = mcp_rpc([])
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertIsNone(res["id"])
        self.assertEqual(res["error"]["code"], -32600)
        self.assertEqual(res["error"]["message"], "Invalid Request")
        self.assertEqual(res["error"]["data"], {"reason": "batch requests are not supported"})


if __name__ == "__main__":
    unittest.main()
