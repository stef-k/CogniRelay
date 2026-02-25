import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.responses import Response

from app.auth import AuthContext
from app.config import Settings
from app.main import mcp_rpc, well_known_mcp


class _GitManagerStub:
    def commit_file(self, _path: Path, _message: str) -> bool:
        return True

    def latest_commit(self) -> str:
        return "test-sha"


class _RequestStub:
    class _Client:
        def __init__(self, host: str) -> None:
            self.host = host

    def __init__(self, host: str) -> None:
        self.client = self._Client(host)


class TestMcpRpcCompatibility(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_well_known_mcp_descriptor(self) -> None:
        payload = well_known_mcp()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["endpoint"], "/v1/mcp")
        self.assertIn("initialize", payload["methods"])
        self.assertIn("notifications/initialized", payload["methods"])
        self.assertIn("tools/list", payload["methods"])
        self.assertIn("tools/call", payload["methods"])

    def test_initialize(self) -> None:
        req = {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "initialize",
            "params": {"protocolVersion": "2026-02-25", "clientInfo": {"name": "agent-x"}},
        }
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 99)
        self.assertEqual(res["result"]["protocolVersion"], "2026-02-25")
        self.assertIn("tools", res["result"]["capabilities"])

    def test_notifications_initialized_no_response_body(self) -> None:
        req = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        res = mcp_rpc(req)
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 204)

    def test_ping(self) -> None:
        req = {"jsonrpc": "2.0", "id": 5, "method": "ping", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 5)
        self.assertTrue(res["result"]["ok"])

    def test_tools_list(self) -> None:
        req = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["jsonrpc"], "2.0")
        self.assertEqual(res["id"], 1)
        tools = res["result"]["tools"]
        by_name = {t["name"]: t for t in tools}
        self.assertIn("system.discovery", by_name)
        self.assertIn("memory.write", by_name)
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

    def test_tools_call_system_manifest_without_auth(self) -> None:
        req = {
            "jsonrpc": "2.0",
            "id": "abc",
            "method": "tools/call",
            "params": {"name": "system.manifest", "arguments": {}},
        }
        res = mcp_rpc(req)
        self.assertIn("result", res)
        self.assertEqual(res["result"]["toolName"], "system.manifest")
        structured = res["result"]["structuredContent"]
        self.assertIn("endpoints", structured)

    def test_tools_call_protected_tool_requires_auth(self) -> None:
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
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("error", res)
        self.assertEqual(res["error"]["code"], -32002)

    def test_tools_call_ops_catalog_allowed_when_local(self) -> None:
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
                res = mcp_rpc(req, authorization="Bearer token")

        self.assertIn("result", res)
        structured = res["result"]["structuredContent"]
        self.assertTrue(structured["ok"])
        self.assertTrue(structured["local_only"])

    def test_mcp_rpc_passes_request_to_auth(self) -> None:
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
                res = mcp_rpc(req, authorization="Bearer token", http_request=_RequestStub("127.0.0.1"))

        self.assertIn("result", res)
        self.assertEqual(res["result"]["toolName"], "metrics.get")
        self.assertEqual(seen["host"], "127.0.0.1")

    def test_invalid_method(self) -> None:
        req = {"jsonrpc": "2.0", "id": "x", "method": "tools/unknown", "params": {}}
        res = mcp_rpc(req)
        self.assertEqual(res["error"]["code"], -32601)

    def test_batch_request(self) -> None:
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
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["id"], 1)
        self.assertEqual(res[1]["id"], 2)
        self.assertIn("result", res[1])

    def test_batch_notifications_only_returns_204(self) -> None:
        req = [
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        ]
        res = mcp_rpc(req)
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 204)


if __name__ == "__main__":
    unittest.main()
