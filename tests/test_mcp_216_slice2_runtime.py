"""Normative runtime tests for #216 slice 2 MCP behavior."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.auth import AuthContext
from app.config import Settings
from app.main import app
from app.mcp.service import reset_bootstrap_state
from tests.helpers import SimpleGitManagerStub


class TestMcp216Slice2Runtime(unittest.TestCase):
    """Validate the exact slice-2 MCP runtime contract."""

    _CALLER_A_AUTH = "Bearer caller-a"
    _CALLER_B_AUTH = "Bearer caller-b"

    @classmethod
    def setUpClass(cls) -> None:
        """Create one HTTP client for the app."""
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        """Close the shared HTTP client."""
        cls._client_context.__exit__(None, None, None)

    def setUp(self) -> None:
        """Reset bootstrap state between tests."""
        reset_bootstrap_state()

    def _settings(self, repo_root: Path) -> Settings:
        """Build repo-rooted settings for MCP runtime tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _bootstrap(self, *, headers: dict[str, str] | None = None) -> None:
        """Advance the MCP bootstrap flow to the ready state."""
        headers = dict(headers or {})
        initialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            headers=headers,
        )
        self.assertEqual(initialize.status_code, 200)
        notify = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            headers=headers,
        )
        self.assertEqual(notify.status_code, 204)

    def test_get_v1_mcp_returns_405_with_allow_post(self) -> None:
        """GET /v1/mcp must stay deferred in slice 2."""
        response = self.client.get("/v1/mcp")
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.headers["allow"], "POST")

    def test_post_invalid_json_returns_exact_parse_error(self) -> None:
        """Invalid JSON must return the exact parse-error mapping."""
        response = self.client.post(
            "/v1/mcp",
            data="{",
            headers={"content-type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": {"reason": "request body must be valid JSON"},
                },
            },
        )

    def test_post_array_body_rejects_batch_requests(self) -> None:
        """Slice 2 must reject JSON-RPC batch requests."""
        response = self.client.post("/v1/mcp", json=[])
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32600,
                    "message": "Invalid Request",
                    "data": {"reason": "batch requests are not supported"},
                },
            },
        )

    def test_post_origin_validation_rejects_non_loopback_origin(self) -> None:
        """A present non-loopback Origin must be denied before method dispatch."""
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}},
            headers={"origin": "https://example.com"},
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32002,
                    "message": "Forbidden",
                    "data": {"reason": "origin not allowed", "origin": "https://example.com"},
                },
            },
        )

    def test_initialize_enforces_exact_protocol_and_bootstrap_gating(self) -> None:
        """Initialize must use the exact request, result, and state-transition rules."""
        headers = {"authorization": self._CALLER_A_AUTH}
        pre_init = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers=headers,
        )
        self.assertEqual(pre_init.status_code, 200)
        self.assertEqual(
            pre_init.json(),
            {
                "jsonrpc": "2.0",
                "id": 2,
                "error": {
                    "code": -32000,
                    "message": "Server not initialized",
                    "data": {"required_step": "initialize"},
                },
            },
        )

        initialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25", "capabilities": {}},
            },
            headers=headers,
        )
        self.assertEqual(initialize.status_code, 200)
        payload = initialize.json()
        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertEqual(payload["id"], 3)
        self.assertEqual(list(payload["result"].keys()), ["protocolVersion", "capabilities", "serverInfo"])
        self.assertEqual(payload["result"]["protocolVersion"], "2025-11-25")
        self.assertEqual(payload["result"]["capabilities"], {"tools": {"listChanged": False}})
        self.assertEqual(payload["result"]["serverInfo"]["name"], "cognirelay")
        self.assertTrue(payload["result"]["serverInfo"]["version"])

        pre_notify = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
            headers=headers,
        )
        self.assertEqual(pre_notify.status_code, 200)
        self.assertEqual(
            pre_notify.json(),
            {
                "jsonrpc": "2.0",
                "id": 4,
                "error": {
                    "code": -32000,
                    "message": "Server not initialized",
                    "data": {"required_step": "notifications/initialized"},
                },
            },
        )

    def test_initialize_rejects_unsupported_protocol_without_advancing_state(self) -> None:
        """Unsupported protocol versions must not advance bootstrap state."""
        rejected = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 7,
                "method": "initialize",
                "params": {"protocolVersion": "2024-01-01"},
            },
        )
        self.assertEqual(rejected.status_code, 200)
        self.assertEqual(
            rejected.json(),
            {
                "jsonrpc": "2.0",
                "id": 7,
                "error": {
                    "code": -32602,
                    "message": "Unsupported protocol version",
                    "data": {"supported": ["2025-11-25"], "requested": "2024-01-01"},
                },
            },
        )

        gated = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 8, "method": "tools/list", "params": {}},
        )
        self.assertEqual(gated.status_code, 200)
        self.assertEqual(gated.json()["error"]["data"], {"required_step": "initialize"})

    def test_notifications_initialized_requires_notification_form(self) -> None:
        """Only notification-form notifications/initialized is valid."""
        self._bootstrap()
        reset_bootstrap_state()

        initialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 10,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
        )
        self.assertEqual(initialize.status_code, 200)

        invalid = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 11, "method": "notifications/initialized", "params": {}},
        )
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(
            invalid.json(),
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32600,
                    "message": "Invalid Request",
                    "data": {"reason": "notifications/initialized is notification-only"},
                },
            },
        )

    def test_tools_list_rejects_non_empty_cursor_and_omits_next_cursor(self) -> None:
        """Slice 2 supports only the first tools/list page."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})

        bad_cursor = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/list",
                "params": {"cursor": "page-2"},
            },
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(bad_cursor.status_code, 200)
        self.assertEqual(
            bad_cursor.json(),
            {
                "jsonrpc": "2.0",
                "id": 12,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "cursor pagination is not supported in slice 2"},
                },
            },
        )

        success = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 13, "method": "tools/list", "params": {"cursor": ""}},
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(success.status_code, 200)
        payload = success.json()
        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertEqual(payload["id"], 13)
        self.assertEqual(list(payload["result"].keys()), ["tools"])
        self.assertNotIn("nextCursor", payload["result"])

    def test_tools_call_uses_exact_success_shape(self) -> None:
        """Successful non-help tools/call responses must expose only content and structuredContent."""
        headers = {"authorization": self._CALLER_A_AUTH}
        auth = AuthContext(
            token="token",
            peer_id="peer-test",
            scopes=set(),
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )
        with patch("app.main.require_auth", return_value=auth):
            self._bootstrap(headers=headers)
            response = self.client.post(
                "/v1/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": "discovery",
                    "method": "tools/call",
                    "params": {"name": "system.discovery", "arguments": {}},
                },
                headers=headers,
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertEqual(payload["id"], "discovery")
        self.assertEqual(sorted(payload["result"].keys()), ["content", "structuredContent"])
        self.assertEqual(payload["result"]["content"][0]["type"], "text")
        self.assertTrue(payload["result"]["content"][0]["text"])
        self.assertNotIn("toolName", payload["result"])

    def test_tools_call_maps_unknown_tool_and_schema_validation_errors(self) -> None:
        """tools/call failures must use the exact slice-2 JSON-RPC mappings."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})

        unknown = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 20,
                "method": "tools/call",
                "params": {"name": "missing.tool", "arguments": {}},
            },
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(unknown.status_code, 200)
        self.assertEqual(
            unknown.json(),
            {
                "jsonrpc": "2.0",
                "id": 20,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "unknown tool", "name": "missing.tool"},
                },
            },
        )

        with tempfile.TemporaryDirectory() as td:
            reset_bootstrap_state()
            self._bootstrap(headers={"authorization": "Bearer token"})
            settings = self._settings(Path(td))
            auth = AuthContext(
                token="token",
                peer_id="peer-test",
                scopes={"read:files"},
                read_namespaces={"*"},
                write_namespaces={"*"},
                client_ip="127.0.0.1",
            )
            with patch("app.main._services", return_value=(settings, SimpleGitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                invalid_args = self.client.post(
                    "/v1/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 21,
                        "method": "tools/call",
                        "params": {"name": "memory.read", "arguments": {}},
                    },
                    headers={"authorization": "Bearer token"},
                )

        self.assertEqual(invalid_args.status_code, 200)
        payload = invalid_args.json()
        self.assertEqual(payload["error"]["code"], -32602)
        self.assertEqual(payload["error"]["message"], "Invalid params")
        self.assertEqual(payload["error"]["data"]["reason"], "schema validation failed")
        self.assertTrue(payload["error"]["data"]["details"])

    def test_tools_call_maps_unauthorized_and_forbidden_failures(self) -> None:
        """Auth failures must use the exact unauthorized and forbidden codes."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})

        unauthorized = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {"name": "memory.read", "arguments": {"path": "memory/core/identity.md"}},
            },
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(unauthorized.status_code, 200)
        self.assertEqual(
            unauthorized.json(),
            {
                "jsonrpc": "2.0",
                "id": 30,
                "error": {
                    "code": -32001,
                    "message": "Unauthorized",
                    "data": {"reason": "authentication required"},
                },
            },
        )

        reset_bootstrap_state()
        self._bootstrap(headers={"authorization": "Bearer token"})
        auth = AuthContext(
            token="token",
            peer_id="peer-test",
            scopes={"admin:peers"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="10.1.2.3",
        )
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, SimpleGitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                forbidden = self.client.post(
                    "/v1/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 31,
                        "method": "tools/call",
                        "params": {"name": "ops.catalog", "arguments": {}},
                    },
                    headers={"authorization": "Bearer token"},
                )

        self.assertEqual(forbidden.status_code, 200)
        self.assertEqual(
            forbidden.json(),
            {
                "jsonrpc": "2.0",
                "id": 31,
                "error": {
                    "code": -32002,
                    "message": "Forbidden",
                    "data": {"reason": "forbidden"},
                },
            },
        )

    def test_initialize_null_present_values_use_exact_invalid_params_mapping(self) -> None:
        """Explicit null initialize fields must stay distinct from missing fields."""
        cases = [
            (
                {"protocolVersion": None},
                {"reason": "protocolVersion must be a string"},
            ),
            (
                {"protocolVersion": "2025-11-25", "capabilities": None},
                {"reason": "capabilities must be an object"},
            ),
            (
                {"protocolVersion": "2025-11-25", "clientInfo": None},
                {"reason": "clientInfo must be an object"},
            ),
        ]

        for request_id, (params, error_data) in enumerate(cases, start=40):
            with self.subTest(params=params):
                response = self.client.post(
                    "/v1/mcp",
                    json={"jsonrpc": "2.0", "id": request_id, "method": "initialize", "params": params},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32602,
                            "message": "Invalid params",
                            "data": error_data,
                        },
                    },
                )

    def test_tools_list_params_null_is_invalid_params(self) -> None:
        """Explicit null params for tools/list must not be treated as omitted."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 50, "method": "tools/list", "params": None},
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 50,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "params must be an object"},
                },
            },
        )

    def test_tools_call_arguments_null_is_invalid_params(self) -> None:
        """Explicit null arguments for tools/call must not be treated as omitted."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})
        response = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 51,
                "method": "tools/call",
                "params": {"name": "system.manifest", "arguments": None},
            },
            headers={"authorization": self._CALLER_A_AUTH},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 51,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "arguments must be an object"},
                },
            },
        )

    def test_bootstrap_state_is_isolated_by_caller_identity(self) -> None:
        """One caller's bootstrap state must not satisfy another caller."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})

        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 60, "method": "tools/list", "params": {}},
            headers={"authorization": self._CALLER_B_AUTH},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 60,
                "error": {
                    "code": -32000,
                    "message": "Server not initialized",
                    "data": {"required_step": "initialize"},
                },
            },
        )

    def test_anonymous_bootstrap_state_is_stateless_between_callers(self) -> None:
        """Anonymous bootstrap steps must not persist across callers."""
        caller_a_headers = {"x-forwarded-for": "198.51.100.10"}
        caller_b_headers = {"x-forwarded-for": "198.51.100.11"}

        initialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 61,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            headers=caller_a_headers,
        )
        self.assertEqual(initialize.status_code, 200)

        notify = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            headers=caller_a_headers,
        )
        self.assertEqual(notify.status_code, 204)

        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 62, "method": "tools/list", "params": {}},
            headers=caller_b_headers,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 62,
                "error": {
                    "code": -32000,
                    "message": "Server not initialized",
                    "data": {"required_step": "initialize"},
                },
            },
        )

    def test_reinitialize_after_ready_does_not_regress_caller_state(self) -> None:
        """A ready caller must not be pushed back into a waiting bootstrap phase."""
        headers = {"authorization": self._CALLER_A_AUTH}
        self._bootstrap(headers=headers)

        reinitialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 70,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            headers=headers,
        )
        self.assertEqual(reinitialize.status_code, 200)
        self.assertEqual(
            reinitialize.json(),
            {
                "jsonrpc": "2.0",
                "id": 70,
                "error": {
                    "code": -32000,
                    "message": "Server not initialized",
                    "data": {"required_step": "notifications/initialized"},
                },
            },
        )

        tools_list = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 71, "method": "tools/list", "params": {}},
            headers=headers,
        )
        self.assertEqual(tools_list.status_code, 200)
        self.assertIn("result", tools_list.json())

    def test_wrong_type_params_use_exact_invalid_params_mapping(self) -> None:
        """Wrong-type hardened branches must keep the exact invalid-params mapping."""
        self._bootstrap(headers={"authorization": self._CALLER_A_AUTH})

        cases = [
            (
                80,
                {
                    "jsonrpc": "2.0",
                    "id": 80,
                    "method": "initialize",
                    "params": {"protocolVersion": "2025-11-25", "capabilities": []},
                },
                None,
                {"reason": "capabilities must be an object"},
            ),
            (
                81,
                {
                    "jsonrpc": "2.0",
                    "id": 81,
                    "method": "tools/list",
                    "params": {"cursor": 1},
                },
                {"authorization": self._CALLER_A_AUTH},
                {"reason": "cursor must be a string or null"},
            ),
            (
                82,
                {
                    "jsonrpc": "2.0",
                    "id": 82,
                    "method": "tools/call",
                    "params": {"name": "system.manifest", "arguments": "wrong-type"},
                },
                {"authorization": self._CALLER_A_AUTH},
                {"reason": "arguments must be an object"},
            ),
        ]

        for request_id, payload, headers, error_data in cases:
            with self.subTest(method=payload["method"], error_data=error_data):
                response = self.client.post("/v1/mcp", json=payload, headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32602,
                            "message": "Invalid params",
                            "data": error_data,
                        },
                    },
                )


if __name__ == "__main__":
    unittest.main()
