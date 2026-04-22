"""MCP help parity tests for issue #214 slice 2."""

from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.mcp.service import reset_bootstrap_state


EXPECTED_HELP_SCHEMAS = {
    "system.help": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "system.tool_usage": {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    "system.topic_help": {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
        "additionalProperties": False,
    },
    "system.hook_guide": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "system.error_guide": {
        "type": "object",
        "properties": {"code": {"type": "string"}},
        "required": ["code"],
        "additionalProperties": False,
    },
}


class TestMcp214Slice2Help(unittest.TestCase):
    """Validate the exact #214 slice-2 MCP help contract."""

    _HEADERS = {"authorization": "Bearer help-slice2"}

    @classmethod
    def setUpClass(cls) -> None:
        """Create one shared HTTP client."""
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        """Close the shared HTTP client."""
        cls._client_context.__exit__(None, None, None)

    def setUp(self) -> None:
        """Reset bootstrap state before each test."""
        reset_bootstrap_state()
        self._bootstrap()

    def _bootstrap(self) -> None:
        """Advance the MCP runtime to ready state."""
        initialize = self.client.post(
            "/v1/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            headers=self._HEADERS,
        )
        self.assertEqual(initialize.status_code, 200)
        notify = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            headers=self._HEADERS,
        )
        self.assertEqual(notify.status_code, 204)

    def _tools_call(self, name: str, *, arguments_marker: object = None, request_id: int = 20):
        """Invoke one MCP tool through the bounded tools/call transport."""
        params = {"name": name}
        if arguments_marker is not None:
            params["arguments"] = arguments_marker
        return self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": params},
            headers=self._HEADERS,
        )

    def test_tools_list_exposes_exact_help_tools_and_argument_schemas(self) -> None:
        """The five #214 help tools must be listed with their exact contracts."""
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers=self._HEADERS,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        listed = {
            tool["name"]: tool
            for tool in payload["result"]["tools"]
            if tool["name"].startswith("system.") and tool["metadata"]["path"].startswith("/v1/help")
        }

        self.assertEqual(set(listed), set(EXPECTED_HELP_SCHEMAS))
        self.assertEqual(listed["system.help"]["inputSchema"], EXPECTED_HELP_SCHEMAS["system.help"])
        self.assertEqual(listed["system.tool_usage"]["inputSchema"], EXPECTED_HELP_SCHEMAS["system.tool_usage"])
        self.assertEqual(listed["system.topic_help"]["inputSchema"], EXPECTED_HELP_SCHEMAS["system.topic_help"])
        self.assertEqual(listed["system.hook_guide"]["inputSchema"], EXPECTED_HELP_SCHEMAS["system.hook_guide"])
        self.assertEqual(listed["system.error_guide"]["inputSchema"], EXPECTED_HELP_SCHEMAS["system.error_guide"])

    def test_system_help_returns_exact_http_root_body_at_result(self) -> None:
        """system.help must place the closed HTTP body directly at result."""
        response = self._tools_call("system.help", arguments_marker={}, request_id=3)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["result"], self.client.get("/v1/help").json())
        self.assertNotIn("structuredContent", payload["result"])
        self.assertNotIn("content", payload["result"])

    def test_targeted_help_tools_return_exact_http_parity_bodies(self) -> None:
        """Every supported help target must round-trip the exact closed HTTP body."""
        cases = [
            ("system.tool_usage", {"name": "continuity.read"}, "/v1/help/tools/continuity.read"),
            ("system.tool_usage", {"name": "continuity.upsert"}, "/v1/help/tools/continuity.upsert"),
            ("system.tool_usage", {"name": "context.retrieve"}, "/v1/help/tools/context.retrieve"),
            (
                "system.topic_help",
                {"id": "continuity.read.startup_view"},
                "/v1/help/topics/continuity.read.startup_view",
            ),
            (
                "system.topic_help",
                {"id": "continuity.read.trust_signals"},
                "/v1/help/topics/continuity.read.trust_signals",
            ),
            (
                "system.topic_help",
                {"id": "continuity.upsert.session_end_snapshot"},
                "/v1/help/topics/continuity.upsert.session_end_snapshot",
            ),
            ("system.hook_guide", {}, "/v1/help/hooks"),
            ("system.error_guide", {"code": "validation"}, "/v1/help/errors/validation"),
            ("system.error_guide", {"code": "tool_not_found"}, "/v1/help/errors/tool_not_found"),
            ("system.error_guide", {"code": "unknown_help_topic"}, "/v1/help/errors/unknown_help_topic"),
        ]

        for index, (name, arguments, http_path) in enumerate(cases, start=30):
            with self.subTest(name=name, arguments=arguments, http_path=http_path):
                response = self._tools_call(name, arguments_marker=arguments, request_id=index)
                self.assertEqual(response.status_code, 200)
                http_response = self.client.get(http_path)
                self.assertEqual(http_response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["result"], http_response.json())
                self.assertNotIn("content", payload["result"])
                self.assertNotIn("structuredContent", payload["result"])

    def test_zero_argument_help_tools_require_explicit_empty_object(self) -> None:
        """system.help and system.hook_guide must reject omitted/non-empty arguments exactly."""
        omitted = self._tools_call("system.help", request_id=100)
        self.assertEqual(
            omitted.json(),
            {
                "jsonrpc": "2.0",
                "id": 100,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {
                        "error": {
                            "code": "validation",
                            "detail": "Arguments object is required.",
                            "validation_hints": [
                                {
                                    "field": "arguments",
                                    "area": "params.arguments",
                                    "reason": "arguments_required",
                                    "limit": None,
                                    "allowed_values": None,
                                    "correction_hint": "Provide arguments as an empty JSON object.",
                                }
                            ],
                        }
                    },
                },
            },
        )

        non_object = self._tools_call("system.hook_guide", arguments_marker=[], request_id=101)
        self.assertEqual(
            non_object.json(),
            {
                "jsonrpc": "2.0",
                "id": 101,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {
                        "error": {
                            "code": "validation",
                            "detail": "Arguments must be a JSON object.",
                            "validation_hints": [
                                {
                                    "field": "arguments",
                                    "area": "params.arguments",
                                    "reason": "arguments_must_be_object",
                                    "limit": None,
                                    "allowed_values": None,
                                    "correction_hint": "Use arguments as a JSON object.",
                                }
                            ],
                        }
                    },
                },
            },
        )

        non_empty = self._tools_call("system.help", arguments_marker={"verbose": True}, request_id=102)
        self.assertEqual(
            non_empty.json(),
            {
                "jsonrpc": "2.0",
                "id": 102,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {
                        "error": {
                            "code": "validation",
                            "detail": "Arguments must be an empty object.",
                            "validation_hints": [
                                {
                                    "field": "arguments",
                                    "area": "params.arguments",
                                    "reason": "arguments_must_be_empty_object",
                                    "limit": 0,
                                    "allowed_values": None,
                                    "correction_hint": "Use arguments as {} with no keys.",
                                }
                            ],
                        }
                    },
                },
            },
        )

    def test_target_help_tools_use_exact_invalid_params_mapping(self) -> None:
        """Target-taking help tools must use the exact nested validation envelope."""
        cases = [
            (
                "system.tool_usage",
                None,
                {
                    "code": "validation",
                    "detail": "Arguments object is required.",
                    "validation_hints": [
                        {
                            "field": "arguments",
                            "area": "params.arguments",
                            "reason": "arguments_required",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Provide arguments as an empty JSON object.",
                        }
                    ],
                },
            ),
            (
                "system.topic_help",
                "continuity.read.startup_view",
                {
                    "code": "validation",
                    "detail": "Arguments must be a JSON object.",
                    "validation_hints": [
                        {
                            "field": "arguments",
                            "area": "params.arguments",
                            "reason": "arguments_must_be_object",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Use arguments as a JSON object.",
                        }
                    ],
                },
            ),
            (
                "system.tool_usage",
                {},
                {
                    "code": "validation",
                    "detail": "Missing required field name.",
                    "validation_hints": [
                        {
                            "field": "name",
                            "area": "params.arguments",
                            "reason": "required_field_missing",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Provide the required name field.",
                        }
                    ],
                },
            ),
            (
                "system.topic_help",
                {"id": 7},
                {
                    "code": "validation",
                    "detail": "Field id must be a string.",
                    "validation_hints": [
                        {
                            "field": "id",
                            "area": "params.arguments",
                            "reason": "wrong_type",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Use id as a JSON string.",
                        }
                    ],
                },
            ),
            (
                "system.error_guide",
                {"code": "validation", "verbose": True, "trace": True},
                {
                    "code": "validation",
                    "detail": "Unexpected field verbose.",
                    "validation_hints": [
                        {
                            "field": "verbose",
                            "area": "params.arguments",
                            "reason": "unexpected_field",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Remove the unsupported verbose field.",
                        }
                    ],
                },
            ),
            (
                "system.tool_usage",
                {"name": "memory.write"},
                {
                    "code": "validation",
                    "detail": "Unsupported tool name.",
                    "validation_hints": [
                        {
                            "field": "name",
                            "area": "params.arguments",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "continuity.read",
                                "continuity.upsert",
                                "context.retrieve",
                            ],
                            "correction_hint": "Use one of: continuity.read, continuity.upsert, context.retrieve.",
                        }
                    ],
                },
            ),
            (
                "system.topic_help",
                {"id": "continuity.read.invalid"},
                {
                    "code": "validation",
                    "detail": "Unsupported topic id.",
                    "validation_hints": [
                        {
                            "field": "id",
                            "area": "params.arguments",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "continuity.read.startup_view",
                                "continuity.read.trust_signals",
                                "continuity.upsert.session_end_snapshot",
                            ],
                            "correction_hint": "Use one of: continuity.read.startup_view, continuity.read.trust_signals, continuity.upsert.session_end_snapshot.",
                        }
                    ],
                },
            ),
            (
                "system.error_guide",
                {"code": "not_found"},
                {
                    "code": "validation",
                    "detail": "Unsupported error code.",
                    "validation_hints": [
                        {
                            "field": "code",
                            "area": "params.arguments",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "validation",
                                "tool_not_found",
                                "unknown_help_topic",
                            ],
                            "correction_hint": "Use one of: validation, tool_not_found, unknown_help_topic.",
                        }
                    ],
                },
            ),
        ]

        for index, (name, arguments, expected_error) in enumerate(cases, start=200):
            with self.subTest(name=name, arguments=arguments):
                response = self._tools_call(name, arguments_marker=arguments, request_id=index)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {
                        "jsonrpc": "2.0",
                        "id": index,
                        "error": {
                            "code": -32602,
                            "message": "Invalid params",
                            "data": {"error": expected_error},
                        },
                    },
                )

    def test_help_validation_hints_follow_exact_single_entry_precedence(self) -> None:
        """Overlapping invalid MCP help requests must resolve to the one winning hint."""
        cases = [
            (
                "system.help",
                {"verbose": True, "trace": True},
                {
                    "detail": "Arguments must be an empty object.",
                    "validation_hints": [
                        {
                            "field": "arguments",
                            "area": "params.arguments",
                            "reason": "arguments_must_be_empty_object",
                            "limit": 0,
                            "allowed_values": None,
                            "correction_hint": "Use arguments as {} with no keys.",
                        }
                    ],
                },
            ),
            (
                "system.tool_usage",
                {"verbose": True},
                {
                    "detail": "Missing required field name.",
                    "validation_hints": [
                        {
                            "field": "name",
                            "area": "params.arguments",
                            "reason": "required_field_missing",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Provide the required name field.",
                        }
                    ],
                },
            ),
            (
                "system.tool_usage",
                {"name": 7, "verbose": True},
                {
                    "detail": "Field name must be a string.",
                    "validation_hints": [
                        {
                            "field": "name",
                            "area": "params.arguments",
                            "reason": "wrong_type",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Use name as a JSON string.",
                        }
                    ],
                },
            ),
            (
                "system.tool_usage",
                {"name": "memory.write", "verbose": True},
                {
                    "detail": "Unexpected field verbose.",
                    "validation_hints": [
                        {
                            "field": "verbose",
                            "area": "params.arguments",
                            "reason": "unexpected_field",
                            "limit": None,
                            "allowed_values": None,
                            "correction_hint": "Remove the unsupported verbose field.",
                        }
                    ],
                },
            ),
        ]

        for index, (name, arguments, expected_error) in enumerate(cases, start=300):
            with self.subTest(name=name, arguments=arguments):
                response = self._tools_call(name, arguments_marker=arguments, request_id=index)
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["error"]["code"], -32602)
                self.assertEqual(payload["error"]["message"], "Invalid params")
                self.assertEqual(payload["error"]["data"]["error"], {"code": "validation", **expected_error})
                self.assertEqual(len(payload["error"]["data"]["error"]["validation_hints"]), 1)

    def test_help_validation_hints_use_first_unexpected_key_in_source_order(self) -> None:
        """The first unexpected key must win consistently when several are present."""
        response = self._tools_call(
            "system.topic_help",
            arguments_marker={"id": "continuity.read.invalid", "trace": True, "verbose": True},
            request_id=350,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 350,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {
                        "error": {
                            "code": "validation",
                            "detail": "Unexpected field trace.",
                            "validation_hints": [
                                {
                                    "field": "trace",
                                    "area": "params.arguments",
                                    "reason": "unexpected_field",
                                    "limit": None,
                                    "allowed_values": None,
                                    "correction_hint": "Remove the unsupported trace field.",
                                }
                            ],
                        }
                    },
                },
            },
        )
