"""MCP help/reference parity tests for issue #216 slice 3."""

from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.mcp.service import reset_bootstrap_state


class TestMcp216Slice3Help(unittest.TestCase):
    """Validate the exact slice-3 MCP help/reference contract."""

    _HEADERS = {"authorization": "Bearer help-slice3"}

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

    def _request(self, request_id: int, method: str, *, params: object = None, include_params: bool = True):
        """Send one MCP request with optional params omission."""
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if include_params:
            payload["params"] = params
        return self.client.post("/v1/mcp", json=payload, headers=self._HEADERS)

    def test_well_known_descriptor_lists_slice3_help_methods(self) -> None:
        """The supplemental MCP descriptor should expose the recognized help methods."""
        response = self.client.get("/.well-known/mcp.json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["methods"],
            [
                "initialize",
                "notifications/initialized",
                "ping",
                "tools/list",
                "tools/call",
                "system.help",
                "system.tool_usage",
                "system.topic_help",
                "system.hook_guide",
                "system.error_guide",
                "system.onboarding_index",
                "system.onboarding_bootstrap",
                "system.onboarding_section",
                "system.validation_limits",
                "system.validation_limit",
            ],
        )

    def test_help_methods_are_pre_initialize_gated_but_ready_after_initialize(self) -> None:
        """Recognized slice-3 help methods must gate only until initialize succeeds."""
        methods = [
            ("system.help", {}),
            ("system.tool_usage", {"name": "continuity.read"}),
            ("system.topic_help", {"id": "continuity.read.startup_view"}),
            ("system.hook_guide", {}),
            ("system.error_guide", {"code": -32602}),
        ]

        for request_id, (method, params) in enumerate(methods, start=10):
            with self.subTest(phase="pre_initialize", method=method):
                response = self._request(request_id, method, params=params)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
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
                "id": 20,
                "method": "initialize",
                "params": {"protocolVersion": "2025-11-25"},
            },
            headers=self._HEADERS,
        )
        self.assertEqual(initialize.status_code, 200)

        for request_id, (method, params) in enumerate(methods, start=30):
            with self.subTest(phase="post_initialize", method=method):
                response = self._request(request_id, method, params=params)
                self.assertEqual(response.status_code, 200)
                self.assertIn("result", response.json())

    def test_tools_list_excludes_help_request_methods(self) -> None:
        """The five slice-3 method names must not appear as tools."""
        self._bootstrap()
        response = self._request(40, "tools/list", params={})
        self.assertEqual(response.status_code, 200)
        tool_names = {tool["name"] for tool in response.json()["result"]["tools"]}
        self.assertTrue({"system.discovery", "system.contracts"}.issubset(tool_names))
        self.assertFalse(
            {
                "system.help",
                "system.tool_usage",
                "system.topic_help",
                "system.hook_guide",
                "system.error_guide",
                "system.onboarding_index",
                "system.onboarding_bootstrap",
                "system.onboarding_section",
                "system.validation_limits",
                "system.validation_limit",
            }
            & tool_names
        )

    def test_help_methods_return_one_text_item_and_exact_structured_shapes(self) -> None:
        """Each slice-3 help method must use the exact success placement and shape."""
        self._bootstrap()
        cases = [
            (
                "system.help",
                {},
                {
                    "surface": "help_index",
                    "httpEquivalent": "/v1/help",
                    "title": str,
                    "summary": str,
                },
            ),
            (
                "system.tool_usage",
                {"name": "continuity.read"},
                {
                    "surface": "tool_help",
                    "httpEquivalent": "/v1/help/tools/continuity.read",
                    "name": "continuity.read",
                    "summary": str,
                },
            ),
            (
                "system.topic_help",
                {"id": "continuity.read.startup_view"},
                {
                    "surface": "topic_help",
                    "httpEquivalent": "/v1/help/topics/continuity.read.startup_view",
                    "id": "continuity.read.startup_view",
                    "title": str,
                    "summary": str,
                },
            ),
            (
                "system.hook_guide",
                {},
                {
                    "surface": "hook_guide",
                    "httpEquivalent": "/v1/help/hooks",
                    "title": str,
                    "summary": str,
                },
            ),
            (
                "system.error_guide",
                {"code": -32602},
                {
                    "surface": "error_guide",
                    "httpEquivalent": "/v1/help/errors/-32602",
                    "code": -32602,
                    "title": str,
                    "summary": str,
                },
            ),
        ]

        for request_id, (method, params, structured_shape) in enumerate(cases, start=50):
            with self.subTest(method=method, params=params):
                response = self._request(request_id, method, params=params)
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["jsonrpc"], "2.0")
                self.assertEqual(payload["id"], request_id)
                self.assertEqual(sorted(payload["result"].keys()), ["content", "structuredContent"])
                self.assertEqual(
                    payload["result"]["content"],
                    [{"type": "text", "text": payload["result"]["content"][0]["text"]}],
                )
                self.assertTrue(payload["result"]["content"][0]["text"])
                self.assertEqual(set(payload["result"]["structuredContent"].keys()), set(structured_shape))
                for key, expected in structured_shape.items():
                    value = payload["result"]["structuredContent"][key]
                    if expected is str:
                        self.assertIsInstance(value, str)
                        self.assertTrue(value)
                    else:
                        self.assertEqual(value, expected)

    def test_tool_usage_summaries_include_issue_264_guidance_without_shape_expansion(self) -> None:
        """Tool usage keeps the compact shape while carrying selector examples in summary text."""
        self._bootstrap()
        cases = {
            "context.retrieve": ("subject_kind", "thread", "subject_id", "release-v1.4-followup"),
            "schedule.list": (
                "due=true&thread_id=release-v1.4-followup",
                "due=true&subject_kind=thread&subject_id=release-v1.4-followup",
            ),
        }
        for request_id, (tool_name, tokens) in enumerate(cases.items(), start=240):
            with self.subTest(tool_name=tool_name):
                response = self._request(request_id, "system.tool_usage", params={"name": tool_name})
                self.assertEqual(response.status_code, 200)
                structured = response.json()["result"]["structuredContent"]
                self.assertEqual(
                    set(structured),
                    {"surface", "httpEquivalent", "name", "summary"},
                )
                summary = structured["summary"]
                for token in tokens:
                    self.assertIn(token, summary)

    def test_error_guide_mentions_initialize_client_info_metadata(self) -> None:
        """Runtime MCP error help should describe the initialize metadata boundary."""
        self._bootstrap()
        response = self._request(250, "system.error_guide", params={"code": -32602})
        self.assertEqual(response.status_code, 200)
        structured = response.json()["result"]["structuredContent"]
        self.assertEqual(set(structured), {"surface", "httpEquivalent", "code", "title", "summary"})
        summary = structured["summary"]
        for token in ("initialize", "clientInfo", "protocolVersion", "Implementation metadata"):
            self.assertIn(token, summary)

    def test_system_help_and_system_hook_guide_enforce_empty_object_params(self) -> None:
        """Zero-argument slice-3 methods accept only omitted params or {} and reject extra keys."""
        self._bootstrap()

        omitted = self._request(70, "system.help", include_params=False)
        self.assertEqual(omitted.status_code, 200)
        self.assertEqual(omitted.json()["result"]["structuredContent"]["surface"], "help_index")

        empty_object = self._request(71, "system.hook_guide", params={})
        self.assertEqual(empty_object.status_code, 200)
        self.assertEqual(empty_object.json()["result"]["structuredContent"]["surface"], "hook_guide")

        request_meta = self._request(74, "system.help", params={"_meta": {"request_id": "help-1"}})
        self.assertEqual(request_meta.status_code, 200)
        self.assertEqual(request_meta.json()["result"]["structuredContent"]["surface"], "help_index")

        non_object = self._request(72, "system.help", params=[])
        self.assertEqual(
            non_object.json(),
            {
                "jsonrpc": "2.0",
                "id": 72,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "params must be an object"},
                },
            },
        )

        extra_key = self._request(73, "system.hook_guide", params={"verbose": True})
        self.assertEqual(
            extra_key.json(),
            {
                "jsonrpc": "2.0",
                "id": 73,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "unexpected system.hook_guide param", "field": "verbose"},
                },
            },
        )

        invalid_meta = self._request(75, "system.help", params={"_meta": []})
        self.assertEqual(
            invalid_meta.json(),
            {
                "jsonrpc": "2.0",
                "id": 75,
                "error": {
                    "code": -32602,
                    "message": "Invalid params",
                    "data": {"reason": "_meta must be an object"},
                },
            },
        )

    def test_targeted_help_methods_accept_request_meta(self) -> None:
        """Targeted MCP help methods should ignore standard request _meta metadata."""
        self._bootstrap()

        tool_usage = self._request(
            76,
            "system.tool_usage",
            params={"name": "continuity.read", "_meta": {"request_id": "usage-1"}},
        )
        self.assertEqual(tool_usage.status_code, 200)
        self.assertEqual(tool_usage.json()["result"]["structuredContent"]["name"], "continuity.read")

        error_guide = self._request(
            77,
            "system.error_guide",
            params={"code": -32602, "_meta": {"request_id": "error-guide-1"}},
        )
        self.assertEqual(error_guide.status_code, 200)
        self.assertEqual(error_guide.json()["result"]["structuredContent"]["code"], -32602)

    def test_targeted_help_methods_use_exact_invalid_params_mappings(self) -> None:
        """Targeted slice-3 methods must use exact invalid-target mappings."""
        self._bootstrap()
        cases = [
            (
                "system.tool_usage",
                False,
                None,
                {"reason": "params must be an object"},
            ),
            (
                "system.tool_usage",
                True,
                [],
                {"reason": "params must be an object"},
            ),
            (
                "system.tool_usage",
                True,
                {},
                {"reason": "name is required"},
            ),
            (
                "system.tool_usage",
                True,
                {"name": 9},
                {"reason": "name must be a non-empty string"},
            ),
            (
                "system.tool_usage",
                True,
                {"name": ""},
                {"reason": "name is required"},
            ),
            (
                "system.tool_usage",
                True,
                {"name": " \t"},
                {"reason": "name is required"},
            ),
            (
                "system.tool_usage",
                True,
                {"name": "memory.write"},
                {"reason": "unknown tool", "name": "memory.write"},
            ),
            (
                "system.tool_usage",
                True,
                {"name": "continuity.read", "extra": True},
                {"reason": "unexpected system.tool_usage param", "field": "extra"},
            ),
            (
                "system.topic_help",
                True,
                {},
                {"reason": "id is required"},
            ),
            (
                "system.topic_help",
                True,
                {"id": 9},
                {"reason": "id must be a non-empty string"},
            ),
            (
                "system.topic_help",
                True,
                {"id": ""},
                {"reason": "id is required"},
            ),
            (
                "system.topic_help",
                True,
                {"id": "\n"},
                {"reason": "id is required"},
            ),
            (
                "system.topic_help",
                True,
                {"id": "continuity.read.startup"},
                {"reason": "unknown topic", "id": "continuity.read.startup"},
            ),
            (
                "system.topic_help",
                True,
                {"id": "continuity.read.startup_view", "extra": True},
                {"reason": "unexpected system.topic_help param", "field": "extra"},
            ),
            (
                "system.error_guide",
                False,
                None,
                {"reason": "params must be an object"},
            ),
            (
                "system.error_guide",
                True,
                {},
                {"reason": "code is required"},
            ),
            (
                "system.error_guide",
                True,
                {"code": "-32602"},
                {"reason": "code must be an integer"},
            ),
            (
                "system.error_guide",
                True,
                {"code": -31999},
                {"reason": "unknown error code", "code": -31999},
            ),
            (
                "system.error_guide",
                True,
                {"code": -32602, "extra": True},
                {"reason": "unexpected system.error_guide param", "field": "extra"},
            ),
        ]

        for request_id, (method, include_params, params, error_data) in enumerate(cases, start=80):
            with self.subTest(method=method, params=params):
                response = self._request(request_id, method, params=params, include_params=include_params)
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
