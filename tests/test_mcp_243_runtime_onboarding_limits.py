"""MCP request-method tests for issue #243 onboarding and validation limits."""

from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.mcp.service import reset_bootstrap_state


NEW_METHODS = [
    "system.onboarding_index",
    "system.onboarding_bootstrap",
    "system.onboarding_section",
    "system.validation_limits",
    "system.validation_limit",
]


class TestMcp243RuntimeOnboardingLimits(unittest.TestCase):
    """Validate new #243 MCP methods as request methods, not tools."""

    _HEADERS = {"authorization": "Bearer issue-243"}

    @classmethod
    def setUpClass(cls) -> None:
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._client_context.__exit__(None, None, None)

    def setUp(self) -> None:
        reset_bootstrap_state()

    def _bootstrap(self) -> None:
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}},
            headers=self._HEADERS,
        )
        self.assertEqual(response.status_code, 200)
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            headers=self._HEADERS,
        )
        self.assertEqual(response.status_code, 204)

    def _request(self, request_id: int, method: str, *, params: object = None, include_params: bool = True):
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if include_params:
            payload["params"] = params
        return self.client.post("/v1/mcp", json=payload, headers=self._HEADERS)

    def test_new_methods_are_descriptor_advertised_and_bootstrap_gated(self) -> None:
        descriptor = self.client.get("/.well-known/mcp.json")
        self.assertEqual(descriptor.status_code, 200)
        for method in NEW_METHODS:
            self.assertIn(method, descriptor.json()["methods"])
            response = self._request(10, method, params={"id": "bootstrap"} if method == "system.onboarding_section" else {"field_path": "continuity.top_priorities"} if method == "system.validation_limit" else {})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["error"]["data"], {"required_step": "initialize"})

    def test_new_methods_do_not_appear_in_tools_list(self) -> None:
        self._bootstrap()
        response = self._request(20, "tools/list", params={})
        self.assertEqual(response.status_code, 200)
        tool_names = {tool["name"] for tool in response.json()["result"]["tools"]}
        self.assertFalse(set(NEW_METHODS) & tool_names)

    def test_success_shapes_and_http_equivalents(self) -> None:
        self._bootstrap()
        cases = [
            ("system.onboarding_index", {}, "Browse the bounded CogniRelay onboarding index.", "/v1/help/onboarding", "onboarding_index"),
            ("system.onboarding_bootstrap", {}, "Read the compact CogniRelay onboarding bootstrap.", "/v1/help/onboarding/bootstrap", "onboarding_bootstrap"),
            ("system.onboarding_section", {"id": "bootstrap"}, "Read CogniRelay onboarding section: Minimum Startup Path.", "/v1/help/onboarding/sections/bootstrap", "onboarding_section"),
            ("system.validation_limits", {}, "Browse bounded validation limits for agent-authored fields.", "/v1/help/limits", "validation_limits_index"),
            ("system.validation_limit", {"field_path": "continuity.top_priorities"}, "Read validation limits for continuity.top_priorities.", "/v1/help/limits/continuity.top_priorities", "validation_limit"),
        ]
        for request_id, (method, params, summary, http_equivalent, kind) in enumerate(cases, start=30):
            with self.subTest(method=method):
                response = self._request(request_id, method, params=params)
                self.assertEqual(response.status_code, 200)
                result = response.json()["result"]
                structured = result["structuredContent"]
                self.assertEqual(result["content"], [{"type": "text", "text": summary}])
                self.assertEqual(structured["summary"], summary)
                self.assertEqual(structured["httpEquivalent"], http_equivalent)
                self.assertEqual(structured["kind"], kind)
                self.assertEqual(list(structured)[:3], ["summary", "httpEquivalent", "kind"])

    def test_zero_param_methods_accept_omitted_and_empty_only(self) -> None:
        self._bootstrap()
        for request_id, method in enumerate(["system.onboarding_index", "system.onboarding_bootstrap", "system.validation_limits"], start=50):
            with self.subTest(method=method):
                self.assertIn("result", self._request(request_id, method, include_params=False).json())
                self.assertIn("result", self._request(request_id + 20, method, params={}).json())
                self.assertEqual(
                    self._request(request_id + 40, method, params=[]).json()["error"]["data"],
                    {"reason": "params must be an object"},
                )
                self.assertEqual(
                    self._request(request_id + 60, method, params={"extra": True}).json()["error"]["data"],
                    {"reason": f"unexpected {method} param", "field": "extra"},
                )

    def test_targeted_method_param_validation_and_unknown_lookup(self) -> None:
        self._bootstrap()
        cases = [
            ("system.onboarding_section", False, None, {"reason": "params must be an object"}),
            ("system.onboarding_section", True, [], {"reason": "params must be an object"}),
            ("system.onboarding_section", True, {}, {"reason": "id is required"}),
            ("system.onboarding_section", True, {"id": 9}, {"reason": "id must be a non-empty string"}),
            ("system.onboarding_section", True, {"id": ""}, {"reason": "id is required"}),
            ("system.onboarding_section", True, {"id": " \t"}, {"reason": "id is required"}),
            ("system.onboarding_section", True, {"id": "bad-id"}, {"reason": "unknown onboarding section", "id": "bad-id"}),
            ("system.onboarding_section", True, {"id": "bootstrap", "extra": True}, {"reason": "unexpected system.onboarding_section param", "field": "extra"}),
            ("system.validation_limit", False, None, {"reason": "params must be an object"}),
            ("system.validation_limit", True, {}, {"reason": "field_path is required"}),
            ("system.validation_limit", True, {"field_path": 9}, {"reason": "field_path must be a non-empty string"}),
            ("system.validation_limit", True, {"field_path": ""}, {"reason": "field_path is required"}),
            ("system.validation_limit", True, {"field_path": "\n"}, {"reason": "field_path is required"}),
            ("system.validation_limit", True, {"field_path": "bad.path"}, {"reason": "unknown validation limit", "field_path": "bad.path"}),
            ("system.validation_limit", True, {"field_path": "continuity.top_priorities", "extra": True}, {"reason": "unexpected system.validation_limit param", "field": "extra"}),
        ]
        for request_id, (method, include_params, params, data) in enumerate(cases, start=100):
            with self.subTest(method=method, params=params):
                response = self._request(request_id, method, params=params, include_params=include_params)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["error"], {"code": -32602, "message": "Invalid params", "data": data})


if __name__ == "__main__":
    unittest.main()
