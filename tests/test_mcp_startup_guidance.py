"""Regression guards for the bounded MCP slice-2 startup-guidance surface."""

from pathlib import Path
import unittest

from app.main import discovery


class TestMcpStartupGuidance(unittest.TestCase):
    """Keep runtime guidance and repo docs aligned on slice-2 bootstrap."""

    _EXPECTED_BOOTSTRAP_CALLS = [
        "GET /.well-known/mcp.json",
        (
            "POST /v1/mcp "
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            "\"params\":{\"protocolVersion\":\"2025-11-25\"}}"
        ),
    ]

    def test_runtime_guidance_uses_exact_bounded_bootstrap_sequence(self) -> None:
        """The machine-readable discovery payload should keep the canonical startup flow."""
        payload = discovery()
        self.assertEqual(payload["agent_guidance"]["mcp_first_calls"], self._EXPECTED_BOOTSTRAP_CALLS)

    def test_mcp_guide_separates_bootstrap_from_post_bootstrap_usage(self) -> None:
        """The MCP guide must keep startup bounded to the initialize flow."""
        doc = Path("docs/mcp.md").read_text(encoding="utf-8")
        bootstrap_block = (
            "For an MCP-oriented client, the canonical slice-2 bootstrap sequence is exactly:\n\n"
            "1. `GET /.well-known/mcp.json`\n"
            '2. `POST /v1/mcp` with `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`\n'
        )
        self.assertIn(bootstrap_block, doc)
        self.assertIn("After `initialize` succeeds, normal MCP usage may call:", doc)
        self.assertIn('`POST /v1/mcp` with `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`', doc)
        self.assertIn("`POST /v1/mcp` with `tools/call` requests as needed", doc)
        self.assertIn("CogniRelay\naccepts it as a notification-only compatibility call and returns `204`.", doc)
        self.assertNotIn(
            '3. `POST /v1/mcp` with `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`',
            doc,
        )
        self.assertNotIn("4. `POST /v1/mcp` with `tools/call` requests as needed", doc)

    def test_system_overview_uses_exact_bounded_bootstrap_wording(self) -> None:
        """The system overview must describe the initialize-ready bootstrap."""
        doc = Path("docs/system-overview.md").read_text(encoding="utf-8")
        self.assertIn(
            "If the runtime prefers MCP-style JSON-RPC, the canonical slice-2 bootstrap sequence is "
            "`GET /.well-known/mcp.json`, then `POST /v1/mcp` for `initialize` with required "
            "`protocolVersion`. `notifications/initialized` remains accepted as an optional "
            "notification-only compatibility call.",
            doc,
        )
        self.assertIn(
            "After `initialize` succeeds, normal MCP usage may proceed with methods such as "
            "`tools/list` and `tools/call`.",
            doc,
        )
        self.assertNotIn("initialize`, `notifications/initialized`, and `tools/list`", doc)


if __name__ == "__main__":
    unittest.main()
