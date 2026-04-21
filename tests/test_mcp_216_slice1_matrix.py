"""Slice-1 audit matrix contract tests for issue #216."""

from __future__ import annotations

import unittest
from pathlib import Path


DOC_PATH = Path(__file__).resolve().parents[1] / "docs" / "mcp-216-convergence-audit.md"
EXPECTED_COLUMNS = [
    "Row ID",
    "Slice Owner",
    "Status",
    "Current Behavior",
    "Required Behavior",
    "Temporary Posture",
    "Owner / Follow-up",
    "Tests Required",
    "Docs Required",
]
EXPECTED_TEST_FILE = "tests/test_mcp_216_slice1_matrix.py"
EXPECTED_DOC_FILE = "docs/mcp-216-convergence-audit.md"
WITHDRAWN_STATUSES = {
    "supported_as_expected",
    "supported_with_intentional_deviation",
    "missing",
    "underspecified",
    "TBD",
}


def _row(
    row_id: str,
    slice_owner: str,
    status: str,
    current_behavior: str,
    required_behavior: str,
    temporary_posture: str,
    follow_up: str,
) -> dict[str, str]:
    """Build one exact expected matrix row."""
    return {
        "Row ID": row_id,
        "Slice Owner": slice_owner,
        "Status": status,
        "Current Behavior": current_behavior,
        "Required Behavior": required_behavior,
        "Temporary Posture": temporary_posture,
        "Owner / Follow-up": follow_up,
        "Tests Required": EXPECTED_TEST_FILE,
        "Docs Required": EXPECTED_DOC_FILE,
    }


EXPECTED_MATRIX = [
    _row(
        "transport.jsonrpc_envelope",
        "slice_1",
        "partially_converged",
        (
            "Single-request JSON-RPC exists, but FastAPI returns 422 on parse failure, "
            "batches are accepted, request-id validation is loose, and error payload "
            "shapes and messages diverge. The hardened `#216` body also leaves an "
            "internal tension because this is the only `slice_1` row while slice 1 "
            "remains audit-only."
        ),
        (
            "Match the hardened `#216` body exactly: this row remains `slice_1` even "
            "though slice 1 is audit-only, and exact envelope closure is still a "
            "`#216` completion requirement. The audit records that tension explicitly "
            "instead of rewriting follow-up semantics; the exact envelope rules remain "
            "one JSON object only, no batches, exact 400 or 200 or 204 mapping, exact "
            "id validation, exact error data, and method-not-found precedence."
        ),
        "intentionally_deferred",
        "implemented",
    ),
    _row(
        "transport.post_v1_mcp_endpoint",
        "slice_2",
        "partially_converged",
        "`POST /v1/mcp` exists and can succeed for `initialize`, `ping`, `tools/list`, and `tools/call`, but request handling still follows the legacy bridge contract.",
        "`POST /v1/mcp` is the only MCP request endpoint that may succeed, and it must apply the exact `#216` envelope, bootstrap, auth, and error rules.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "transport.get_v1_mcp_behavior",
        "slice_2",
        "not_yet_converged",
        (
            "`GET /v1/mcp` already returns 405 with `Allow: POST` and no success "
            "payload; the hardened `#216` body fixes that as the slice-2 posture and "
            "requires any future GET success behavior to move to a "
            "`later_issue:<number>` follow-up instead."
        ),
        (
            "Keep `GET /v1/mcp` at 405 with `Allow: POST`; no SSE and no alternate GET "
            "success behavior under `#216`. If GET support is ever added later, this "
            "row must point to `later_issue:<number>` instead of `slice_2`."
        ),
        "intentionally_deferred",
        "implemented",
    ),
    _row(
        "transport.origin_validation",
        "slice_2",
        "not_yet_converged",
        "`POST /v1/mcp` does not validate `Origin`; present and missing origins are treated the same today.",
        "When `Origin` is present on `POST /v1/mcp`, allow only loopback origins and reject every other present origin with the exact 403 JSON-RPC body from `#216`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "transport.localhost_posture",
        "audit_only",
        "converged",
        "No transport-level localhost success path exists; loopback matters today only through per-tool local-only checks after auth resolution.",
        "Record localhost posture without weakening `transport.origin_validation`; loopback is not a separate MCP transport mode.",
        "none",
        "implemented",
    ),
    _row(
        "transport.auth_posture",
        "audit_only",
        "converged",
        "MCP calls use bearer auth and the same HTTP scope, namespace, and local-only enforcement as the wrapped routes.",
        "Record that the HTTP MCP transport reuses the normal bearer auth posture and does not create a separate permission system.",
        "none",
        "implemented",
    ),
    _row(
        "transport.well_known_metadata_accuracy",
        "audit_only",
        "partially_converged",
        "`/.well-known/mcp.json` points at `/v1/mcp` and bearer auth, but it still advertises legacy `mcp-compatible` wording and omits the `2025-11-25` target plus deferred GET posture.",
        "`/.well-known/mcp.json` must remain supplemental metadata only and accurately describe the narrowed `#216` posture instead of implying a broader MCP bridge.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.initialize_request_acceptance",
        "slice_2",
        "partially_converged",
        "`initialize` accepts missing or arbitrary params, treats missing request ids as acceptable, and does not reject extra fields or malformed `clientInfo`.",
        "Accept only the `#216` `initialize` request shape, require a params object with the allowed keys only, and use the exact invalid-params mappings.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.initialize_response_shape",
        "slice_2",
        "partially_converged",
        "`initialize` returns `protocolVersion`, `capabilities`, `serverInfo`, and an extra `instructions` field.",
        "Return only `result.protocolVersion`, `result.capabilities`, and `result.serverInfo` with no extra success keys.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.protocol_version_negotiation",
        "slice_2",
        "not_yet_converged",
        "`initialize` echoes any requested protocol version and falls back to the server contract version.",
        "Support only `protocolVersion = \"2025-11-25\"` and reject every other value with the exact unsupported-version error.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.server_capability_schema",
        "slice_2",
        "partially_converged",
        "The initialize result already includes `tools.listChanged = false`, but it also exposes forbidden `sampling`.",
        "Advertise exactly `{\"tools\":{\"listChanged\":false}}` and no other top-level capability keys.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.pre_initialize_ping",
        "slice_2",
        "converged",
        "Before any `initialize`, `ping` already returns the normal success result.",
        "Before successful `initialize`, `ping` must return the normal success response.",
        "none",
        "implemented",
    ),
    _row(
        "bootstrap.pre_initialize_tools_list",
        "slice_2",
        "not_yet_converged",
        "Before `initialize`, `tools/list` succeeds immediately instead of returning a bootstrap gate error.",
        "Before successful `initialize`, `tools/list` must return `-32000` `Server not initialized` with `{\"required_step\":\"initialize\"}`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.pre_initialize_tools_call",
        "slice_2",
        "not_yet_converged",
        "Before `initialize`, `tools/call` proceeds to normal validation and execution instead of returning a bootstrap gate error.",
        "Before successful `initialize`, `tools/call` must return `-32000` `Server not initialized` with `{\"required_step\":\"initialize\"}`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.pre_initialize_other_methods",
        "slice_2",
        "converged",
        "Unknown methods already return method-not-found instead of being bootstrap gated.",
        "Before successful `initialize`, unknown methods must still use method-not-found, not bootstrap gating.",
        "none",
        "implemented",
    ),
    _row(
        "bootstrap.post_initialize_pre_initialized_ping",
        "slice_2",
        "not_yet_converged",
        "No bootstrap state is tracked after `initialize`, so there is no distinct post-initialize and pre-notification phase to preserve.",
        "After successful `initialize` and before `notifications/initialized`, `ping` must succeed while that intermediate bootstrap phase remains active.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.post_initialize_pre_initialized_tools_list",
        "slice_2",
        "not_yet_converged",
        "No intermediate bootstrap phase exists, so `tools/list` never returns the post-initialize gate error.",
        "After successful `initialize` and before `notifications/initialized`, `tools/list` must return `-32000` with `{\"required_step\":\"notifications/initialized\"}`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.post_initialize_pre_initialized_tools_call",
        "slice_2",
        "not_yet_converged",
        "No intermediate bootstrap phase exists, so `tools/call` never returns the post-initialize gate error.",
        "After successful `initialize` and before `notifications/initialized`, `tools/call` must return `-32000` with `{\"required_step\":\"notifications/initialized\"}`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "bootstrap.post_initialize_pre_initialized_other_methods",
        "slice_2",
        "converged",
        "Unknown methods already return method-not-found even though bootstrap state is not persisted between calls.",
        "After successful `initialize` and before `notifications/initialized`, unknown methods must still use method-not-found.",
        "none",
        "implemented",
    ),
    _row(
        "bootstrap.notifications_initialized_acceptance",
        "slice_2",
        "partially_converged",
        "Proper `notifications/initialized` notifications return 204, but requests with `id` are accepted and no bootstrap completion state is recorded.",
        "Accept only notification-form `notifications/initialized`, return 204, and mark bootstrap complete.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "tools.list.response_shape",
        "slice_2",
        "converged",
        "`tools/list` already returns `{\"tools\":[...]}` with no top-level `nextCursor` or other result keys.",
        "Keep the base `tools/list` result shape exactly `{\"tools\":[...]}` and forbid extra top-level result keys.",
        "none",
        "implemented",
    ),
    _row(
        "tools.list.metadata_minimum",
        "slice_2",
        "partially_converged",
        "Tool objects already carry `name`, `description`, `inputSchema`, and `metadata`, but slice-2 acceptance has not been tightened against every callable argument contract.",
        "Every returned tool must have a unique callable name, a non-placeholder description, and an `inputSchema` that represents all accepted arguments and required fields.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "tools.list.pagination",
        "slice_2",
        "not_yet_converged",
        "`tools/list` ignores pagination today; non-empty cursors are not rejected with the exact slice-2 error.",
        "Treat omitted, `null`, and empty-string cursors as the first page only, and reject non-empty cursor strings with the exact slice-2 invalid-params error.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "tools.call.request_shape",
        "slice_2",
        "partially_converged",
        (
            "`tools/call` requires a string `name` in practice, but it does not enforce "
            "the exact params key allowlist, missing-params mapping, whitespace-only "
            "rejection, or `arguments` object validation."
        ),
        "Apply the exact `#216` `tools/call` request contract, including params requirement, key allowlist, byte-for-byte name matching, whitespace-only rejection, and `arguments` object validation.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "tools.call.success_shape",
        "slice_2",
        "partially_converged",
        "Successful `tools/call` responses include `content` and `structuredContent`, but they still carry the forbidden `toolName` wrapper field.",
        "Return only `content` and `structuredContent` at top-level JSON-RPC `result`, with no wrapper fields such as `toolName`, `ok`, `status`, or `isError`.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "tools.call.error_mapping",
        "slice_2",
        "partially_converged",
        "Error codes roughly map auth, not-found, and runtime classes, but unknown-tool, schema-validation, and `error.data` shapes do not match the exact `#216` contract.",
        "Use the exact `#216` `tools/call` failure mapping, including unknown-tool invalid-params, schema-validation `details`, exact auth and forbidden shapes, and the narrow `-32004` rule.",
        "intentionally_deferred",
        "slice_2",
    ),
    _row(
        "help.mcp_error_actionability",
        "slice_3",
        "not_yet_converged",
        "MCP errors expose terse legacy messages only; the slice-3 actionability layer does not exist.",
        "Limit future help-text improvements to the slice-3 wording and allowed data additions without changing slice-2 codes or method precedence.",
        "intentionally_deferred",
        "slice_3",
    ),
    _row(
        "help.mcp_reference_parity",
        "slice_3",
        "not_yet_converged",
        "The five MCP help and reference methods are absent; help remains available only through the HTTP surfaces.",
        "Add only the five slice-3 MCP help and reference methods with the exact params contracts, success shapes, and invalid-target mappings from `#216`.",
        "intentionally_deferred",
        "slice_3",
    ),
    _row(
        "features.resources",
        "audit_only",
        "intentionally_deviated",
        "No MCP resources are exposed today.",
        (
            "CogniRelay remains tools-first for this issue; `#216` does not choose a "
            "new help/reference carrier beyond the exact slice-3 parity surfaces "
            "below; any future resources work must be introduced in a later issue, "
            "not implied here."
        ),
        "intentionally_not_supported",
        "implemented",
    ),
    _row(
        "features.prompts",
        "audit_only",
        "intentionally_deviated",
        "No MCP prompts are exposed today.",
        (
            "CogniRelay remains tools-first for this issue; `#216` does not choose a "
            "new help/reference carrier beyond the exact slice-3 parity surfaces "
            "below; any future prompts work must be introduced in a later issue, not "
            "implied here."
        ),
        "intentionally_not_supported",
        "implemented",
    ),
]


def _parse_table_rows() -> tuple[list[str], list[dict[str, str]], str]:
    """Parse the canonical matrix table from the audit document."""
    text = DOC_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()
    header_index = next(i for i, line in enumerate(lines) if line.strip().startswith("| Row ID |"))
    table_lines: list[str] = []
    for line in lines[header_index:]:
        if not line.strip().startswith("|"):
            break
        table_lines.append(line.rstrip())

    rows = []
    for idx, line in enumerate(table_lines):
        cells = [part.strip() for part in line.strip().split("|")[1:-1]]
        if idx == 0:
            header = cells
            continue
        if idx == 1:
            continue
        rows.append(dict(zip(header, cells, strict=True)))
    return header, rows, text


class TestMcp216Slice1Matrix(unittest.TestCase):
    """Validate the deterministic audit matrix introduced by slice 1."""

    def test_doc_exists(self) -> None:
        """The slice-1 audit document must exist in docs/."""
        self.assertTrue(DOC_PATH.exists(), f"missing {DOC_PATH}")

    def test_intro_records_convergence_target(self) -> None:
        """The doc should record the fixed convergence target and temporary transport posture."""
        _, _, text = _parse_table_rows()
        self.assertIn("MCP `2025-11-25` Streamable HTTP", text)
        self.assertIn("`POST /v1/mcp` only", text)
        self.assertIn("`GET /v1/mcp = 405 + Allow: POST`", text)

    def test_table_header_is_exact(self) -> None:
        """The canonical matrix must use the exact required columns in order."""
        header, _, _ = _parse_table_rows()
        self.assertEqual(header, EXPECTED_COLUMNS)

    def test_matrix_content_is_exact(self) -> None:
        """The canonical matrix must match the hardened issue-body text exactly."""
        _, rows, _ = _parse_table_rows()
        self.assertEqual(rows, EXPECTED_MATRIX)

    def test_doc_avoids_withdrawn_status_words(self) -> None:
        """The matrix must not use withdrawn status vocabulary."""
        _, rows, _ = _parse_table_rows()
        for status in WITHDRAWN_STATUSES:
            self.assertNotIn(status, {row["Status"] for row in rows})

    def test_fixed_rows_record_issue_body_tension_and_later_issue_posture(self) -> None:
        """The hardened issue-body contradictions must stay explicit in the audit text."""
        _, rows, _ = _parse_table_rows()
        by_id = {row["Row ID"]: row for row in rows}
        self.assertIn("only `slice_1` row while slice 1 remains audit-only", by_id["transport.jsonrpc_envelope"]["Current Behavior"])
        self.assertIn("instead of `slice_2`", by_id["transport.get_v1_mcp_behavior"]["Required Behavior"])
        for row_id in ("features.resources", "features.prompts"):
            required_behavior = by_id[row_id]["Required Behavior"]
            self.assertIn("tools-first", required_behavior)
            self.assertIn("does not choose a new help/reference carrier", required_behavior)
            self.assertIn("later issue, not implied here", required_behavior)


if __name__ == "__main__":
    unittest.main()
