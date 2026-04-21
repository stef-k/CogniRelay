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
EXPECTED_ROWS = [
    ("transport.jsonrpc_envelope", "slice_1", "partially_converged", "intentionally_deferred", "slice_2"),
    ("transport.post_v1_mcp_endpoint", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("transport.get_v1_mcp_behavior", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("transport.origin_validation", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("transport.localhost_posture", "audit_only", "converged", "none", "implemented"),
    ("transport.auth_posture", "audit_only", "converged", "none", "implemented"),
    ("transport.well_known_metadata_accuracy", "audit_only", "partially_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.initialize_request_acceptance", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.initialize_response_shape", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.protocol_version_negotiation", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.server_capability_schema", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.pre_initialize_ping", "slice_2", "converged", "none", "implemented"),
    ("bootstrap.pre_initialize_tools_list", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.pre_initialize_tools_call", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.pre_initialize_other_methods", "slice_2", "converged", "none", "implemented"),
    ("bootstrap.post_initialize_pre_initialized_ping", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.post_initialize_pre_initialized_tools_list", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.post_initialize_pre_initialized_tools_call", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("bootstrap.post_initialize_pre_initialized_other_methods", "slice_2", "converged", "none", "implemented"),
    ("bootstrap.notifications_initialized_acceptance", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("tools.list.response_shape", "slice_2", "converged", "none", "implemented"),
    ("tools.list.metadata_minimum", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("tools.list.pagination", "slice_2", "not_yet_converged", "intentionally_deferred", "slice_2"),
    ("tools.call.request_shape", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("tools.call.success_shape", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("tools.call.error_mapping", "slice_2", "partially_converged", "intentionally_deferred", "slice_2"),
    ("help.mcp_error_actionability", "slice_3", "not_yet_converged", "intentionally_deferred", "slice_3"),
    ("help.mcp_reference_parity", "slice_3", "not_yet_converged", "intentionally_deferred", "slice_3"),
    ("features.resources", "audit_only", "intentionally_deviated", "intentionally_not_supported", "implemented"),
    ("features.prompts", "audit_only", "intentionally_deviated", "intentionally_not_supported", "implemented"),
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

    def test_matrix_row_inventory_and_statuses_are_exact(self) -> None:
        """The matrix must contain all 30 rows in the exact issue-defined order."""
        _, rows, _ = _parse_table_rows()
        self.assertEqual(len(rows), len(EXPECTED_ROWS))
        for row, expected in zip(rows, EXPECTED_ROWS, strict=True):
            row_id, slice_owner, status, temporary_posture, follow_up = expected
            self.assertEqual(row["Row ID"], row_id)
            self.assertEqual(row["Slice Owner"], slice_owner)
            self.assertEqual(row["Status"], status)
            self.assertEqual(row["Temporary Posture"], temporary_posture)
            self.assertEqual(row["Owner / Follow-up"], follow_up)
            self.assertEqual(row["Tests Required"], EXPECTED_TEST_FILE)
            self.assertEqual(row["Docs Required"], EXPECTED_DOC_FILE)
            self.assertTrue(row["Current Behavior"])
            self.assertTrue(row["Required Behavior"])

    def test_doc_avoids_withdrawn_status_words(self) -> None:
        """The matrix must not use withdrawn status vocabulary."""
        _, rows, _ = _parse_table_rows()
        for status in WITHDRAWN_STATUSES:
            self.assertNotIn(status, {row["Status"] for row in rows})

    def test_fixed_resource_and_prompt_reasons_are_present(self) -> None:
        """The fixed intentionally-deviated rows must carry the required reason text."""
        _, rows, _ = _parse_table_rows()
        by_id = {row["Row ID"]: row for row in rows}
        for row_id in ("features.resources", "features.prompts"):
            required_behavior = by_id[row_id]["Required Behavior"]
            self.assertIn("tools-first", required_behavior)
            self.assertIn("later issue", required_behavior)


if __name__ == "__main__":
    unittest.main()
