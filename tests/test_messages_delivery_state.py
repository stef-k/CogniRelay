"""Tests for load_delivery_state error handling and warning propagation."""

import json
import logging
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from app.messages.service import (
    DELIVERY_STATE_REL,
    _write_delivery_state,
    load_delivery_state,
    messages_inbox_service,
    messages_pending_service,
    messages_thread_service,
)
from app.storage import safe_path


class _AuthStub:
    """Auth stub that permits all scopes and paths."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


def _noop_audit(_auth: Any, _event: str, _detail: dict[str, Any]) -> None:
    pass


def _parse_iso(_v: str | None) -> None:
    return None


class TestLoadDeliveryState(unittest.TestCase):
    """Validate load_delivery_state handles errors correctly."""

    def _state_path(self, repo_root: Path) -> Path:
        return safe_path(repo_root, DELIVERY_STATE_REL)

    def test_missing_file_returns_empty_no_warnings(self) -> None:
        """A missing file is normal startup; no warnings should be emitted."""
        with tempfile.TemporaryDirectory() as td:
            result = load_delivery_state(Path(td))
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertNotIn("warnings", result)

    def test_valid_json_returns_data_no_warnings(self) -> None:
        """Valid JSON should be loaded without warnings."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            state = {"version": "1", "records": {"msg1": {"status": "acked"}}, "idempotency": {"k": "v"}}
            path.write_text(json.dumps(state), encoding="utf-8")
            result = load_delivery_state(root)
        self.assertEqual(result["records"], {"msg1": {"status": "acked"}})
        self.assertEqual(result["idempotency"], {"k": "v"})
        self.assertNotIn("warnings", result)

    def test_corrupt_json_returns_empty_with_warning(self) -> None:
        """Corrupt JSON should return empty state with a warning and log at WARNING."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{invalid json", encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("delivery_state_corrupt", result["warnings"][0])
        self.assertTrue(any("Corrupt delivery state" in m for m in cm.output))

    def test_empty_file_returns_empty_with_warning(self) -> None:
        """An empty (zero-byte) file should be treated as corrupt JSON."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertIn("delivery_state_corrupt", result["warnings"][0])
        self.assertTrue(any("Corrupt delivery state" in m for m in cm.output))

    def test_non_dict_json_returns_empty_with_warning(self) -> None:
        """Non-object JSON should return empty state with a warning."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertIn("delivery_state_corrupt", result["warnings"][0])
        self.assertTrue(any("not a JSON object" in m for m in cm.output))

    def test_non_dict_records_returns_warning(self) -> None:
        """Non-dict records field should be reset with a warning."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            state = {"version": "1", "records": "bad", "idempotency": {"k": "v"}}
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {"k": "v"})
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("partial_corrupt", result["warnings"][0])
        self.assertIn("records", result["warnings"][0])
        self.assertTrue(any("non-dict 'records'" in m for m in cm.output))

    def test_non_dict_idempotency_returns_warning(self) -> None:
        """Non-dict idempotency field should be reset with a warning."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            state = {"version": "1", "records": {}, "idempotency": 42}
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("partial_corrupt", result["warnings"][0])
        self.assertIn("idempotency", result["warnings"][0])
        self.assertTrue(any("non-dict 'idempotency'" in m for m in cm.output))

    def test_both_records_and_idempotency_non_dict_returns_two_warnings(self) -> None:
        """Both non-dict fields should each produce a warning."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            state = {"version": "1", "records": None, "idempotency": "bad"}
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertEqual(len(result["warnings"]), 2)

    def test_permission_error_returns_empty_with_warning(self) -> None:
        """An unexpected exception (e.g. PermissionError) should log at ERROR and return a warning via the catch-all handler."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = self._state_path(root)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
            with (
                patch.object(Path, "read_text", side_effect=PermissionError("denied")),
                self.assertLogs("app.messages.service", level=logging.ERROR) as cm,
            ):
                result = load_delivery_state(root)
        self.assertEqual(result["records"], {})
        self.assertEqual(result["idempotency"], {})
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("delivery_state_unreadable", result["warnings"][0])
        self.assertTrue(any("Unexpected error reading" in m for m in cm.output))

    def test_warnings_not_persisted_to_disk(self) -> None:
        """_write_delivery_state should strip warnings before writing."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = {
                "version": "1",
                "records": {"m1": {"status": "acked"}},
                "idempotency": {"k": "v"},
                "warnings": ["should_not_persist"],
            }
            path = _write_delivery_state(root, state)
            on_disk = json.loads(path.read_text(encoding="utf-8"))
        self.assertNotIn("warnings", on_disk)
        self.assertEqual(on_disk["version"], "1")
        self.assertEqual(on_disk["records"], {"m1": {"status": "acked"}})
        self.assertEqual(on_disk["idempotency"], {"k": "v"})


class TestWarningPropagation(unittest.TestCase):
    """Verify that caller functions propagate delivery-state warnings."""

    def test_pending_service_propagates_warnings(self) -> None:
        """messages_pending_service should include warnings from degraded state."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = safe_path(root, DELIVERY_STATE_REL)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{invalid", encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = messages_pending_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    recipient=None,
                    status=None,
                    include_terminal=True,
                    limit=100,
                    parse_iso=_parse_iso,
                    audit=_noop_audit,
                )
        self.assertTrue(result["ok"])
        self.assertIn("warnings", result)
        self.assertIn("delivery_state_corrupt", result["warnings"][0])

    def test_pending_service_omits_warnings_when_healthy(self) -> None:
        """messages_pending_service should not include warnings when state is clean."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = safe_path(root, DELIVERY_STATE_REL)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"version": "1", "records": {}, "idempotency": {}}), encoding="utf-8")
            result = messages_pending_service(
                repo_root=root,
                auth=_AuthStub(),  # type: ignore[arg-type]
                recipient=None,
                status=None,
                include_terminal=True,
                limit=100,
                parse_iso=_parse_iso,
                audit=_noop_audit,
            )
        self.assertTrue(result["ok"])
        self.assertNotIn("warnings", result)

    def test_pending_service_warns_on_non_dict_records(self) -> None:
        """messages_pending_service should warn when individual records are non-dict."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = safe_path(root, DELIVERY_STATE_REL)
            path.parent.mkdir(parents=True, exist_ok=True)
            state = {"version": "1", "records": {"m1": "not-a-dict", "m2": 42}, "idempotency": {}}
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = messages_pending_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    recipient=None,
                    status=None,
                    include_terminal=True,
                    limit=100,
                    parse_iso=_parse_iso,
                    audit=_noop_audit,
                )
        self.assertIn("warnings", result)
        self.assertTrue(any("non-dict record" in w for w in result["warnings"]))


class TestInboxThreadJsonlParsing(unittest.TestCase):
    """Verify inbox and thread services handle malformed JSONL gracefully."""

    def test_inbox_skips_malformed_lines_and_returns_valid(self) -> None:
        """Malformed JSONL lines should be skipped with per-line logging."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inbox_path = safe_path(root, "messages/inbox/agent-a.jsonl")
            inbox_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                json.dumps({"id": "msg1", "body": "hello"}),
                "{corrupt line",
                json.dumps({"id": "msg2", "body": "world"}),
            ]
            inbox_path.write_text("\n".join(lines), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_inbox_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    recipient="agent-a",
                    limit=100,
                    audit=_noop_audit,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 2)
        self.assertEqual(len(result["messages"]), 2)
        self.assertIn("warnings", result)
        self.assertIn("inbox_partial_corrupt", result["warnings"][0])
        self.assertIn("1 malformed", result["warnings"][0])
        # Log includes file line number and truncated content
        self.assertTrue(any("file line 2" in m for m in cm.output))
        self.assertTrue(any("{corrupt line" in m for m in cm.output))

    def test_inbox_no_warnings_when_all_valid(self) -> None:
        """No warnings should appear when all JSONL lines are valid."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inbox_path = safe_path(root, "messages/inbox/agent-b.jsonl")
            inbox_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [json.dumps({"id": "msg1"}), json.dumps({"id": "msg2"})]
            inbox_path.write_text("\n".join(lines), encoding="utf-8")
            result = messages_inbox_service(
                repo_root=root,
                auth=_AuthStub(),  # type: ignore[arg-type]
                recipient="agent-b",
                limit=100,
                audit=_noop_audit,
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 2)
        self.assertNotIn("warnings", result)

    def test_thread_skips_malformed_lines_and_returns_valid(self) -> None:
        """Malformed JSONL lines in thread files should be skipped with per-line logging."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            thread_path = safe_path(root, "messages/threads/thread-1.jsonl")
            thread_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                json.dumps({"id": "msg1"}),
                "not json",
                "",
                json.dumps({"id": "msg2"}),
            ]
            thread_path.write_text("\n".join(lines), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_thread_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    thread_id="thread-1",
                    limit=100,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 2)
        self.assertIn("warnings", result)
        self.assertIn("thread_partial_corrupt", result["warnings"][0])
        # Log includes file line numbers and content
        self.assertTrue(any("file line 2" in m for m in cm.output))
        self.assertTrue(any("not json" in m for m in cm.output))

    def test_inbox_skips_non_dict_json(self) -> None:
        """Non-dict JSON lines (e.g. arrays, strings) should be skipped."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inbox_path = safe_path(root, "messages/inbox/agent-c.jsonl")
            inbox_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                json.dumps({"id": "msg1"}),
                json.dumps([1, 2, 3]),
                json.dumps("just a string"),
                json.dumps({"id": "msg2"}),
            ]
            inbox_path.write_text("\n".join(lines), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.DEBUG) as cm:
                result = messages_inbox_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    recipient="agent-c",
                    limit=100,
                    audit=_noop_audit,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 2)
        self.assertIn("warnings", result)
        self.assertIn("2 malformed", result["warnings"][0])
        self.assertTrue(any("non-dict JSON" in m for m in cm.output))

    def test_thread_skips_non_dict_json(self) -> None:
        """Non-dict JSON lines in thread files should be skipped."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            thread_path = safe_path(root, "messages/threads/thread-2.jsonl")
            thread_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                json.dumps({"id": "msg1"}),
                json.dumps(42),
                json.dumps({"id": "msg2"}),
            ]
            thread_path.write_text("\n".join(lines), encoding="utf-8")
            with self.assertLogs("app.messages.service", level=logging.DEBUG) as cm:
                result = messages_thread_service(
                    repo_root=root,
                    auth=_AuthStub(),  # type: ignore[arg-type]
                    thread_id="thread-2",
                    limit=100,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 2)
        self.assertIn("warnings", result)
        self.assertIn("1 malformed", result["warnings"][0])
        self.assertTrue(any("non-dict JSON" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
