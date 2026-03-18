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
    messages_pending_service,
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

    def test_permission_error_returns_empty_with_warning(self) -> None:
        """A PermissionError should log at ERROR and return a warning."""
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


if __name__ == "__main__":
    unittest.main()
