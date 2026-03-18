"""Tests for load_delivery_state error handling and warning propagation."""

import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.messages.service import (
    _write_delivery_state,
    load_delivery_state,
)
from app.storage import safe_path


class TestLoadDeliveryState(unittest.TestCase):
    """Validate load_delivery_state handles errors correctly."""

    def _state_path(self, repo_root: Path) -> Path:
        return safe_path(repo_root, "messages/state/delivery_index.json")

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
                "records": {},
                "idempotency": {},
                "warnings": ["should_not_persist"],
            }
            path = _write_delivery_state(root, state)
            on_disk = json.loads(path.read_text(encoding="utf-8"))
        self.assertNotIn("warnings", on_disk)
        self.assertEqual(on_disk["version"], "1")


if __name__ == "__main__":
    unittest.main()
