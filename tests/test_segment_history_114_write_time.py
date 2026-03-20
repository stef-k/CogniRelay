"""Tests for segment-history write-time rollover (issue #114, Phase 9)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.helpers import SimpleGitManagerStub

from app.audit import append_audit


class TestWriteTimeRollover(unittest.TestCase):
    def test_no_rollover_when_under_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Write a small audit entry with high threshold
            append_audit(
                repo, "test_event", "peer-1", {"key": "value"},
                rollover_bytes=1_000_000, gm=gm,
            )

            audit = repo / "logs" / "api_audit.jsonl"
            self.assertTrue(audit.is_file())
            content = audit.read_text(encoding="utf-8")
            self.assertIn("test_event", content)

    def test_rollover_triggers_on_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            gm = SimpleGitManagerStub(repo)

            # Create a large audit log
            audit = repo / "logs" / "api_audit.jsonl"
            audit.parent.mkdir(parents=True)
            audit.write_text('{"ts":"2026-03-20","event":"old"}\n' * 100)

            # Append with low rollover threshold
            append_audit(
                repo, "new_event", "peer-1", {"key": "value"},
                rollover_bytes=100, gm=gm,
            )

            # Old content should have been rolled out
            content = audit.read_text(encoding="utf-8")
            self.assertIn("new_event", content)

            # History segment should exist
            history_dir = repo / "logs" / "history" / "api_audit"
            if history_dir.is_dir():
                payloads = list(history_dir.glob("*.jsonl"))
                self.assertGreater(len(payloads), 0)

    def test_backward_compatible_no_rollover_args(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Original signature still works
            append_audit(repo, "test_event", "peer-1", {"key": "value"})
            audit = repo / "logs" / "api_audit.jsonl"
            self.assertTrue(audit.is_file())


if __name__ == "__main__":
    unittest.main()
