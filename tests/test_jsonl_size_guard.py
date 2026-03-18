"""Tests for JSONL file size guard to prevent OOM on large files (issue #75).

Verifies that messages_inbox_service, messages_thread_service, and
_load_ops_runs return degraded responses when a JSONL file exceeds the
configured size threshold.
"""

import json
import logging
import tempfile
import unittest
from pathlib import Path

from app.messages.service import messages_inbox_service, messages_thread_service
from app.ops.service import _load_ops_runs
from tests.helpers import AllowAllAuthStub


def _noop_audit(*_args, **_kwargs):
    """No-op audit callable for service functions that require one."""


def _make_large_jsonl(path: Path, target_bytes: int) -> None:
    """Write a JSONL file that exceeds the given byte threshold."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"body": "x" * 200}) + "\n"
    line_bytes = line.encode("utf-8")
    count = (target_bytes // len(line_bytes)) + 2
    path.write_bytes(line_bytes * count)
    assert path.stat().st_size > target_bytes


class TestInboxSizeGuard(unittest.TestCase):
    """messages_inbox_service returns degraded response for oversized files."""

    def test_oversized_inbox_returns_warning(self) -> None:
        """Inbox reader returns empty messages with a warning when file is too large."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox_dir = repo / "messages" / "inbox"
            inbox_dir.mkdir(parents=True)
            inbox_file = inbox_dir / "agent-a.jsonl"
            threshold = 1024  # 1 KB for testing
            _make_large_jsonl(inbox_file, threshold)

            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = messages_inbox_service(
                    repo_root=repo,
                    auth=AllowAllAuthStub(),
                    recipient="agent-a",
                    limit=20,
                    audit=_noop_audit,
                    max_jsonl_read_bytes=threshold,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 0)
            self.assertEqual(result["messages"], [])
            self.assertIn("warnings", result)
            self.assertTrue(any("inbox_too_large" in w for w in result["warnings"]))

    def test_inbox_under_limit_reads_normally(self) -> None:
        """Inbox reader works normally when file is under the size limit."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox_dir = repo / "messages" / "inbox"
            inbox_dir.mkdir(parents=True)
            inbox_file = inbox_dir / "agent-a.jsonl"
            msg = json.dumps({"body": "hello"})
            inbox_file.write_text(msg + "\n", encoding="utf-8")

            result = messages_inbox_service(
                repo_root=repo,
                auth=AllowAllAuthStub(),
                recipient="agent-a",
                limit=20,
                audit=_noop_audit,
                max_jsonl_read_bytes=10 * 1024 * 1024,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 1)
            self.assertNotIn("warnings", result)


class TestThreadSizeGuard(unittest.TestCase):
    """messages_thread_service returns degraded response for oversized files."""

    def test_oversized_thread_returns_warning(self) -> None:
        """Thread reader returns empty messages with a warning when file is too large."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            thread_dir = repo / "messages" / "threads"
            thread_dir.mkdir(parents=True)
            thread_file = thread_dir / "thread-xyz.jsonl"
            threshold = 1024
            _make_large_jsonl(thread_file, threshold)

            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = messages_thread_service(
                    repo_root=repo,
                    auth=AllowAllAuthStub(),
                    thread_id="thread-xyz",
                    limit=100,
                    max_jsonl_read_bytes=threshold,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 0)
            self.assertEqual(result["messages"], [])
            self.assertIn("warnings", result)
            self.assertTrue(any("thread_too_large" in w for w in result["warnings"]))

    def test_thread_under_limit_reads_normally(self) -> None:
        """Thread reader works normally when file is under the size limit."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            thread_dir = repo / "messages" / "threads"
            thread_dir.mkdir(parents=True)
            thread_file = thread_dir / "thread-xyz.jsonl"
            msg = json.dumps({"body": "hello"})
            thread_file.write_text(msg + "\n", encoding="utf-8")

            result = messages_thread_service(
                repo_root=repo,
                auth=AllowAllAuthStub(),
                thread_id="thread-xyz",
                limit=100,
                max_jsonl_read_bytes=10 * 1024 * 1024,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 1)
            self.assertNotIn("warnings", result)


class TestOpsRunsSizeGuard(unittest.TestCase):
    """_load_ops_runs returns empty list for oversized files."""

    def test_oversized_ops_runs_returns_empty(self) -> None:
        """Ops runs reader returns empty list when file is too large."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            ops_file = logs_dir / "ops_runs.jsonl"
            threshold = 1024
            _make_large_jsonl(ops_file, threshold)

            with self.assertLogs("app.ops.service", level=logging.WARNING):
                result = _load_ops_runs(repo, max_jsonl_read_bytes=threshold)

            self.assertEqual(result, [])

    def test_ops_runs_under_limit_reads_normally(self) -> None:
        """Ops runs reader works normally when file is under the size limit."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            ops_file = logs_dir / "ops_runs.jsonl"
            row = json.dumps({"job_id": "test", "status": "succeeded"})
            ops_file.write_text(row + "\n", encoding="utf-8")

            result = _load_ops_runs(repo, max_jsonl_read_bytes=10 * 1024 * 1024)

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["job_id"], "test")


if __name__ == "__main__":
    unittest.main()
