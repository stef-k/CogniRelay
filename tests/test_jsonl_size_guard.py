"""Tests for JSONL file size guard to prevent OOM on large files (issue #75).

Verifies that messages_inbox_service, messages_thread_service, and
_load_ops_runs return degraded responses when a JSONL file exceeds the
configured size threshold, and that stat() failures are handled safely.
"""

import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import DEFAULT_MAX_JSONL_READ_BYTES
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


def _stat_fails_on_target(target_path: Path):
    """Return a replacement for Path.stat that raises OSError only for target_path.

    The code under test calls ``path.exists()`` (which internally calls
    ``stat()``) before the explicit ``path.stat()`` in the size guard.
    We let the first ``stat()`` call on the target succeed so
    ``exists()`` returns True, then raise on the second call.
    """
    _real_stat = Path.stat
    # Resolve once before patching so we can compare by string
    target_str = str(target_path.resolve())
    state = {"calls": 0}

    def _replacement(self, *args, **kwargs):
        if str(self) == target_str:
            state["calls"] += 1
            # Allow calls from safe_path resolve() and exists() to succeed;
            # fail only on the explicit stat() in the size guard.
            if state["calls"] <= 2:
                return _real_stat(self, *args, **kwargs)
            raise OSError("permission denied")
        return _real_stat(self, *args, **kwargs)

    return _replacement


# ---------------------------------------------------------------------------
# Inbox size guard
# ---------------------------------------------------------------------------
class TestInboxSizeGuard(unittest.TestCase):
    """messages_inbox_service returns degraded response for oversized files."""

    def test_oversized_inbox_returns_warning_and_degraded(self) -> None:
        """Inbox reader returns empty messages with a warning and degraded flag when file is too large."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox_dir = repo / "messages" / "inbox"
            inbox_dir.mkdir(parents=True)
            inbox_file = inbox_dir / "agent-a.jsonl"
            threshold = 1024
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
            self.assertTrue(result["degraded"])
            self.assertEqual(result["count"], 0)
            self.assertEqual(result["messages"], [])
            self.assertIn("warnings", result)
            self.assertTrue(any("inbox_too_large" in w for w in result["warnings"]))
            self.assertTrue(any("compacted or truncated" in w for w in result["warnings"]))

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
            self.assertNotIn("degraded", result)

    def test_inbox_stat_oserror_returns_degraded(self) -> None:
        """Inbox reader returns degraded response when stat() raises OSError."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox_dir = repo / "messages" / "inbox"
            inbox_dir.mkdir(parents=True)
            inbox_file = inbox_dir / "agent-a.jsonl"
            inbox_file.write_text(json.dumps({"body": "x"}) + "\n", encoding="utf-8")

            with patch.object(Path, "stat", _stat_fails_on_target(inbox_file)):
                with self.assertLogs("app.messages.service", level=logging.WARNING):
                    result = messages_inbox_service(
                        repo_root=repo,
                        auth=AllowAllAuthStub(),
                        recipient="agent-a",
                        limit=20,
                        audit=_noop_audit,
                    )

            self.assertTrue(result["ok"])
            self.assertTrue(result["degraded"])
            self.assertEqual(result["count"], 0)
            self.assertTrue(any("inbox_stat_failed" in w for w in result["warnings"]))

    def test_inbox_file_exactly_at_threshold_reads_normally(self) -> None:
        """A file whose size equals the threshold is still read (guard is > not >=)."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox_dir = repo / "messages" / "inbox"
            inbox_dir.mkdir(parents=True)
            inbox_file = inbox_dir / "agent-a.jsonl"
            msg = json.dumps({"body": "hello"})
            inbox_file.write_text(msg + "\n", encoding="utf-8")
            threshold = inbox_file.stat().st_size  # exactly at boundary

            result = messages_inbox_service(
                repo_root=repo,
                auth=AllowAllAuthStub(),
                recipient="agent-a",
                limit=20,
                audit=_noop_audit,
                max_jsonl_read_bytes=threshold,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 1)


# ---------------------------------------------------------------------------
# Thread size guard
# ---------------------------------------------------------------------------
class TestThreadSizeGuard(unittest.TestCase):
    """messages_thread_service returns degraded response for oversized files."""

    def test_oversized_thread_returns_warning_and_degraded(self) -> None:
        """Thread reader returns empty messages with a warning and degraded flag when file is too large."""
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
            self.assertTrue(result["degraded"])
            self.assertEqual(result["count"], 0)
            self.assertEqual(result["messages"], [])
            self.assertIn("warnings", result)
            self.assertTrue(any("thread_too_large" in w for w in result["warnings"]))
            self.assertTrue(any("compacted or truncated" in w for w in result["warnings"]))

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

    def test_thread_stat_oserror_returns_degraded(self) -> None:
        """Thread reader returns degraded response when stat() raises OSError."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            thread_dir = repo / "messages" / "threads"
            thread_dir.mkdir(parents=True)
            thread_file = thread_dir / "thread-xyz.jsonl"
            thread_file.write_text(json.dumps({"body": "x"}) + "\n", encoding="utf-8")

            with patch.object(Path, "stat", _stat_fails_on_target(thread_file)):
                with self.assertLogs("app.messages.service", level=logging.WARNING):
                    result = messages_thread_service(
                        repo_root=repo,
                        auth=AllowAllAuthStub(),
                        thread_id="thread-xyz",
                        limit=100,
                    )

            self.assertTrue(result["ok"])
            self.assertTrue(result["degraded"])
            self.assertEqual(result["count"], 0)
            self.assertTrue(any("thread_stat_failed" in w for w in result["warnings"]))


# ---------------------------------------------------------------------------
# Ops runs size guard
# ---------------------------------------------------------------------------
class TestOpsRunsSizeGuard(unittest.TestCase):
    """_load_ops_runs returns empty list with warnings for oversized files."""

    def test_oversized_ops_runs_returns_empty_with_warning(self) -> None:
        """Ops runs reader returns empty list with warning when file is too large."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            ops_file = logs_dir / "ops_runs.jsonl"
            threshold = 1024
            _make_large_jsonl(ops_file, threshold)

            with self.assertLogs("app.ops.service", level=logging.WARNING):
                runs, warnings = _load_ops_runs(repo, max_jsonl_read_bytes=threshold)

            self.assertEqual(runs, [])
            self.assertTrue(any("ops_runs_too_large" in w for w in warnings))

    def test_ops_runs_under_limit_reads_normally(self) -> None:
        """Ops runs reader works normally when file is under the size limit."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            ops_file = logs_dir / "ops_runs.jsonl"
            row = json.dumps({"job_id": "test", "status": "succeeded"})
            ops_file.write_text(row + "\n", encoding="utf-8")

            runs, warnings = _load_ops_runs(repo, max_jsonl_read_bytes=10 * 1024 * 1024)

            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["job_id"], "test")
            self.assertEqual(warnings, [])

    def test_ops_runs_stat_oserror_returns_empty_with_warning(self) -> None:
        """Ops runs reader returns empty list with warning when stat() raises OSError."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            ops_file = logs_dir / "ops_runs.jsonl"
            ops_file.write_text(json.dumps({"job_id": "x"}) + "\n", encoding="utf-8")

            with patch.object(Path, "stat", _stat_fails_on_target(ops_file)):
                with self.assertLogs("app.ops.service", level=logging.WARNING):
                    runs, warnings = _load_ops_runs(repo)

            self.assertEqual(runs, [])
            self.assertTrue(any("ops_runs_stat_failed" in w for w in warnings))

    def test_ops_runs_missing_file_returns_empty_no_warning(self) -> None:
        """Missing ops runs file returns empty with no warnings."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            runs, warnings = _load_ops_runs(repo)
            self.assertEqual(runs, [])
            self.assertEqual(warnings, [])


# ---------------------------------------------------------------------------
# Config constant and env parsing
# ---------------------------------------------------------------------------
class TestConfigDefault(unittest.TestCase):
    """DEFAULT_MAX_JSONL_READ_BYTES is importable and sane."""

    def test_default_is_10mb(self) -> None:
        """The default constant equals 10 MB."""
        self.assertEqual(DEFAULT_MAX_JSONL_READ_BYTES, 10 * 1024 * 1024)


class TestConfigEnvParsing(unittest.TestCase):
    """COGNIRELAY_MAX_JSONL_READ_BYTES env var is parsed by get_settings."""

    def test_env_var_overrides_default(self) -> None:
        """Settings picks up the env var value."""
        from app.config import get_settings

        with patch.dict(os.environ, {"COGNIRELAY_MAX_JSONL_READ_BYTES": "2048"}):
            settings = get_settings(force_reload=True)
            self.assertEqual(settings.max_jsonl_read_bytes, 2048)
        # Restore default
        get_settings(force_reload=True)

    def test_env_var_below_minimum_is_clamped(self) -> None:
        """Values below minimum (1024) are clamped up."""
        from app.config import get_settings

        with patch.dict(os.environ, {"COGNIRELAY_MAX_JSONL_READ_BYTES": "100"}):
            settings = get_settings(force_reload=True)
            self.assertGreaterEqual(settings.max_jsonl_read_bytes, 1024)
        get_settings(force_reload=True)


if __name__ == "__main__":
    unittest.main()
