"""Tests for UTF-8 replacement handling in JSONL readers (issue #74).

Verifies that invalid UTF-8 bytes are replaced with U+FFFD rather than
silently dropped, and that a warning is logged when replacement occurs.
"""

import json
import logging
import tempfile
import unittest
from pathlib import Path

from app.messages.service import messages_inbox_service, messages_thread_service
from app.ops.service import _load_ops_runs


class _AuthStub:
    """Auth stub that permits the scopes used by reader tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class TestUtf8ReplacementMessages(unittest.TestCase):
    """Invalid UTF-8 bytes in inbox/thread JSONL should be replaced, not dropped."""

    def test_inbox_replaces_invalid_utf8_and_warns(self) -> None:
        """messages_inbox_service replaces invalid bytes and logs a warning."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            # Write a valid JSON line with an invalid UTF-8 byte embedded
            line = json.dumps({"body": "hello"}).encode("utf-8") + b"\n"
            bad_line = b'{"body": "caf\\u00e9' + b"\xfe" + b'"}\n'
            (inbox / "agent-a.jsonl").write_bytes(line + bad_line)

            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_inbox_service(
                    repo_root=repo, auth=_AuthStub(), recipient="agent-a", limit=50,
                    audit=lambda *_a, **_kw: None,
                )

            self.assertTrue(result["ok"])
            # The valid line should be present
            self.assertGreaterEqual(result["count"], 1)
            # Warning about invalid UTF-8 should have been logged
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_thread_replaces_invalid_utf8_and_warns(self) -> None:
        """messages_thread_service replaces invalid bytes and logs a warning."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            threads = repo / "messages" / "threads"
            threads.mkdir(parents=True)
            line = json.dumps({"body": "ok"}).encode("utf-8") + b"\n"
            bad_line = b'{"body": "caf\\u00e9' + b"\xfe" + b'"}\n'
            (threads / "t1.jsonl").write_bytes(line + bad_line)

            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_thread_service(
                    repo_root=repo, auth=_AuthStub(), thread_id="t1", limit=50,
                )

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["count"], 1)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_inbox_clean_utf8_no_warning(self) -> None:
        """No warning when inbox file contains only valid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            line = json.dumps({"body": "café"}).encode("utf-8") + b"\n"
            (inbox / "agent-b.jsonl").write_bytes(line)

            logger = logging.getLogger("app.messages.service")
            with self.assertRaises(AssertionError):
                # assertLogs raises AssertionError when no logs are emitted
                with self.assertLogs(logger, level=logging.WARNING):
                    messages_inbox_service(
                        repo_root=repo, auth=_AuthStub(), recipient="agent-b", limit=50,
                        audit=lambda *_a, **_kw: None,
                    )


class TestUtf8ReplacementOps(unittest.TestCase):
    """Invalid UTF-8 bytes in ops runs JSONL should be replaced, not dropped."""

    def test_ops_runs_replaces_invalid_utf8_and_warns(self) -> None:
        """_load_ops_runs replaces invalid bytes and logs a warning."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            runs_dir = repo / "logs"
            runs_dir.mkdir(parents=True)
            good = json.dumps({"op": "test", "status": "ok"}).encode("utf-8") + b"\n"
            bad = b'{"op": "bad' + b"\xfe" + b'"}\n'
            (runs_dir / "ops_runs.jsonl").write_bytes(good + bad)

            with self.assertLogs("app.ops.service", level=logging.WARNING) as cm:
                result = _load_ops_runs(repo)

            self.assertGreaterEqual(len(result), 1)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))


if __name__ == "__main__":
    unittest.main()
