"""Tests for UTF-8 replacement handling across all file readers (issue #74).

Covers messages (inbox + thread), ops, continuity, maintenance, context,
and indexer modules to verify that invalid UTF-8 bytes are replaced with
U+FFFD rather than silently dropped, and that warnings are logged.
"""

import json
import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.context.service import _raw_scan_recent_relevant
from app.continuity.service import _audit_recent_selectors
from app.indexer import _record_for_file
from app.config import Settings
from app.maintenance.service import _candidate_policy, _load_access_stats, metrics_service
from app.messages.service import messages_inbox_service, messages_thread_service
from app.models import ContextRetrieveRequest
from app.ops.service import _load_ops_runs
from tests.helpers import AllowAllAuthStub


def _noop_audit(*_args, **_kwargs):
    """No-op audit callable for service functions that require one."""


def _parse_iso_stub(_value):
    """Stub parse_iso that always returns None."""
    return None


def _write_corrupt_jsonl(path: Path, good_body: str = "hello") -> None:
    """Write a JSONL file with one valid line and one containing an invalid UTF-8 byte."""
    good = json.dumps({"body": good_body}).encode("utf-8") + b"\n"
    bad = b'{"body": "caf\xc3\xa9' + b"\xfe" + b'"}\n'
    path.write_bytes(good + bad)


class TestUtf8ReplacementMessages(unittest.TestCase):
    """Invalid UTF-8 bytes in inbox/thread JSONL should be replaced, not dropped."""

    def test_inbox_replaces_invalid_utf8_and_warns(self) -> None:
        """Inbox reader replaces invalid bytes, logs a warning, and surfaces it in API response."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            _write_corrupt_jsonl(inbox / "agent-a.jsonl")

            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_inbox_service(
                    repo_root=repo, auth=AllowAllAuthStub(), recipient="agent-a",
                    limit=50, audit=_noop_audit,
                )

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["count"], 1)
            # Warning logged about U+FFFD
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))
            # Corruption surfaced in API response warnings
            warnings = result.get("warnings", [])
            self.assertTrue(any("utf8_corrupted" in w for w in warnings))

    def test_inbox_ufffd_present_in_returned_data(self) -> None:
        """Replacement character U+FFFD actually appears in the returned message body."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            _write_corrupt_jsonl(inbox / "agent-a.jsonl")

            with self.assertLogs("app.messages.service", level=logging.WARNING):
                result = messages_inbox_service(
                    repo_root=repo, auth=AllowAllAuthStub(), recipient="agent-a",
                    limit=50, audit=_noop_audit,
                )

            bodies = [m.get("body", "") for m in result["messages"]]
            self.assertTrue(
                any("\ufffd" in body for body in bodies),
                "Expected U+FFFD in at least one message body",
            )

    def test_thread_replaces_invalid_utf8_and_warns(self) -> None:
        """Thread reader replaces invalid bytes and surfaces corruption in API warnings."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            threads = repo / "messages" / "threads"
            threads.mkdir(parents=True)
            _write_corrupt_jsonl(threads / "t1.jsonl")

            with self.assertLogs("app.messages.service", level=logging.WARNING) as cm:
                result = messages_thread_service(
                    repo_root=repo, auth=AllowAllAuthStub(), thread_id="t1", limit=50,
                )

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["count"], 1)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))
            warnings = result.get("warnings", [])
            self.assertTrue(any("utf8_corrupted" in w for w in warnings))
            # U+FFFD should appear in returned message bodies
            bodies = [m.get("body", "") for m in result["messages"]]
            self.assertTrue(
                any("\ufffd" in body for body in bodies),
                "Expected U+FFFD in at least one thread message body",
            )

    def test_inbox_clean_utf8_no_warning(self) -> None:
        """No warning when inbox file contains only valid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            inbox = repo / "messages" / "inbox"
            inbox.mkdir(parents=True)
            line = json.dumps({"body": "café"}).encode("utf-8") + b"\n"
            (inbox / "agent-b.jsonl").write_bytes(line)

            with patch("app.messages.service._logger") as mock_logger:
                result = messages_inbox_service(
                    repo_root=repo, auth=AllowAllAuthStub(), recipient="agent-b",
                    limit=50, audit=_noop_audit,
                )

            self.assertTrue(result["ok"])
            mock_logger.warning.assert_not_called()
            self.assertNotIn("warnings", result)


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
                result, _warnings = _load_ops_runs(repo)

            self.assertGreaterEqual(len(result), 1)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_ops_runs_graceful_on_unreadable_file(self) -> None:
        """_load_ops_runs returns empty list on I/O error instead of crashing."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            runs_dir = repo / "logs"
            runs_dir.mkdir(parents=True)
            path = runs_dir / "ops_runs.jsonl"
            path.write_text("{}\n")
            path.chmod(0o000)
            try:
                with self.assertLogs("app.ops.service", level=logging.WARNING):
                    result, _warnings = _load_ops_runs(repo)
                self.assertEqual(result, [])
            finally:
                path.chmod(0o644)


class TestUtf8ReplacementContinuity(unittest.TestCase):
    """Invalid UTF-8 bytes in continuity audit log should be replaced, not dropped."""

    def test_audit_recent_selectors_warns_on_corrupt_utf8(self) -> None:
        """_audit_recent_selectors logs warning when audit log has invalid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            good = json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "event": "read",
                "detail": {"selector": "task:demo", "subject": "task:demo"},
            }).encode("utf-8") + b"\n"
            bad = b'{"ts": "2026-01-01T00:00:00Z", "event": "read\xfe"}\n'
            (logs_dir / "api_audit.jsonl").write_bytes(good + bad)

            with self.assertLogs("app.continuity.service", level=logging.WARNING) as cm:
                _audit_recent_selectors(repo, datetime.now(timezone.utc))

            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_audit_recent_selectors_logs_on_io_error(self) -> None:
        """_audit_recent_selectors logs warning on unreadable audit file."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            path = logs_dir / "api_audit.jsonl"
            path.write_text("{}\n")
            path.chmod(0o000)
            try:
                with self.assertLogs("app.continuity.service", level=logging.WARNING):
                    result = _audit_recent_selectors(repo, datetime.now(timezone.utc))
                self.assertEqual(result, set())
            finally:
                path.chmod(0o644)


class TestUtf8ReplacementMaintenance(unittest.TestCase):
    """Invalid UTF-8 in maintenance readers should be replaced, not dropped."""

    def test_load_access_stats_warns_on_corrupt_utf8(self) -> None:
        """_load_access_stats logs warning when audit log has invalid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            good = json.dumps({"event": "read", "detail": {"path": "journal/test.md"}}).encode("utf-8") + b"\n"
            bad = b'{"event": "read\xfe"}\n'
            (logs_dir / "api_audit.jsonl").write_bytes(good + bad)

            with self.assertLogs("app.maintenance.service", level=logging.WARNING) as cm:
                _load_access_stats(repo)

            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_candidate_policy_warns_on_corrupt_utf8(self) -> None:
        """_candidate_policy logs warning when a text file has invalid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            test_file = repo / "journal" / "note.md"
            test_file.parent.mkdir(parents=True)
            test_file.write_bytes(b"# Hello\xfe\n")

            with self.assertLogs("app.maintenance.service", level=logging.WARNING) as cm:
                result = _candidate_policy(repo, test_file, {}, parse_iso=_parse_iso_stub)

            self.assertIsNotNone(result)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_candidate_policy_logs_on_io_error(self) -> None:
        """_candidate_policy logs warning on unreadable file instead of crashing."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            test_file = repo / "journal" / "note.md"
            test_file.parent.mkdir(parents=True)
            test_file.write_text("# OK\n")
            test_file.chmod(0o000)
            try:
                with self.assertLogs("app.maintenance.service", level=logging.WARNING):
                    result = _candidate_policy(repo, test_file, {}, parse_iso=_parse_iso_stub)
                # Should not crash; returns a policy with empty text
                self.assertIsNotNone(result)
            finally:
                test_file.chmod(0o644)


    def test_load_access_stats_graceful_on_unreadable_file(self) -> None:
        """_load_access_stats returns empty dict on I/O error instead of crashing."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            path = logs_dir / "api_audit.jsonl"
            path.write_text("{}\n")
            path.chmod(0o000)
            try:
                with self.assertLogs("app.maintenance.service", level=logging.WARNING):
                    result = _load_access_stats(repo)
                self.assertEqual(result, {})
            finally:
                path.chmod(0o644)

    def test_metrics_service_warns_on_corrupt_utf8(self) -> None:
        """metrics_service logs warning when audit log has invalid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            good = json.dumps({"event": "read", "peer_id": "p1"}).encode("utf-8") + b"\n"
            bad = b'{"event": "write\xfe", "peer_id": "p2"}\n'
            (logs_dir / "api_audit.jsonl").write_bytes(good + bad)

            settings = Settings(
                repo_root=repo, auto_init_git=False,
                git_author_name="n/a", git_author_email="n/a",
                tokens={}, audit_log_enabled=False,
            )

            with self.assertLogs("app.maintenance.service", level=logging.WARNING) as cm:
                result = metrics_service(
                    settings=settings, auth=AllowAllAuthStub(),
                    load_delivery_state=lambda _r: {"records": {}},
                    delivery_record_view=lambda _r, _n: {},
                    load_check_artifacts=lambda _r: [],
                    load_rate_limit_state=lambda _r: {},
                    parse_iso=_parse_iso_stub,
                )

            self.assertTrue(result.get("ok", False) or "event_counts" in result)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))

    def test_metrics_service_graceful_on_unreadable_audit(self) -> None:
        """metrics_service degrades gracefully when audit log is unreadable."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            logs_dir = repo / "logs"
            logs_dir.mkdir(parents=True)
            path = logs_dir / "api_audit.jsonl"
            path.write_text("{}\n")
            path.chmod(0o000)

            settings = Settings(
                repo_root=repo, auto_init_git=False,
                git_author_name="n/a", git_author_email="n/a",
                tokens={}, audit_log_enabled=False,
            )

            try:
                with self.assertLogs("app.maintenance.service", level=logging.WARNING):
                    result = metrics_service(
                        settings=settings, auth=AllowAllAuthStub(),
                        load_delivery_state=lambda _r: {"records": {}},
                        delivery_record_view=lambda _r, _n: {},
                        load_check_artifacts=lambda _r: [],
                        load_rate_limit_state=lambda _r: {},
                        parse_iso=_parse_iso_stub,
                    )
                # Should not crash; event/peer counts should be empty
                self.assertEqual(result.get("event_counts", {}), {})
            finally:
                path.chmod(0o644)


class TestUtf8ReplacementContext(unittest.TestCase):
    """Invalid UTF-8 bytes in context scan should be replaced, not dropped."""

    def test_raw_scan_warns_on_corrupt_utf8(self) -> None:
        """_raw_scan_recent_relevant logs warning when a scanned file has invalid UTF-8."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            journal = repo / "journal"
            journal.mkdir(parents=True)
            (journal / "note.md").write_bytes(b"# Caf\xc3\xa9\xfe content\n")

            req = ContextRetrieveRequest(task="café note")

            with self.assertLogs("app.context.service", level=logging.WARNING) as cm:
                results = _raw_scan_recent_relevant(repo, AllowAllAuthStub(), req)

            self.assertGreaterEqual(len(results), 1)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))
            # U+FFFD should appear in the scanned snippet
            self.assertTrue(
                any("\ufffd" in r.get("snippet", "") for r in results),
                "Expected U+FFFD in at least one context scan snippet",
            )


class TestUtf8ReplacementIndexer(unittest.TestCase):
    """Invalid UTF-8 bytes in indexed files should be replaced, not dropped."""

    def test_record_for_file_warns_on_corrupt_utf8(self) -> None:
        """_record_for_file replaces invalid bytes and logs a warning."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            test_file = repo / "journal" / "note.md"
            test_file.parent.mkdir(parents=True)
            test_file.write_bytes(b"# Caf\xc3\xa9\xfe content\n")

            with self.assertLogs("app.indexer", level=logging.WARNING) as cm:
                record = _record_for_file(repo, test_file)

            self.assertIsNotNone(record)
            self.assertTrue(any("U+FFFD" in msg for msg in cm.output))
            # U+FFFD should appear in the indexed snippet
            self.assertIn("\ufffd", record.get("snippet", ""))


if __name__ == "__main__":
    unittest.main()
