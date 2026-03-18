"""Tests for partial-failure handling in message service append operations."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import messages_send, relay_forward, replay_messages
from app.models import MessageReplayRequest, MessageSendRequest, RelayForwardRequest


class _AuthStub:
    """Auth stub that permits all scopes for test purposes."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    """Git manager stub for partial-failure tests."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        return True

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        return True

    def latest_commit(self) -> str:
        return "test-sha"


class _FailingCommitGitManagerStub(_GitManagerStub):
    """Git manager stub where commit_paths raises an exception."""

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        raise RuntimeError("git commit failed")


class _FalseCommitGitManagerStub(_GitManagerStub):
    """Git manager stub where commit_paths returns False (no changes committed)."""

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        return False


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


class TestMessagesSendPartialFailure(unittest.TestCase):
    """Verify partial-failure handling in messages_send."""

    def test_send_oserror_propagates_no_partial_state(self) -> None:
        """When append_jsonl_multi raises OSError, no JSONL files are left behind."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub()
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.messages.service.append_jsonl_multi", side_effect=OSError("disk full")):
                    with self.assertRaises(OSError):
                        messages_send(req=req, auth=_AuthStub())

            inbox = repo_root / "messages" / "inbox" / "peer-b.jsonl"
            outbox = repo_root / "messages" / "outbox" / "peer-a.jsonl"
            thread = repo_root / "messages" / "threads" / "thread-1.jsonl"
            for p in (inbox, outbox, thread):
                self.assertFalse(p.exists(), f"Expected {p.name} to not exist after OSError")

    # --- Gap 2: real rollback exercised (not mocked) ---

    def test_send_real_rollback_on_io_failure(self) -> None:
        """Exercise real rollback logic by failing the third file during append."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub()
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
            )

            # Make the thread directory read-only so the third file fails
            thread_dir = repo_root / "messages" / "threads"
            thread_dir.mkdir(parents=True, exist_ok=True)

            original_open = Path.open
            thread_file = thread_dir / "thread-1.jsonl"

            def fail_third(self_path, *args, **kwargs):
                if self_path == thread_file and "a" in args:
                    raise OSError("disk full")
                return original_open(self_path, *args, **kwargs)

            with patch("app.main._services", return_value=(settings, gm)):
                with patch.object(Path, "open", fail_third):
                    with self.assertRaises(OSError):
                        messages_send(req=req, auth=_AuthStub())

            # Rollback should have cleaned up the first two files
            inbox = repo_root / "messages" / "inbox" / "peer-b.jsonl"
            outbox = repo_root / "messages" / "outbox" / "peer-a.jsonl"
            self.assertFalse(inbox.exists(), "inbox should be deleted by rollback")
            self.assertFalse(outbox.exists(), "outbox should be deleted by rollback")

    # --- Gap 7: commit_paths returning False ---

    def test_send_commit_paths_returns_false(self) -> None:
        """When commit_paths returns False, committed_files is empty but data is on disk."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _FalseCommitGitManagerStub()
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                result = messages_send(req=req, auth=_AuthStub())

            # Data is on disk
            inbox = repo_root / "messages" / "inbox" / "peer-b.jsonl"
            self.assertTrue(inbox.exists())
            # But committed_files should not include the message rels
            msg_rels = [
                "messages/inbox/peer-b.jsonl",
                "messages/outbox/peer-a.jsonl",
                "messages/threads/thread-1.jsonl",
            ]
            for rel in msg_rels:
                self.assertNotIn(rel, result["committed_files"])

    # --- C3: commit_paths exception degrades gracefully ---

    def test_send_commit_paths_exception_degrades_gracefully(self) -> None:
        """When commit_paths raises, the service still returns a result (data on disk)."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _FailingCommitGitManagerStub()
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                result = messages_send(req=req, auth=_AuthStub())

            self.assertTrue(result["ok"])
            self.assertIn("id", result["message"])
            # Data is on disk despite git failure
            inbox = repo_root / "messages" / "inbox" / "peer-b.jsonl"
            self.assertTrue(inbox.exists())
            lines = inbox.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)


class TestRelayForwardPartialFailure(unittest.TestCase):
    """Verify partial-failure handling in relay_forward."""

    # --- Gap 4: relay_forward failure path ---

    def test_relay_oserror_propagates(self) -> None:
        """When append_jsonl_multi raises OSError in relay_forward, it propagates."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub()
            req = RelayForwardRequest(
                relay_id="relay-1",
                target_recipient="peer-b",
                thread_id="thread-1",
                sender="peer-a",
                subject="relayed",
                body_md="content",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.messages.service.append_jsonl_multi", side_effect=OSError("disk full")):
                    with self.assertRaises(OSError):
                        relay_forward(req=req, auth=_AuthStub())

    def test_relay_commit_paths_exception_degrades(self) -> None:
        """When commit_paths raises in relay_forward, the service still returns."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _FailingCommitGitManagerStub()
            req = RelayForwardRequest(
                relay_id="relay-1",
                target_recipient="peer-b",
                thread_id="thread-1",
                sender="peer-a",
                subject="relayed",
                body_md="content",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                result = relay_forward(req=req, auth=_AuthStub())

            self.assertTrue(result["ok"])


class TestReplayPartialFailure(unittest.TestCase):
    """Verify partial-failure handling in replay_messages."""

    def _seed_dead_letter(self, repo_root: Path) -> str:
        """Create a dead-letter delivery record and return its message_id."""
        state = {
            "version": "1",
            "records": {
                "msg_dead": {
                    "message_id": "msg_dead",
                    "thread_id": "thread-1",
                    "from": "peer-a",
                    "to": "peer-b",
                    "subject": "test",
                    "status": "dead_letter",
                    "requires_ack": True,
                    "ack_timeout_seconds": 300,
                    "max_retries": 3,
                    "retry_count": 0,
                    "sent_at": "2026-03-01T00:00:00+00:00",
                    "ack_deadline": "2026-03-01T00:05:00+00:00",
                    "acks": [],
                    "last_error": "timeout",
                    "message": {
                        "id": "msg_dead",
                        "thread_id": "thread-1",
                        "from": "peer-a",
                        "to": "peer-b",
                        "subject": "test",
                        "body_md": "content",
                        "priority": "normal",
                        "attachments": [],
                    },
                }
            },
            "idempotency": {},
        }
        state_dir = repo_root / "messages" / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "delivery_index.json").write_text(
            json.dumps(state), encoding="utf-8"
        )
        return "msg_dead"

    # --- Gap 4: replay failure path ---

    def test_replay_oserror_propagates(self) -> None:
        """When append_jsonl_multi raises OSError in replay, it propagates."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub()
            msg_id = self._seed_dead_letter(repo_root)
            req = MessageReplayRequest(message_id=msg_id)

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.messages.service.append_jsonl_multi", side_effect=OSError("disk full")):
                    with self.assertRaises(OSError):
                        replay_messages(req=req, auth=_AuthStub())

    def test_replay_commit_paths_exception_degrades(self) -> None:
        """When commit_paths raises in replay, the service still returns."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _FailingCommitGitManagerStub()
            msg_id = self._seed_dead_letter(repo_root)
            req = MessageReplayRequest(message_id=msg_id)

            with patch("app.main._services", return_value=(settings, gm)):
                result = replay_messages(req=req, auth=_AuthStub())

            self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
