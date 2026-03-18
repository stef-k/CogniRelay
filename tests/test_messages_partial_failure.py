"""Tests for partial-failure handling in message service append operations."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import messages_send
from app.models import MessageSendRequest


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


class TestMessagesPartialFailure(unittest.TestCase):
    """Verify that OSError during multi-file append propagates and leaves no partial state."""

    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_send_oserror_propagates_no_partial_state(self) -> None:
        """When append_jsonl_multi raises OSError, no JSONL files are left behind."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
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
                self.assertFalse(
                    p.exists(),
                    f"Expected {p.name} to not exist after OSError",
                )


if __name__ == "__main__":
    unittest.main()
