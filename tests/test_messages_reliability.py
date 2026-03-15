"""Tests for message delivery tracking, acknowledgements, and replay semantics."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import messages_ack, messages_pending, messages_send
from app.models import MessageAckRequest, MessageSendRequest


class _AuthStub:
    """Auth stub that permits the scopes used by messaging tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_write_path(self, _path: str) -> None:
        """Accept any requested write path for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None


class _GitManagerStub:
    """Git manager stub that pretends every file commit succeeds."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestMessageReliability(unittest.TestCase):
    """Validate message delivery tracking and acknowledgement behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build a settings object rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_idempotent_send_deduplicates_writes(self) -> None:
        """Sending with the same idempotency key should not duplicate persisted messages."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
                idempotency_key="k-1",
                delivery={"requires_ack": True, "ack_timeout_seconds": 120, "max_retries": 2},
            )

            with patch("app.main._services", return_value=(settings, gm)):
                first = messages_send(req=req, auth=_AuthStub())
                second = messages_send(req=req, auth=_AuthStub())

            self.assertFalse(first["idempotent_replay"])
            self.assertTrue(second["idempotent_replay"])

            inbox = repo_root / "messages" / "inbox" / "peer-b.jsonl"
            lines = inbox.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)

            state_path = repo_root / "messages" / "state" / "delivery_index.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(len(state["records"]), 1)
            self.assertEqual(len(state["idempotency"]), 1)

    def test_ack_updates_delivery_state_to_acked(self) -> None:
        """A successful ack should move the tracked delivery state to acked."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            send_req = MessageSendRequest(
                thread_id="thread-1",
                sender="peer-a",
                recipient="peer-b",
                subject="hello",
                body_md="content",
                idempotency_key="k-2",
                delivery={"requires_ack": True},
            )
            with patch("app.main._services", return_value=(settings, gm)):
                sent = messages_send(req=send_req, auth=_AuthStub())
                msg_id = sent["message"]["id"]
                acked = messages_ack(req=MessageAckRequest(message_id=msg_id, status="accepted"), auth=_AuthStub())

            self.assertEqual(acked["delivery_state"]["status"], "acked")
            pending = messages_pending(auth=_AuthStub())
            ids = [m["message_id"] for m in pending["messages"]]
            self.assertNotIn(msg_id, ids)

    def test_pending_reports_overdue_dead_letter(self) -> None:
        """Pending listing should surface overdue tracked messages as dead letters."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "messages" / "state").mkdir(parents=True, exist_ok=True)
            state_path = repo_root / "messages" / "state" / "delivery_index.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": "1",
                        "records": {
                            "msg_old": {
                                "message_id": "msg_old",
                                "from": "peer-a",
                                "to": "peer-b",
                                "status": "pending_ack",
                                "sent_at": "2026-02-25T00:00:00+00:00",
                                "ack_deadline": "2026-02-25T00:01:00+00:00",
                                "acks": [],
                            }
                        },
                        "idempotency": {},
                    }
                ),
                encoding="utf-8",
            )

            gm = _GitManagerStub()
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, gm)):
                pending = messages_pending(include_terminal=True, auth=_AuthStub())

            self.assertEqual(pending["count"], 1)
            self.assertEqual(pending["messages"][0]["effective_status"], "dead_letter")


if __name__ == "__main__":
    unittest.main()
