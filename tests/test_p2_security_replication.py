"""Tests for phase-2 security, messaging, metrics, and replication behavior."""

import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from app.config import Settings
from app.main import (
    messages_send,
    relay_forward,
    messages_verify,
    metrics,
    replay_messages,
    replication_pull,
    replication_push,
    security_keys_rotate,
)
from app.models import (
    MessageSendRequest,
    RelayForwardRequest,
    MessageReplayRequest,
    MessageVerifyRequest,
    ReplicationFilePayload,
    ReplicationPullRequest,
    ReplicationPushRequest,
    SecurityKeysRotateRequest,
)


class _AuthStub:
    """Auth stub that permits the scopes used by security and replication tests."""

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

    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful single-file commit without touching git."""
        return True

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        """Report a successful multi-path commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class _FailingCommitPathsGitManagerStub(_GitManagerStub):
    """Git manager stub that fails grouped commits."""

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        raise OSError("git commit failed")


class _FailingCommitFileGitManagerStub(_GitManagerStub):
    """Git manager stub that fails single-file commits."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        raise OSError("git commit failed")


class _FakeHTTPResponse:
    """Minimal HTTP response stub for outbound replication calls."""

    def __init__(self, payload: dict):
        """Serialize the provided JSON payload into the fake response body."""
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        """Return the serialized response payload."""
        return self._raw

    def __enter__(self):
        """Support use as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Propagate exceptions raised inside the response context."""
        return False


class TestP2SecurityReplication(unittest.TestCase):
    """Validate security, replay, metrics, and replication integrations."""

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

    def test_keys_rotate_and_verify_with_nonce_replay_guard(self) -> None:
        """Signature verification should reject nonce replay after first success."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                signing_secret = "secret-a"
                rotated = security_keys_rotate(
                    req=SecurityKeysRotateRequest(key_id="key-a", secret=signing_secret),
                    auth=_AuthStub(),
                )
                self.assertNotIn("secret", rotated["key"])

                payload = {"thread_id": "thread-1", "body_md": "hello"}
                canonical = json.dumps(
                    {"payload": payload, "key_id": "key-a", "nonce": "nonce-1", "expires_at": None},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                sig = hmac.new(signing_secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()

                first = messages_verify(
                    req=MessageVerifyRequest(
                        payload=payload,
                        key_id="key-a",
                        nonce="nonce-1",
                        signature=sig,
                        consume_nonce=True,
                    ),
                    auth=_AuthStub(),
                )
                second = messages_verify(
                    req=MessageVerifyRequest(
                        payload=payload,
                        key_id="key-a",
                        nonce="nonce-1",
                        signature=sig,
                        consume_nonce=True,
                    ),
                    auth=_AuthStub(),
                )

            self.assertTrue(first["valid"])
            self.assertTrue(first["nonce_consumed"])
            self.assertFalse(second["valid"])
            self.assertEqual(second["reason"], "replay_detected")
    def test_keys_rotate_can_return_secret_when_explicitly_requested(self) -> None:
        """Key rotation may return the secret only when explicitly requested."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = security_keys_rotate(
                    req=SecurityKeysRotateRequest(key_id="key-b", secret="secret-b", return_secret=True),
                    auth=_AuthStub(),
                )

            self.assertEqual(out["key"]["key_id"], "key-b")
            self.assertEqual(out["key"]["secret"], "secret-b")

    def test_strict_signed_ingress_for_send_and_relay(self) -> None:
        """Strict signed-ingress mode should require valid signed envelopes."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = Settings(
                repo_root=repo_root,
                auto_init_git=False,
                git_author_name="n/a",
                git_author_email="n/a",
                tokens={},
                audit_log_enabled=False,
                require_signed_ingress=True,
            )
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                security_keys_rotate(
                    req=SecurityKeysRotateRequest(key_id="key-strict", secret="secret-strict"),
                    auth=_AuthStub(),
                )

                with self.assertRaises(HTTPException) as send_err:
                    messages_send(
                        req=MessageSendRequest(
                            thread_id="thread-1",
                            sender="peer-a",
                            recipient="peer-b",
                            subject="hello",
                            body_md="content",
                        ),
                        auth=_AuthStub(),
                    )

                send_payload = {
                    "thread_id": "thread-1",
                    "sender": "peer-a",
                    "recipient": "peer-b",
                    "subject": "hello",
                    "body_md": "content",
                    "priority": "normal",
                    "attachments": [],
                    "idempotency_key": None,
                    "delivery": {"requires_ack": False, "ack_timeout_seconds": 300, "max_retries": 5},
                }
                send_canonical = json.dumps(
                    {"payload": send_payload, "key_id": "key-strict", "nonce": "nonce-send", "expires_at": None},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                send_sig = hmac.new("secret-strict".encode("utf-8"), send_canonical, hashlib.sha256).hexdigest()
                send_ok = messages_send(
                    req=MessageSendRequest(
                        thread_id="thread-1",
                        sender="peer-a",
                        recipient="peer-b",
                        subject="hello",
                        body_md="content",
                        signed_envelope={
                            "key_id": "key-strict",
                            "nonce": "nonce-send",
                            "signature": send_sig,
                            "algorithm": "hmac-sha256",
                            "consume_nonce": True,
                        },
                    ),
                    auth=_AuthStub(),
                )

                with self.assertRaises(HTTPException) as relay_err:
                    relay_forward(
                        req=RelayForwardRequest(
                            relay_id="relay-a",
                            target_recipient="peer-b",
                            thread_id="thread-1",
                            sender="peer-a",
                            subject="hello",
                            body_md="relay-content",
                        ),
                        auth=_AuthStub(),
                    )

                relay_payload = {
                    "relay_id": "relay-a",
                    "target_recipient": "peer-b",
                    "thread_id": "thread-1",
                    "sender": "peer-a",
                    "subject": "hello",
                    "body_md": "relay-content",
                    "priority": "normal",
                    "attachments": [],
                    "envelope": {},
                }
                relay_canonical = json.dumps(
                    {"payload": relay_payload, "key_id": "key-strict", "nonce": "nonce-relay", "expires_at": None},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                relay_sig = hmac.new("secret-strict".encode("utf-8"), relay_canonical, hashlib.sha256).hexdigest()
                relay_ok = relay_forward(
                    req=RelayForwardRequest(
                        relay_id="relay-a",
                        target_recipient="peer-b",
                        thread_id="thread-1",
                        sender="peer-a",
                        subject="hello",
                        body_md="relay-content",
                        signed_envelope={
                            "key_id": "key-strict",
                            "nonce": "nonce-relay",
                            "signature": relay_sig,
                            "algorithm": "hmac-sha256",
                            "consume_nonce": True,
                        },
                    ),
                    auth=_AuthStub(),
                )

            self.assertEqual(send_err.exception.status_code, 400)
            self.assertEqual(relay_err.exception.status_code, 400)
            self.assertTrue(send_ok["ok"])
            self.assertTrue(send_ok["signature_verification"]["valid"])
            self.assertTrue(relay_ok["ok"])
            self.assertTrue(relay_ok["signature_verification"]["valid"])

    def test_replay_messages_from_dead_letter(self) -> None:
        """Dead-letter messages should replay into a new tracked delivery record."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            delivery_state = repo_root / "messages" / "state" / "delivery_index.json"
            delivery_state.parent.mkdir(parents=True, exist_ok=True)
            delivery_state.write_text(
                json.dumps(
                    {
                        "version": "1",
                        "records": {
                            "msg_dead": {
                                "message_id": "msg_dead",
                                "thread_id": "thread-1",
                                "from": "peer-a",
                                "to": "peer-b",
                                "subject": "x",
                                "status": "dead_letter",
                                "requires_ack": True,
                                "ack_timeout_seconds": 300,
                                "max_retries": 5,
                                "retry_count": 1,
                                "acks": [],
                                "message": {
                                    "id": "msg_dead",
                                    "thread_id": "thread-1",
                                    "from": "peer-a",
                                    "to": "peer-b",
                                    "subject": "x",
                                    "body_md": "payload",
                                    "attachments": [],
                                    "priority": "normal",
                                    "delivery": {"requires_ack": True, "ack_timeout_seconds": 300, "max_retries": 5},
                                },
                            }
                        },
                        "idempotency": {},
                    }
                ),
                encoding="utf-8",
            )

            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                out = replay_messages(
                    req=MessageReplayRequest(message_id="msg_dead", reason="retry"),
                    auth=_AuthStub(),
                )

            new_id = out["replayed_message_id"]
            state = json.loads(delivery_state.read_text(encoding="utf-8"))
            self.assertTrue(out["ok"])
            self.assertIn(new_id, state["records"])
            self.assertEqual(state["records"]["msg_dead"]["status"], "replayed")
            self.assertEqual(state["records"][new_id]["status"], "pending_ack")
            self.assertTrue((repo_root / "messages" / "inbox" / "peer-b.jsonl").exists())

    def test_metrics_aggregation(self) -> None:
        """Metrics endpoint should aggregate delivery, audit, check, and replication data."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "messages" / "state").mkdir(parents=True, exist_ok=True)
            (repo_root / "logs").mkdir(parents=True, exist_ok=True)
            (repo_root / "runs" / "checks").mkdir(parents=True, exist_ok=True)
            (repo_root / "peers").mkdir(parents=True, exist_ok=True)

            (repo_root / "messages" / "state" / "delivery_index.json").write_text(
                json.dumps(
                    {
                        "version": "1",
                        "records": {
                            "m1": {"message_id": "m1", "to": "peer-b", "status": "acked", "acks": []},
                            "m2": {"message_id": "m2", "to": "peer-b", "status": "dead_letter", "acks": []},
                            "m3": {"message_id": "m3", "to": "peer-c", "status": "pending_ack", "acks": []},
                        },
                        "idempotency": {},
                    }
                ),
                encoding="utf-8",
            )
            (repo_root / "logs" / "api_audit.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"event": "messages_send", "peer_id": "peer-a", "detail": {}}),
                        json.dumps({"event": "messages_ack", "peer_id": "peer-b", "detail": {}}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (repo_root / "runs" / "checks" / "run-1.json").write_text(
                json.dumps({"profile": "test", "status": "passed"}), encoding="utf-8"
            )
            (repo_root / "peers" / "replication_state.json").write_text(
                json.dumps({"schema_version": "1.0", "last_pull_by_source": {"peer-z": {"received_count": 1}}, "last_push": None}),
                encoding="utf-8",
            )

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = metrics(auth=_AuthStub())

            self.assertTrue(out["ok"])
            self.assertEqual(out["delivery"]["backlog_depth"], 1)
            self.assertEqual(out["delivery"]["summary"]["acked"], 1)
            self.assertEqual(out["delivery"]["summary"]["dead_letter"], 1)
            self.assertIn("messages_send", out["audit"]["event_counts"])
            self.assertIn("test:passed", out["checks"]["summary"])

    def test_replication_pull_and_push(self) -> None:
        """Replication pull and push should persist state and honor dry-run behavior."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            content = "# identity\n"
            file_row = ReplicationFilePayload(path="memory/core/identity.md", content=content, sha256=hashlib.sha256(content.encode("utf-8")).hexdigest())
            with patch("app.main._services", return_value=(settings, gm)):
                pulled = replication_pull(
                    req=ReplicationPullRequest(source_peer="peer-remote", files=[file_row]),
                    auth=_AuthStub(),
                )
                pulled_again = replication_pull(
                    req=ReplicationPullRequest(source_peer="peer-remote", files=[file_row]),
                    auth=_AuthStub(),
                )

            self.assertEqual(pulled["changed_count"], 1)
            self.assertEqual(pulled_again["changed_count"], 0)
            self.assertEqual(pulled_again["skipped_count"], 1)
            self.assertTrue((repo_root / "memory" / "core" / "identity.md").exists())

            (repo_root / "messages" / "threads").mkdir(parents=True, exist_ok=True)
            (repo_root / "messages" / "threads" / "t1.jsonl").write_text('{"x":1}\n', encoding="utf-8")
            with patch("app.main._services", return_value=(settings, gm)):
                dry = replication_push(
                    req=ReplicationPushRequest(dry_run=True, include_prefixes=["memory", "messages"], max_files=100),
                    auth=_AuthStub(),
                )
            self.assertTrue(dry["dry_run"])
            self.assertGreaterEqual(dry["file_count"], 2)

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.maintenance.service.urlopen", return_value=_FakeHTTPResponse({"ok": True, "accepted": True})):
                    pushed = replication_push(
                        req=ReplicationPushRequest(
                            dry_run=False,
                            base_url="https://peer-remote.example.net",
                            include_prefixes=["memory"],
                            target_token="tok",
                        ),
                        auth=_AuthStub(),
                    )
            self.assertTrue(pushed["ok"])
            self.assertFalse(pushed["dry_run"])
            self.assertTrue(pushed["remote"]["ok"])

    def test_replication_pull_rolls_back_on_commit_failure(self) -> None:
        """Replication pull should restore files when the grouped durability step fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _FailingCommitPathsGitManagerStub(repo_root)

            content = "# identity\n"
            file_row = ReplicationFilePayload(path="memory/core/identity.md", content=content, sha256=hashlib.sha256(content.encode("utf-8")).hexdigest())
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException):
                    replication_pull(
                        req=ReplicationPullRequest(source_peer="peer-remote", files=[file_row]),
                        auth=_AuthStub(),
                    )

            self.assertFalse((repo_root / "memory" / "core" / "identity.md").exists())
            self.assertFalse((repo_root / "peers" / "replication_state.json").exists())
            self.assertFalse((repo_root / "peers" / "replication_tombstones.json").exists())

    def test_replication_push_warns_when_state_not_durable(self) -> None:
        """Replication push should degrade safely after remote success if local state commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _FailingCommitFileGitManagerStub(repo_root)

            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")

            with patch("app.main._services", return_value=(settings, gm)):
                with patch("app.maintenance.service.urlopen", return_value=_FakeHTTPResponse({"ok": True, "accepted": True})):
                    pushed = replication_push(
                        req=ReplicationPushRequest(
                            dry_run=False,
                            base_url="https://peer-remote.example.net",
                            include_prefixes=["memory"],
                            target_token="tok",
                        ),
                        auth=_AuthStub(),
                    )

            self.assertTrue(pushed["ok"])
            self.assertEqual(pushed["committed_files"], [])
            self.assertTrue(any("replication_push_not_durable" in warning for warning in pushed["warnings"]))

    def test_replication_push_resolves_target_base_from_peer_registry(self) -> None:
        """Replication push should resolve the target base URL from peer registry metadata."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")

            with patch("app.main._services", return_value=(settings, gm)):
                from app.main import peers_register
                from app.models import PeerRegisterRequest

                peers_register(
                    req=PeerRegisterRequest(peer_id="peer-beta", base_url="https://peer-beta.example.net"),
                    auth=_AuthStub(),
                )
                with patch("app.maintenance.service.urlopen", return_value=_FakeHTTPResponse({"ok": True, "accepted": True})):
                    pushed = replication_push(
                        req=ReplicationPushRequest(peer_id="peer-beta", include_prefixes=["memory"], target_token="tok"),
                        auth=_AuthStub(),
                    )

            self.assertTrue(pushed["ok"])
            self.assertEqual(pushed["target_url"], "https://peer-beta.example.net/v1/replication/pull")


if __name__ == "__main__":
    unittest.main()
