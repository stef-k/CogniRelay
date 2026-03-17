"""Tests for Issue #36 Phase 2 handoff consume behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.main import coordination_handoff_consume, coordination_handoff_read
from app.models import CoordinationHandoffConsumeRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that exposes scopes for admin visibility when needed."""

    def __init__(self, *, peer_id: str, scopes: set[str] | None = None) -> None:
        """Store the caller identity and optional scope set."""
        super().__init__(peer_id=peer_id)
        self.scopes = scopes or set()


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub with optional consume-commit failure injection."""

    def __init__(self, *, fail_message_prefix: str | None = None) -> None:
        """Store optional failure injection criteria."""
        self.fail_message_prefix = fail_message_prefix

    def commit_file(self, _path: Path, message: str) -> bool:
        """Raise when the configured consume commit should fail."""
        if self.fail_message_prefix and message.startswith(self.fail_message_prefix):
            raise RuntimeError("simulated commit failure")
        return True


class TestCoordination36Phase2(unittest.TestCase):
    """Validate recipient-only consume semantics and rollback behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for consume tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _now(self) -> str:
        """Return a stable UTC timestamp string for persisted fixtures."""
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _write_peer_registry(self, repo_root: Path, peer_id: str, *, trust_level: str) -> None:
        """Persist one peer registry row for trust-transition tests."""
        path = repo_root / "peers" / "registry.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "updated_at": now,
                    "peers": {
                        peer_id: {
                            "base_url": f"https://{peer_id}.example.net",
                            "public_key": None,
                            "public_key_fingerprint": None,
                            "capabilities_url": "/v1/manifest",
                            "trust_level": trust_level,
                            "allowed_scopes": [],
                            "created_at": now,
                            "updated_at": now,
                            "trust_history": [],
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

    def _write_capsule(self, repo_root: Path) -> None:
        """Persist one local capsule used to prove consume does not mutate it."""
        path = repo_root / "memory" / "continuity" / "task-build-phase-5a.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "subject_kind": "task",
                    "subject_id": "build-phase-5a",
                    "updated_at": now,
                    "verified_at": now,
                    "source": {"producer": "handoff-hook", "update_reason": "manual", "inputs": []},
                    "continuity": {
                        "top_priorities": ["finish phase 5A"],
                        "active_concerns": ["none"],
                        "active_constraints": ["Do not weaken durability guarantees."],
                        "open_loops": ["coordinate consume semantics"],
                        "stance_summary": "Local continuity remains primary.",
                        "drift_signals": ["Pending review."],
                    },
                    "confidence": {"continuity": 0.9, "relationship_model": 0.0},
                }
            ),
            encoding="utf-8",
        )

    def _write_handoff(self, repo_root: Path, *, recipient_status: str = "pending", recipient_reason: str | None = None) -> str:
        """Persist one handoff artifact fixture and return its id."""
        handoff_id = "handoff_1234567890abcdef1234567890abcdef"
        path = repo_root / "memory" / "coordination" / "handoffs" / f"{handoff_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        path.write_text(
            json.dumps(
                {
                    "schema_type": "continuity_handoff",
                    "schema_version": "1.0",
                    "handoff_id": handoff_id,
                    "created_at": now,
                    "created_by": "peer-alpha",
                    "sender_peer": "peer-alpha",
                    "recipient_peer": "peer-beta",
                    "source_selector": {"subject_kind": "task", "subject_id": "build-phase-5a"},
                    "source_summary": {
                        "path": "memory/continuity/task-build-phase-5a.json",
                        "updated_at": now,
                        "verified_at": now,
                        "verification_status": "peer_confirmed",
                        "health_status": "healthy",
                    },
                    "task_id": "task-123",
                    "thread_id": "thread-abc",
                    "note": "Use as advisory input only.",
                    "shared_continuity": {
                        "active_constraints": ["Do not weaken durability guarantees."],
                        "drift_signals": ["Pending review."],
                    },
                    "recipient_status": recipient_status,
                    "recipient_reason": recipient_reason,
                    "consumed_at": None,
                    "consumed_by": None,
                }
            ),
            encoding="utf-8",
        )
        return handoff_id

    def test_consume_is_recipient_only_and_does_not_mutate_local_continuity(self) -> None:
        """Consume should update only handoff recipient fields and leave local capsules untouched."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_capsule(repo_root)
            handoff_id = self._write_handoff(repo_root)
            before_capsule = (repo_root / "memory" / "continuity" / "task-build-phase-5a.json").read_text(encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as forbidden_cm:
                    coordination_handoff_consume(
                        handoff_id=handoff_id,
                        req=CoordinationHandoffConsumeRequest(status="accepted_advisory", reason="looks good"),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(forbidden_cm.exception.status_code, 403)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_handoff_consume(
                    handoff_id=handoff_id,
                    req=CoordinationHandoffConsumeRequest(status="accepted_advisory", reason="looks good"),
                    auth=_AuthStub(peer_id="peer-beta"),
                )

            self.assertTrue(out["updated"])
            self.assertEqual(out["handoff"]["recipient_status"], "accepted_advisory")
            self.assertEqual(out["handoff"]["recipient_reason"], "looks good")
            self.assertEqual(out["handoff"]["consumed_by"], "peer-beta")
            self.assertIsNotNone(out["handoff"]["consumed_at"])
            after_capsule = (repo_root / "memory" / "continuity" / "task-build-phase-5a.json").read_text(encoding="utf-8")
            self.assertEqual(before_capsule, after_capsule)

    def test_consume_same_status_and_reason_is_noop(self) -> None:
        """Same-status same-reason replay should be an idempotent no-op."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            handoff_id = self._write_handoff(repo_root, recipient_status="accepted_advisory", recipient_reason="looks good")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_handoff_consume(
                    handoff_id=handoff_id,
                    req=CoordinationHandoffConsumeRequest(status="accepted_advisory", reason="looks good"),
                    auth=_AuthStub(peer_id="peer-beta"),
                )

            self.assertFalse(out["updated"])
            self.assertEqual(out["latest_commit"], "test-sha")

    def test_consume_conflicts_on_reason_or_status_changes_after_first_write(self) -> None:
        """Later consume attempts should fail once the handoff leaves pending."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            handoff_id = self._write_handoff(repo_root, recipient_status="deferred", recipient_reason="busy")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as reason_cm:
                    coordination_handoff_consume(
                        handoff_id=handoff_id,
                        req=CoordinationHandoffConsumeRequest(status="deferred", reason="waiting on info"),
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(reason_cm.exception.status_code, 409)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as status_cm:
                    coordination_handoff_consume(
                        handoff_id=handoff_id,
                        req=CoordinationHandoffConsumeRequest(status="rejected", reason="no longer relevant"),
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(status_cm.exception.status_code, 409)

    def test_consume_rolls_back_prior_bytes_when_commit_fails(self) -> None:
        """Consume should restore the previous artifact bytes if the commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            handoff_id = self._write_handoff(repo_root)
            path = repo_root / "memory" / "coordination" / "handoffs" / f"{handoff_id}.json"
            before = path.read_text(encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub(fail_message_prefix="handoff: consume"))):
                with self.assertRaises(HTTPException) as cm:
                    coordination_handoff_consume(
                        handoff_id=handoff_id,
                        req=CoordinationHandoffConsumeRequest(status="deferred", reason="busy"),
                        auth=_AuthStub(peer_id="peer-beta"),
                    )

            self.assertEqual(cm.exception.status_code, 500)
            self.assertEqual(cm.exception.detail, "Failed to commit handoff consume update")
            self.assertEqual(path.read_text(encoding="utf-8"), before)

    def test_existing_handoffs_remain_readable_and_consumable_after_trust_demotion(self) -> None:
        """Trust demotion after creation should not revoke artifact-based visibility or consume rights."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            handoff_id = self._write_handoff(repo_root)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="untrusted")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                read_out = coordination_handoff_read(handoff_id=handoff_id, auth=_AuthStub(peer_id="peer-beta"))
                consume_out = coordination_handoff_consume(
                    handoff_id=handoff_id,
                    req=CoordinationHandoffConsumeRequest(status="deferred", reason="later"),
                    auth=_AuthStub(peer_id="peer-beta"),
                )

            self.assertEqual(read_out["handoff"]["handoff_id"], handoff_id)
            self.assertEqual(consume_out["handoff"]["recipient_status"], "deferred")


if __name__ == "__main__":
    unittest.main()
