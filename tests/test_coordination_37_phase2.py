"""Tests for Issue #37 Phase 2 shared coordination update behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.main import coordination_shared_read, coordination_shared_update
from app.models import CoordinationSharedUpdateRequest
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that exposes optional admin scopes for shared update tests."""

    def __init__(
        self,
        *,
        peer_id: str,
        scopes: set[str] | None = None,
        deny_scope: str | None = None,
        deny_write_path: str | None = None,
    ) -> None:
        """Store caller identity plus optional scope and path denials."""
        super().__init__(peer_id=peer_id)
        self.scopes = scopes or set()
        self.deny_scope = deny_scope
        self.deny_write_path = deny_write_path

    def require(self, scope: str) -> None:
        """Raise when the test requests a denied scope."""
        if self.deny_scope == scope:
            raise HTTPException(status_code=403, detail=f"Missing scope: {scope}")

    def require_write_path(self, path: str) -> None:
        """Raise when the test requests a denied write namespace."""
        if self.deny_write_path == path:
            raise HTTPException(status_code=403, detail=f"Write path namespace not allowed: {path.split('/', 1)[0]}")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub with optional shared-update commit failure injection."""

    def __init__(self, *, fail_message_prefix: str | None = None) -> None:
        """Store optional failure injection criteria."""
        self.fail_message_prefix = fail_message_prefix

    def commit_file(self, _path: Path, message: str) -> bool:
        """Raise when the configured update commit should fail."""
        if self.fail_message_prefix and message.startswith(self.fail_message_prefix):
            raise RuntimeError("simulated commit failure")
        return True


class TestCoordination37Phase2(unittest.TestCase):
    """Validate owner-only shared updates, conflicts, and rollback behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for shared update tests."""
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

    def _write_capsule(self, repo_root: Path) -> str:
        """Persist one local capsule fixture used to prove non-mutation."""
        path = repo_root / "memory" / "continuity" / "task-shared-state-source.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "subject_kind": "task",
                    "subject_id": "shared-state-source",
                    "updated_at": now,
                    "verified_at": now,
                    "source": {"producer": "phase-2-test", "update_reason": "manual", "inputs": []},
                    "continuity": {
                        "top_priorities": ["keep private continuity stable"],
                        "active_concerns": ["none"],
                        "active_constraints": ["Do not weaken durability guarantees."],
                        "open_loops": ["finish shared-state update flow"],
                        "stance_summary": "Local continuity remains primary.",
                        "drift_signals": ["Awaiting owner update."],
                    },
                    "confidence": {"continuity": 0.9, "relationship_model": 0.0},
                }
            ),
            encoding="utf-8",
        )
        return path.read_text(encoding="utf-8")

    def _artifact_payload(self, **overrides: object) -> dict:
        """Return one valid stored shared coordination artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_shared_state",
            "schema_version": "1.0",
            "shared_id": "shared_0123456789abcdef0123456789abcdef",
            "created_at": now,
            "updated_at": now,
            "created_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta", "peer-gamma"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Retry slice coordination",
            "summary": "Shared constraints and drift signals.",
            "shared_state": {
                "constraints": ["Do not weaken durability guarantees."],
                "drift_signals": ["External review may invalidate timing assumptions."],
                "coordination_alerts": ["One participant reports missing context."],
            },
            "version": 1,
            "last_updated_by": "peer-alpha",
        }
        payload.update(overrides)
        return payload

    def _write_shared_artifact(self, repo_root: Path, payload: dict) -> Path:
        """Persist one raw shared coordination artifact fixture."""
        path = repo_root / "memory" / "coordination" / "shared" / f"{payload['shared_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")
        return path

    def _update_request(self, **overrides: object) -> CoordinationSharedUpdateRequest:
        """Return a valid owner-authored shared coordination update request."""
        payload = {
            "expected_version": 1,
            "title": "Retry slice coordination v2",
            "summary": "Updated shared constraints and alerts.",
            "constraints": ["Do not weaken durability guarantees.", "Do not bypass rollback safety."],
            "drift_signals": ["External review may invalidate timing assumptions."],
            "coordination_alerts": ["One participant reports missing context.", "Waiting on owner confirmation."],
        }
        payload.update(overrides)
        return CoordinationSharedUpdateRequest(**payload)

    def test_update_is_owner_only_and_does_not_mutate_local_continuity(self) -> None:
        """Update should modify only the shared artifact and leave local capsules untouched."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            capsule_before = self._write_capsule(repo_root)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as forbidden_cm:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(),
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(forbidden_cm.exception.status_code, 403)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_shared_update(
                    shared_id=artifact["shared_id"],
                    req=self._update_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertTrue(out["updated"])
            self.assertEqual(out["shared"]["version"], 2)
            self.assertEqual(out["shared"]["last_updated_by"], "peer-alpha")
            self.assertEqual(out["shared"]["participant_peers"], ["peer-beta", "peer-gamma"])
            self.assertEqual(out["shared"]["task_id"], "task-123")
            self.assertEqual(out["shared"]["thread_id"], "thread-abc")
            capsule_after = (repo_root / "memory" / "continuity" / "task-shared-state-source.json").read_text(encoding="utf-8")
            self.assertEqual(capsule_before, capsule_after)

    def test_update_requires_write_scope_and_memory_path_access(self) -> None:
        """Update should enforce both write:projects and memory path authorization."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as scope_cm:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(),
                        auth=_AuthStub(peer_id="peer-alpha", deny_scope="write:projects"),
                    )
            self.assertEqual(scope_cm.exception.status_code, 403)
            self.assertEqual(scope_cm.exception.detail, "Missing scope: write:projects")

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as path_cm:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(),
                        auth=_AuthStub(peer_id="peer-alpha", deny_write_path="memory/coordination/shared/x.json"),
                    )
            self.assertEqual(path_cm.exception.status_code, 403)
            self.assertEqual(path_cm.exception.detail, "Write path namespace not allowed: memory")

    def test_update_rejects_stale_expected_version(self) -> None:
        """Update should fail with a deterministic conflict on stale expected_version."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload(version=2)
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(expected_version=1),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 409)
            self.assertEqual(ctx.exception.detail, "Shared coordination version conflict")

    def test_update_rejects_invalid_shared_id_format(self) -> None:
        """Update should reject malformed shared ids before probing the filesystem."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_update(
                        shared_id="shared_foo",
                        req=self._update_request(),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Invalid shared coordination artifact id")

    def test_update_rejects_all_empty_shared_state(self) -> None:
        """Update should reject requests where all three shared-state arrays are empty."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(constraints=[], drift_signals=[], coordination_alerts=[]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Shared coordination state must include at least one shared item")

    def test_two_updates_against_one_version_allow_only_one_success(self) -> None:
        """Two owner updates against the same stored version should allow at most one success."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                first = coordination_shared_update(
                    shared_id=artifact["shared_id"],
                    req=self._update_request(expected_version=1, title="First update"),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )
                with self.assertRaises(HTTPException) as second_cm:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(expected_version=1, title="Second update"),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(first["shared"]["version"], 2)
            self.assertEqual(second_cm.exception.status_code, 409)
            self.assertEqual(second_cm.exception.detail, "Shared coordination version conflict")

    def test_read_observes_pre_or_post_update_state(self) -> None:
        """A read around an update should see either durable state without requiring stronger isolation."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                before = coordination_shared_read(shared_id=artifact["shared_id"], auth=_AuthStub(peer_id="peer-alpha"))
                coordination_shared_update(
                    shared_id=artifact["shared_id"],
                    req=self._update_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )
                after = coordination_shared_read(shared_id=artifact["shared_id"], auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(before["shared"]["version"], 1)
            self.assertEqual(after["shared"]["version"], 2)

    def test_update_rolls_back_prior_bytes_when_commit_fails(self) -> None:
        """Update should restore the previous artifact bytes if the commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            path = self._write_shared_artifact(repo_root, artifact)
            before = path.read_text(encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub(fail_message_prefix="coordination: update"))):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_update(
                        shared_id=artifact["shared_id"],
                        req=self._update_request(),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertEqual(ctx.exception.detail, "Failed to commit shared coordination update")
            self.assertEqual(path.read_text(encoding="utf-8"), before)

    def test_update_preserves_frozen_fields_and_replaces_shared_arrays_wholesale(self) -> None:
        """Update should change only mutable fields and replace bounded arrays as whole payloads."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._artifact_payload()
            self._write_shared_artifact(repo_root, artifact)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_shared_update(
                    shared_id=artifact["shared_id"],
                    req=self._update_request(
                        constraints=["Do not weaken durability guarantees."],
                        drift_signals=[],
                        coordination_alerts=["Waiting on owner confirmation."],
                    ),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertEqual(out["shared"]["created_by"], "peer-alpha")
            self.assertEqual(out["shared"]["owner_peer"], "peer-alpha")
            self.assertEqual(out["shared"]["participant_peers"], ["peer-beta", "peer-gamma"])
            self.assertEqual(out["shared"]["task_id"], "task-123")
            self.assertEqual(out["shared"]["thread_id"], "thread-abc")
            self.assertEqual(
                out["shared"]["shared_state"],
                {
                    "constraints": ["Do not weaken durability guarantees."],
                    "drift_signals": [],
                    "coordination_alerts": ["Waiting on owner confirmation."],
                },
            )

    def test_update_request_rejects_forbidden_participant_peers_field(self) -> None:
        """Update should reject participant membership mutation fields in 5B."""
        with self.assertRaises(ValidationError):
            CoordinationSharedUpdateRequest(
                expected_version=1,
                title="Retry slice coordination v2",
                summary="Updated shared constraints and alerts.",
                constraints=["Do not weaken durability guarantees."],
                drift_signals=[],
                coordination_alerts=["Waiting on owner confirmation."],
                participant_peers=["peer-beta"],
            )
