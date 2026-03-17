"""Tests for Issue #36 Phase 1 handoff create/read/query behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.main import coordination_handoff_create, coordination_handoff_read, coordination_handoffs_query
from app.models import CoordinationHandoffCreateRequest
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with optional scope and path denial for coordination tests."""

    def __init__(
        self,
        *,
        peer_id: str,
        deny_scope: str | None = None,
        deny_write_path: str | None = None,
        deny_read_path: str | None = None,
        scopes: set[str] | None = None,
    ) -> None:
        """Store identity plus optional denial knobs for one test caller."""
        super().__init__(peer_id=peer_id)
        self.deny_scope = deny_scope
        self.deny_write_path = deny_write_path
        self.deny_read_path = deny_read_path
        self.scopes = scopes or set()

    def require(self, scope: str) -> None:
        """Raise when the test requests a denied scope."""
        if self.deny_scope == scope:
            raise HTTPException(status_code=403, detail=f"Missing scope: {scope}")

    def require_write_path(self, path: str) -> None:
        """Raise when the test requests a denied write namespace."""
        if self.deny_write_path == path:
            raise HTTPException(status_code=403, detail=f"Write path namespace not allowed: {path.split('/', 1)[0]}")

    def require_read_path(self, path: str) -> None:
        """Raise when the test requests a denied read namespace."""
        if self.deny_read_path == path:
            raise HTTPException(status_code=403, detail=f"Read path namespace not allowed: {path.split('/', 1)[0]}")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records commit requests for coordination tests."""

    def __init__(self, *, fail_message_prefix: str | None = None) -> None:
        """Store optional failure injection criteria."""
        self.fail_message_prefix = fail_message_prefix
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record one commit or raise to simulate a git failure."""
        self.commits.append((str(path), message))
        if self.fail_message_prefix and message.startswith(self.fail_message_prefix):
            raise RuntimeError("simulated commit failure")
        return True


class TestCoordination36Phase1(unittest.TestCase):
    """Validate handoff artifact creation, visibility, and query rules."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for coordination tests."""
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

    def _write_peer_registry(self, repo_root: Path, peer_id: str, *, trust_level: str = "restricted") -> None:
        """Persist one peer registry row for coordination tests."""
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

    def _write_capsule(
        self,
        repo_root: Path,
        *,
        subject_kind: str = "task",
        subject_id: str = "build-phase-5a",
        active_constraints: list[str] | None = None,
        drift_signals: list[str] | None = None,
    ) -> None:
        """Persist one active continuity capsule that can be projected into a handoff."""
        now = self._now()
        path = repo_root / "memory" / "continuity" / f"{subject_kind}-{subject_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "subject_kind": subject_kind,
                    "subject_id": subject_id,
                    "updated_at": now,
                    "verified_at": now,
                    "verification_kind": "peer_confirmation",
                    "source": {
                        "producer": "handoff-hook",
                        "update_reason": "manual",
                        "inputs": ["memory/core/identity.md"],
                    },
                    "continuity": {
                        "top_priorities": ["finish phase 5A"],
                        "active_concerns": ["do not break local-first semantics"],
                        "active_constraints": active_constraints if active_constraints is not None else ["Do not weaken durability guarantees."],
                        "open_loops": ["coordinate the retry slice"],
                        "stance_summary": "Keep inter-agent sharing bounded and advisory.",
                        "drift_signals": drift_signals if drift_signals is not None else ["Pending external review may change timing assumptions."],
                    },
                    "confidence": {"continuity": 0.9, "relationship_model": 0.0},
                    "verification_state": {
                        "status": "peer_confirmed",
                        "last_revalidated_at": now,
                        "strongest_signal": "peer_confirmation",
                        "evidence_refs": ["messages/thread-123"],
                    },
                    "capsule_health": {
                        "status": "healthy",
                        "reasons": [],
                        "last_checked_at": now,
                    },
                }
            ),
            encoding="utf-8",
        )

    def _write_handoff_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw handoff artifact fixture."""
        path = repo_root / "memory" / "coordination" / "handoffs" / f"{payload['handoff_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _create_request(self, **overrides: object) -> CoordinationHandoffCreateRequest:
        """Return a valid create request with optional overrides."""
        payload = {
            "recipient_peer": "peer-beta",
            "subject_kind": "task",
            "subject_id": "build-phase-5a",
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "note": "Coordinate constraints before resuming the retry slice.",
        }
        payload.update(overrides)
        return CoordinationHandoffCreateRequest(**payload)

    def test_create_persists_handoff_projection_and_source_summary(self) -> None:
        """Create should persist only the shareable subset plus exact source-summary mappings."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            self._write_capsule(repo_root)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_handoff_create(req=self._create_request(), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertTrue(out["ok"])
            self.assertTrue(out["created"])
            self.assertEqual(out["path"], f"memory/coordination/handoffs/{out['handoff']['handoff_id']}.json")
            self.assertRegex(out["handoff"]["handoff_id"], r"^handoff_[0-9a-f]{32}$")
            self.assertEqual(out["handoff"]["sender_peer"], "peer-alpha")
            self.assertEqual(out["handoff"]["created_by"], "peer-alpha")
            self.assertEqual(
                out["handoff"]["shared_continuity"],
                {
                    "active_constraints": ["Do not weaken durability guarantees."],
                    "drift_signals": ["Pending external review may change timing assumptions."],
                },
            )
            self.assertNotIn("top_priorities", out["handoff"]["shared_continuity"])
            self.assertEqual(out["handoff"]["source_summary"]["path"], "memory/continuity/task-build-phase-5a.json")
            self.assertEqual(out["handoff"]["source_summary"]["verification_status"], "peer_confirmed")
            self.assertEqual(out["handoff"]["source_summary"]["health_status"], "healthy")

            stored_path = repo_root / out["path"]
            stored_text = stored_path.read_text(encoding="utf-8")
            self.assertEqual(stored_text, canonical_json(json.loads(stored_text)))

    def test_create_projects_empty_arrays_when_source_fields_are_empty(self) -> None:
        """Empty source arrays should project as empty arrays rather than failing create."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta")
            self._write_capsule(repo_root, active_constraints=[], drift_signals=[])
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_handoff_create(req=self._create_request(note=""), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(out["handoff"]["shared_continuity"]["active_constraints"], [])
            self.assertEqual(out["handoff"]["shared_continuity"]["drift_signals"], [])

    def test_create_rejects_unknown_and_untrusted_recipients(self) -> None:
        """Unknown and untrusted recipients should fail with the spec status codes."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_capsule(repo_root)
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as missing_cm:
                    coordination_handoff_create(req=self._create_request(), auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(missing_cm.exception.status_code, 404)

            self._write_peer_registry(repo_root, "peer-beta", trust_level="untrusted")
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as untrusted_cm:
                    coordination_handoff_create(req=self._create_request(), auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(untrusted_cm.exception.status_code, 409)

    def test_create_rejects_overlong_commit_message_with_service_level_400(self) -> None:
        """Create should reject overlong custom commit messages through the service-layer contract."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            self._write_capsule(repo_root)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_handoff_create(
                        req=self._create_request(commit_message="x" * 121),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Value too long in coordination_handoff.commit_message")

    def test_create_requires_write_scope_and_memory_path_access(self) -> None:
        """Create should enforce both write:projects and memory path authorization."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta")
            self._write_capsule(repo_root)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as scope_cm:
                    coordination_handoff_create(
                        req=self._create_request(),
                        auth=_AuthStub(peer_id="peer-alpha", deny_scope="write:projects"),
                    )
            self.assertEqual(scope_cm.exception.status_code, 403)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as path_cm:
                    coordination_handoff_create(
                        req=self._create_request(),
                        auth=_AuthStub(peer_id="peer-alpha", deny_write_path="memory/coordination/handoffs/x.json"),
                    )
            self.assertEqual(path_cm.exception.status_code, 403)

    def test_read_enforces_sender_recipient_or_admin_visibility(self) -> None:
        """Read should allow sender, recipient, and admin callers but reject unrelated peers."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = {
                "schema_type": "continuity_handoff",
                "schema_version": "1.0",
                "handoff_id": "handoff_1234567890abcdef1234567890abcdef",
                "created_at": self._now(),
                "created_by": "peer-alpha",
                "sender_peer": "peer-alpha",
                "recipient_peer": "peer-beta",
                "source_selector": {"subject_kind": "task", "subject_id": "build-phase-5a"},
                "source_summary": {
                    "path": "memory/continuity/task/build-phase-5a.json",
                    "updated_at": self._now(),
                    "verified_at": self._now(),
                    "verification_status": "peer_confirmed",
                    "health_status": "healthy",
                },
                "task_id": None,
                "thread_id": None,
                "note": None,
                "shared_continuity": {"active_constraints": [], "drift_signals": []},
                "recipient_status": "pending",
                "recipient_reason": None,
                "consumed_at": None,
                "consumed_by": None,
            }
            self._write_handoff_artifact(repo_root, payload)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                sender = coordination_handoff_read(handoff_id=payload["handoff_id"], auth=_AuthStub(peer_id="peer-alpha"))
                recipient = coordination_handoff_read(handoff_id=payload["handoff_id"], auth=_AuthStub(peer_id="peer-beta"))
                admin = coordination_handoff_read(
                    handoff_id=payload["handoff_id"],
                    auth=_AuthStub(peer_id="peer-gamma", scopes={"admin:peers"}),
                )

            self.assertEqual(sender["handoff"]["handoff_id"], payload["handoff_id"])
            self.assertEqual(recipient["handoff"]["handoff_id"], payload["handoff_id"])
            self.assertEqual(admin["handoff"]["handoff_id"], payload["handoff_id"])

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    coordination_handoff_read(handoff_id=payload["handoff_id"], auth=_AuthStub(peer_id="peer-other"))
            self.assertEqual(cm.exception.status_code, 403)

    def test_read_returns_400_for_invalid_stored_artifact(self) -> None:
        """Read should surface structurally invalid stored artifacts as HTTP 400."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            path = repo_root / "memory" / "coordination" / "handoffs" / "handoff_bad.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{not-json", encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    coordination_handoff_read(handoff_id="handoff_bad", auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(cm.exception.status_code, 400)

    def test_query_discovers_visible_handoffs_sorts_counts_and_applies_offset(self) -> None:
        """Query should discover visible handoffs, sort deterministically, and paginate after filtering."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            base = {
                "schema_type": "continuity_handoff",
                "schema_version": "1.0",
                "created_by": "peer-alpha",
                "sender_peer": "peer-alpha",
                "recipient_peer": "peer-beta",
                "source_selector": {"subject_kind": "task", "subject_id": "build-phase-5a"},
                "source_summary": {
                    "path": "memory/continuity/task/build-phase-5a.json",
                    "updated_at": "2026-03-17T10:00:00Z",
                    "verified_at": "2026-03-17T10:00:00Z",
                    "verification_status": "peer_confirmed",
                    "health_status": "healthy",
                },
                "task_id": None,
                "thread_id": None,
                "note": None,
                "shared_continuity": {"active_constraints": [], "drift_signals": []},
                "recipient_reason": None,
                "consumed_at": None,
                "consumed_by": None,
            }
            self._write_handoff_artifact(
                repo_root,
                {
                    **base,
                    "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "created_at": "2026-03-17T12:00:00Z",
                    "recipient_status": "pending",
                },
            )
            self._write_handoff_artifact(
                repo_root,
                {
                    **base,
                    "handoff_id": "handoff_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "created_at": "2026-03-17T12:00:00Z",
                    "recipient_status": "pending",
                },
            )
            self._write_handoff_artifact(
                repo_root,
                {
                    **base,
                    "handoff_id": "handoff_cccccccccccccccccccccccccccccccc",
                    "created_at": "2026-03-16T12:00:00Z",
                    "recipient_status": "accepted_advisory",
                },
            )
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_handoffs_query(
                    recipient_peer="peer-beta",
                    sender_peer=None,
                    status=None,
                    offset=1,
                    limit=1,
                    auth=_AuthStub(peer_id="peer-beta"),
                )

            self.assertEqual(out["total_matches"], 3)
            self.assertEqual(out["count"], 1)
            self.assertNotIn("handoff_artifact_skipped_invalid", out["warnings"])
            self.assertEqual(out["handoffs"][0]["handoff_id"], "handoff_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

    def test_query_rejects_foreign_identity_and_skips_invalid_artifacts_with_warning(self) -> None:
        """Query should reject foreign identities and degrade safely around invalid artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_handoff_artifact(
                repo_root,
                {
                    "schema_type": "continuity_handoff",
                    "schema_version": "1.0",
                    "handoff_id": "handoff_11111111111111111111111111111111",
                    "created_at": "2026-03-17T12:00:00Z",
                    "created_by": "peer-alpha",
                    "sender_peer": "peer-alpha",
                    "recipient_peer": "peer-beta",
                    "source_selector": {"subject_kind": "task", "subject_id": "build-phase-5a"},
                    "source_summary": {
                        "path": "memory/continuity/task/build-phase-5a.json",
                        "updated_at": "2026-03-17T10:00:00Z",
                        "verified_at": "2026-03-17T10:00:00Z",
                        "verification_status": "peer_confirmed",
                        "health_status": "healthy",
                    },
                    "task_id": None,
                    "thread_id": None,
                    "note": None,
                    "shared_continuity": {"active_constraints": [], "drift_signals": []},
                    "recipient_status": "pending",
                    "recipient_reason": None,
                    "consumed_at": None,
                    "consumed_by": None,
                },
            )
            bad_path = repo_root / "memory" / "coordination" / "handoffs" / "handoff_invalid.json"
            bad_path.write_text("{bad-json", encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as foreign_cm:
                    coordination_handoffs_query(
                        recipient_peer="peer-gamma",
                        sender_peer=None,
                        status=None,
                        offset=0,
                        limit=20,
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(foreign_cm.exception.status_code, 403)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = coordination_handoffs_query(
                    recipient_peer="peer-beta",
                    sender_peer=None,
                    status=None,
                    offset=0,
                    limit=20,
                    auth=_AuthStub(peer_id="peer-beta"),
                )
            self.assertEqual(out["count"], 1)
            self.assertEqual(out["total_matches"], 1)
            self.assertIn("handoff_artifact_skipped_invalid", out["warnings"])

    def test_query_rejects_invalid_status_value_with_http_400(self) -> None:
        """Direct route calls should surface invalid status values as HTTP 400 instead of 500."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as cm:
                    coordination_handoffs_query(
                        recipient_peer="peer-beta",
                        sender_peer=None,
                        status="invalid",  # type: ignore[arg-type]
                        offset=0,
                        limit=20,
                        auth=_AuthStub(peer_id="peer-beta"),
                    )

            self.assertEqual(cm.exception.status_code, 400)
            self.assertIn("Invalid coordination handoff query", str(cm.exception.detail))

    def test_create_rolls_back_artifact_when_commit_fails(self) -> None:
        """Create should remove the newly written artifact if the commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta")
            self._write_capsule(repo_root)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub(fail_message_prefix="handoff: create"))):
                with self.assertRaises(HTTPException) as cm:
                    coordination_handoff_create(req=self._create_request(commit_message=""), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(cm.exception.status_code, 500)
            handoff_dir = repo_root / "memory" / "coordination" / "handoffs"
            self.assertFalse(handoff_dir.exists() and any(handoff_dir.iterdir()))


if __name__ == "__main__":
    unittest.main()
