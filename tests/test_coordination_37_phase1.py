"""Tests for Issue #37 Phase 1 shared coordination create/read/query behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import Settings
from app.auth import require_auth
from app.main import app, coordination_shared_create, coordination_shared_read, coordination_shared_query
from app.models import CoordinationSharedCreateRequest
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with optional scope and path denial for shared-state tests."""

    def __init__(
        self,
        *,
        peer_id: str,
        deny_scope: str | None = None,
        deny_write_path: str | None = None,
        scopes: set[str] | None = None,
    ) -> None:
        """Store identity plus optional denial controls for one test caller."""
        super().__init__(peer_id=peer_id)
        self.deny_scope = deny_scope
        self.deny_write_path = deny_write_path
        self.scopes = scopes or set()

    def require(self, scope: str) -> None:
        """Raise when the test requests a denied scope."""
        if self.deny_scope == scope:
            raise HTTPException(status_code=403, detail=f"Missing scope: {scope}")

    def require_write_path(self, path: str) -> None:
        """Raise when the test requests a denied write namespace."""
        if self.deny_write_path == path:
            raise HTTPException(status_code=403, detail=f"Write path namespace not allowed: {path.split('/', 1)[0]}")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records commit requests for shared coordination tests."""

    def __init__(self, *, fail_message_prefix: str | None = None) -> None:
        """Store optional commit failure injection criteria."""
        self.fail_message_prefix = fail_message_prefix
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record one commit or raise to simulate a git failure."""
        self.commits.append((str(path), message))
        if self.fail_message_prefix and message.startswith(self.fail_message_prefix):
            raise RuntimeError("simulated commit failure")
        return True


class TestCoordination37Phase1(unittest.TestCase):
    """Validate shared coordination creation, visibility, and query rules."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for shared coordination tests."""
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
        payload = {"schema_version": "1.0", "updated_at": now, "peers": {}}
        payload["peers"][peer_id] = {
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
        if path.exists():
            current = json.loads(path.read_text(encoding="utf-8"))
            current.setdefault("peers", {}).update(payload["peers"])
            payload = current
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_shared_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw shared coordination artifact fixture."""
        path = repo_root / "memory" / "coordination" / "shared" / f"{payload['shared_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

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

    def _create_request(self, **overrides: object) -> CoordinationSharedCreateRequest:
        """Return a valid shared coordination create request."""
        payload = {
            "participant_peers": ["peer-beta", "peer-gamma"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Retry slice coordination",
            "summary": "Shared constraints and drift signals.",
            "constraints": ["Do not weaken durability guarantees."],
            "drift_signals": ["External review may invalidate timing assumptions."],
            "coordination_alerts": ["One participant reports missing context."],
        }
        payload.update(overrides)
        return CoordinationSharedCreateRequest(**payload)

    def test_create_persists_owner_authored_shared_artifact(self) -> None:
        """Create should persist the bounded shared-state artifact with owner-derived identity."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-gamma", trust_level="restricted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_shared_create(req=self._create_request(), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertTrue(out["ok"])
            self.assertTrue(out["created"])
            self.assertEqual(out["path"], f"memory/coordination/shared/{out['shared']['shared_id']}.json")
            self.assertRegex(out["shared"]["shared_id"], r"^shared_[0-9a-f]{32}$")
            self.assertEqual(out["shared"]["owner_peer"], "peer-alpha")
            self.assertEqual(out["shared"]["created_by"], "peer-alpha")
            self.assertEqual(out["shared"]["last_updated_by"], "peer-alpha")
            self.assertEqual(out["shared"]["version"], 1)
            self.assertEqual(
                out["shared"]["shared_state"],
                {
                    "constraints": ["Do not weaken durability guarantees."],
                    "drift_signals": ["External review may invalidate timing assumptions."],
                    "coordination_alerts": ["One participant reports missing context."],
                },
            )

    def test_create_rejects_overlong_commit_message(self) -> None:
        """Create should reject overlong custom commit messages with the exact detail string."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-beta"], commit_message="x" * 121),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Value too long in coordination_shared.commit_message")

    def test_create_rejects_title_summary_and_item_length_bounds(self) -> None:
        """Create should enforce the deterministic title, summary, and item-length validation rules."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            cases = [
                (
                    {"title": ""},
                    "Value too short in coordination_shared.title",
                ),
                (
                    {"title": "x" * 121},
                    "Value too long in coordination_shared.title",
                ),
                (
                    {"summary": ""},
                    "Value too short in coordination_shared.summary",
                ),
                (
                    {"summary": "x" * 241},
                    "Value too long in coordination_shared.summary",
                ),
                (
                    {"constraints": [""]},
                    "Value too short in coordination_shared.constraints",
                ),
                (
                    {"constraints": ["x" * 161]},
                    "Value too long in coordination_shared.constraints",
                ),
                (
                    {"drift_signals": [""]},
                    "Value too short in coordination_shared.drift_signals",
                ),
                (
                    {"drift_signals": ["x" * 161]},
                    "Value too long in coordination_shared.drift_signals",
                ),
                (
                    {"coordination_alerts": [""]},
                    "Value too short in coordination_shared.coordination_alerts",
                ),
                (
                    {"coordination_alerts": ["x" * 161]},
                    "Value too long in coordination_shared.coordination_alerts",
                ),
            ]

            for overrides, expected_detail in cases:
                with self.subTest(expected_detail=expected_detail):
                    with patch("app.main._services", return_value=(settings, gm)):
                        with self.assertRaises(HTTPException) as ctx:
                            coordination_shared_create(
                                req=self._create_request(participant_peers=["peer-beta"], **overrides),
                                auth=_AuthStub(peer_id="peer-alpha"),
                            )
                    self.assertEqual(ctx.exception.status_code, 400)
                    self.assertEqual(ctx.exception.detail, expected_detail)

    def test_create_rejects_all_empty_shared_state(self) -> None:
        """Create should reject requests where all three shared-state arrays are empty."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-beta"], constraints=[], drift_signals=[], coordination_alerts=[]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Shared coordination state must include at least one shared item")

    def test_create_fails_fast_on_first_invalid_participant(self) -> None:
        """Create should stop at the first invalid participant in caller-supplied order."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-known", trust_level="untrusted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-missing", "peer-known"]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 404)
            self.assertEqual(ctx.exception.detail, "Peer not found: peer-missing")

    def test_create_rejects_empty_duplicate_owner_and_untrusted_participants(self) -> None:
        """Create should reject the remaining participant admission failure modes explicitly."""
        with self.assertRaises(ValidationError):
            self._create_request(participant_peers=[])

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-dup", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-beta", trust_level="untrusted")
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as dup_ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-dup", "peer-dup"]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(dup_ctx.exception.status_code, 400)
            self.assertEqual(dup_ctx.exception.detail, "participant_peers must not contain duplicates")

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as owner_ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-alpha"]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(owner_ctx.exception.status_code, 400)
            self.assertEqual(owner_ctx.exception.detail, "participant_peers must not include owner_peer")

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as untrusted_ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-beta"]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(untrusted_ctx.exception.status_code, 409)
            self.assertEqual(untrusted_ctx.exception.detail, "Peer is untrusted: peer-beta")

    def test_read_allows_owner_participant_and_admin(self) -> None:
        """Read should allow only the owner, listed participants, or an admin caller."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = self._artifact_payload()
            self._write_shared_artifact(repo_root, payload)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                owner = coordination_shared_read(shared_id=payload["shared_id"], auth=_AuthStub(peer_id="peer-alpha"))
                participant = coordination_shared_read(shared_id=payload["shared_id"], auth=_AuthStub(peer_id="peer-beta"))
                admin = coordination_shared_read(
                    shared_id=payload["shared_id"],
                    auth=_AuthStub(peer_id="peer-other", scopes={"admin:peers"}),
                )

            self.assertEqual(owner["shared"]["shared_id"], payload["shared_id"])
            self.assertEqual(participant["shared"]["shared_id"], payload["shared_id"])
            self.assertEqual(admin["shared"]["shared_id"], payload["shared_id"])

    def test_read_rejects_unrelated_caller(self) -> None:
        """Read should reject authenticated callers outside owner/participant/admin visibility."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = self._artifact_payload()
            self._write_shared_artifact(repo_root, payload)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_read(shared_id=payload["shared_id"], auth=_AuthStub(peer_id="peer-other"))

            self.assertEqual(ctx.exception.status_code, 403)
            self.assertEqual(ctx.exception.detail, "Shared coordination artifact not visible to caller")

    def test_read_rejects_invalid_shared_id_format(self) -> None:
        """Read should reject malformed shared ids before probing the filesystem."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_read(shared_id="shared_foo", auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Invalid shared coordination artifact id")

    def test_query_requires_explicit_self_identity_for_non_admin(self) -> None:
        """Non-admin callers should not be able to run task-only or thread-only queries."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_query(task_id="task-123", auth=_AuthStub(peer_id="peer-beta"))

            self.assertEqual(ctx.exception.status_code, 403)
            self.assertEqual(ctx.exception.detail, "Non-admin callers must include their own shared coordination identity")

    def test_query_treats_participant_filter_as_membership(self) -> None:
        """Query should match artifacts where the participant appears in the participant list."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            newer = self._artifact_payload(
                shared_id="shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                participant_peers=["peer-beta", "peer-gamma"],
                updated_at="2026-03-17T12:00:00Z",
            )
            older = self._artifact_payload(
                shared_id="shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                participant_peers=["peer-zeta"],
                updated_at="2026-03-16T12:00:00Z",
            )
            self._write_shared_artifact(repo_root, newer)
            self._write_shared_artifact(repo_root, older)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                out = coordination_shared_query(
                    participant_peer="peer-beta",
                    auth=_AuthStub(peer_id="peer-beta"),
                )

            self.assertEqual(out["count"], 1)
            self.assertEqual(out["total_matches"], 1)
            self.assertEqual(out["shared_artifacts"][0]["shared_id"], newer["shared_id"])

    def test_query_uses_conjunctive_filters_and_deterministic_sorting(self) -> None:
        """Query should apply all filters conjunctively and sort by updated_at desc then shared_id asc."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            first = self._artifact_payload(
                shared_id="shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                owner_peer="peer-alpha",
                participant_peers=["peer-beta"],
                task_id="task-123",
                updated_at="2026-03-17T12:00:00Z",
            )
            second = self._artifact_payload(
                shared_id="shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                owner_peer="peer-alpha",
                participant_peers=["peer-beta"],
                task_id="task-123",
                updated_at="2026-03-17T12:00:00Z",
            )
            third = self._artifact_payload(
                shared_id="shared_cccccccccccccccccccccccccccccccc",
                owner_peer="peer-alpha",
                participant_peers=["peer-beta"],
                task_id="task-123",
                updated_at="2026-03-18T12:00:00Z",
            )
            filtered = self._artifact_payload(
                shared_id="shared_dddddddddddddddddddddddddddddddd",
                owner_peer="peer-alpha",
                participant_peers=["peer-beta"],
                task_id="task-other",
                updated_at="2026-03-19T12:00:00Z",
            )
            self._write_shared_artifact(repo_root, first)
            self._write_shared_artifact(repo_root, second)
            self._write_shared_artifact(repo_root, third)
            self._write_shared_artifact(repo_root, filtered)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                out = coordination_shared_query(
                    owner_peer="peer-alpha",
                    participant_peer="peer-beta",
                    task_id="task-123",
                    auth=_AuthStub(peer_id="peer-admin", scopes={"admin:peers"}),
                )

            self.assertEqual(out["count"], 3)
            self.assertEqual(out["total_matches"], 3)
            self.assertEqual(
                [item["shared_id"] for item in out["shared_artifacts"]],
                [
                    "shared_cccccccccccccccccccccccccccccccc",
                    "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                ],
            )

    def test_query_skips_invalid_artifacts_and_uses_shared_artifacts_key(self) -> None:
        """Query should skip invalid stored artifacts and surface a single warning."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            valid = self._artifact_payload(updated_at="2026-03-17T12:00:00Z")
            self._write_shared_artifact(repo_root, valid)
            invalid_path = repo_root / "memory" / "coordination" / "shared" / "shared_invalid.json"
            invalid_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_path.write_text("{bad json", encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                out = coordination_shared_query(owner_peer="peer-alpha", auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(out["warnings"], ["coordination_shared_artifact_skipped_invalid"])
            self.assertEqual(out["count"], 1)
            self.assertIn("shared_artifacts", out)
            self.assertNotIn("shared", out)

    def test_create_rolls_back_artifact_when_commit_fails(self) -> None:
        """Create should remove the newly written shared artifact if the commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-beta", trust_level="trusted")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub(fail_message_prefix="coordination: create"))):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_shared_create(
                        req=self._create_request(participant_peers=["peer-beta"]),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertEqual(ctx.exception.detail, "Failed to commit shared coordination artifact")
            shared_dir = repo_root / "memory" / "coordination" / "shared"
            self.assertEqual(sorted(shared_dir.glob("*.json")), [])

    def test_http_query_route_hits_query_handler_not_read_handler(self) -> None:
        """HTTP GET /v1/coordination/shared/query should reach the query route, not the {shared_id} read route."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            valid = self._artifact_payload(updated_at="2026-03-17T12:00:00Z")
            self._write_shared_artifact(repo_root, valid)
            settings = self._settings(repo_root)
            auth = _AuthStub(peer_id="peer-alpha")
            client = TestClient(app)
            app.dependency_overrides[require_auth] = lambda: auth
            try:
                with patch("app.main._services", return_value=(settings, None)):
                    response = client.get(
                        "/v1/coordination/shared/query",
                        params={"owner_peer": "peer-alpha"},
                    )
            finally:
                app.dependency_overrides.clear()

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertIn("shared_artifacts", body)
            self.assertNotIn("shared", body)
            self.assertEqual(body["count"], 1)


if __name__ == "__main__":
    unittest.main()
