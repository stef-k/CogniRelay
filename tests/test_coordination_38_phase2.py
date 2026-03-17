"""Tests for Issue #38 Phase 2 reconciliation resolve flow and conflict semantics."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.main import (
    coordination_reconciliation_read,
    coordination_reconciliation_resolve,
)
from app.models import CoordinationReconciliationResolveRequest
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with optional scope and path denial for reconciliation resolve tests."""

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
    """Git stub that records commit requests for reconciliation resolve tests."""

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


class TestCoordination38Phase2(unittest.TestCase):
    """Validate reconciliation resolve flow, version checking, replay, and rollback."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for reconciliation resolve tests."""
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

    def _write_reconciliation_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw reconciliation artifact fixture."""
        path = repo_root / "memory" / "coordination" / "reconciliations" / f"{payload['reconciliation_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _reconciliation_payload(self, **overrides: object) -> dict:
        """Return one valid stored open reconciliation artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_reconciliation_record",
            "schema_version": "1.0",
            "reconciliation_id": "recon_cccccccccccccccccccccccccccccccc",
            "created_at": now,
            "updated_at": now,
            "opened_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Constraint disagreement on release timing",
            "summary": "Two visible coordination claims disagree.",
            "classification": "contradictory",
            "trigger": "shared_vs_shared",
            "claims": [
                {
                    "source_kind": "shared",
                    "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "claimant_peer": "peer-alpha",
                    "claim_summary": "The freeze still applies.",
                    "epistemic_status": "frame_present",
                    "evidence_refs": ["msg_123"],
                    "observed_version": 2,
                },
                {
                    "source_kind": "shared",
                    "source_id": "shared_dddddddddddddddddddddddddddddddd",
                    "claimant_peer": "peer-beta",
                    "claim_summary": "The freeze was lifted.",
                    "epistemic_status": "frame_status_unknown",
                    "evidence_refs": ["msg_456"],
                    "observed_version": 3,
                },
            ],
            "status": "open",
            "resolution_outcome": None,
            "resolution_summary": None,
            "resolved_at": None,
            "resolved_by": None,
            "version": 1,
            "last_updated_by": "peer-alpha",
        }
        payload.update(overrides)
        return payload

    def _resolve_request(self, **overrides: object) -> CoordinationReconciliationResolveRequest:
        """Return a valid reconciliation resolve request."""
        payload: dict = {
            "expected_version": 1,
            "outcome": "conflicted",
            "resolution_summary": "Evidence is insufficient to reject either claim.",
        }
        payload.update(overrides)
        return CoordinationReconciliationResolveRequest(**payload)

    # ------------------------------------------------------------------
    # Successful resolve
    # ------------------------------------------------------------------

    def test_resolve_owner_succeeds_and_writes_bounded_fields(self) -> None:
        """Owner resolve should set status, outcome, summary, resolved_at/by, version, and last_updated_by."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._reconciliation_payload()
            self._write_reconciliation_artifact(repo_root, artifact)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertTrue(out["ok"])
            self.assertTrue(out["updated"])
            resolved = out["reconciliation"]
            self.assertEqual(resolved["status"], "resolved")
            self.assertEqual(resolved["resolution_outcome"], "conflicted")
            self.assertEqual(resolved["resolution_summary"], "Evidence is insufficient to reject either claim.")
            self.assertEqual(resolved["resolved_by"], "peer-alpha")
            self.assertEqual(resolved["last_updated_by"], "peer-alpha")
            self.assertEqual(resolved["version"], 2)
            self.assertIsNotNone(resolved["resolved_at"])
            self.assertIn("latest_commit", out)
            # Verify commit message format
            self.assertEqual(len(gm.commits), 1)
            self.assertIn("coordination: resolve recon_cccccccccccccccccccccccccccccccc conflicted", gm.commits[0][1])

    def test_resolve_admin_can_resolve_any_artifact(self) -> None:
        """Admin callers should be able to resolve artifacts they do not own."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            artifact = self._reconciliation_payload(owner_peer="peer-alpha")
            self._write_reconciliation_artifact(repo_root, artifact)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(outcome="advisory_only", resolution_summary="Noted for visibility."),
                    auth=_AuthStub(peer_id="peer-admin", scopes={"admin:peers"}),
                )

            self.assertTrue(out["ok"])
            self.assertTrue(out["updated"])
            self.assertEqual(out["reconciliation"]["resolution_outcome"], "advisory_only")
            self.assertEqual(out["reconciliation"]["resolved_by"], "peer-admin")

    def test_resolve_custom_commit_message(self) -> None:
        """A non-empty custom commit message should be used verbatim."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(commit_message="custom: closing disagreement"),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertEqual(gm.commits[0][1], "custom: closing disagreement")

    def test_resolve_all_three_outcomes(self) -> None:
        """Each first-slice outcome should be accepted."""
        for outcome in ("advisory_only", "conflicted", "rejected"):
            with tempfile.TemporaryDirectory() as td:
                repo_root = Path(td)
                self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
                settings = self._settings(repo_root)
                gm = _GitManagerStub()

                with patch("app.main._services", return_value=(settings, gm)):
                    out = coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(outcome=outcome, resolution_summary="Outcome recorded."),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

                self.assertEqual(out["reconciliation"]["resolution_outcome"], outcome)

    def test_resolve_persists_canonical_json(self) -> None:
        """Resolved artifact on disk should use canonical JSON serialization."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            path = repo_root / "memory" / "coordination" / "reconciliations" / "recon_cccccccccccccccccccccccccccccccc.json"
            on_disk = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(on_disk["status"], "resolved")
            self.assertEqual(on_disk["version"], 2)
            # Verify canonical JSON: sorted keys, 2-space indent
            raw = path.read_text(encoding="utf-8")
            self.assertEqual(raw, canonical_json(on_disk))

    # ------------------------------------------------------------------
    # Auth and ownership
    # ------------------------------------------------------------------

    def test_resolve_rejects_non_owner_non_admin(self) -> None:
        """Non-owner, non-admin callers should be rejected with HTTP 403."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload(owner_peer="peer-alpha"))
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(),
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(ctx.exception.status_code, 403)
            self.assertIn("owner", ctx.exception.detail.lower())

    def test_resolve_requires_write_projects_scope(self) -> None:
        """Resolve should require write:projects scope."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(),
                        auth=_AuthStub(peer_id="peer-alpha", deny_scope="write:projects"),
                    )
            self.assertEqual(ctx.exception.status_code, 403)

    def test_resolve_requires_write_path_access(self) -> None:
        """Resolve should require write access to the reconciliation namespace."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(),
                        auth=_AuthStub(
                            peer_id="peer-alpha",
                            deny_write_path="memory/coordination/reconciliations/x.json",
                        ),
                    )
            self.assertEqual(ctx.exception.status_code, 403)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def test_resolve_rejects_malformed_reconciliation_id(self) -> None:
        """Malformed reconciliation_id should return HTTP 400."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            for bad_id in ("bad-id", "recon_short", "recon_ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ", "shared_aaa"):
                with patch("app.main._services", return_value=(settings, gm)):
                    with self.assertRaises(HTTPException) as ctx:
                        coordination_reconciliation_resolve(
                            reconciliation_id=bad_id,
                            req=self._resolve_request(),
                            auth=_AuthStub(peer_id="peer-alpha"),
                        )
                self.assertEqual(ctx.exception.status_code, 400, f"Expected 400 for {bad_id}")
                self.assertEqual(ctx.exception.detail, "Invalid reconciliation artifact id")

    def test_resolve_returns_404_for_missing_artifact(self) -> None:
        """Resolve against a non-existent artifact should return HTTP 404."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                        req=self._resolve_request(),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertEqual(ctx.exception.detail, "Reconciliation artifact not found")

    def test_resolve_rejects_missing_resolution_summary(self) -> None:
        """Resolve without resolution_summary should return HTTP 400."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(resolution_summary=None),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "resolution_summary is required for reconciliation resolve")

    def test_resolve_rejects_empty_resolution_summary(self) -> None:
        """Empty resolution_summary should return HTTP 400 with the exact detail."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(resolution_summary=""),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Value too short in coordination_reconciliation.resolution_summary")

    def test_resolve_rejects_overlong_resolution_summary(self) -> None:
        """Overlong resolution_summary should return HTTP 400 with the exact detail."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(resolution_summary="x" * 241),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Value too long in coordination_reconciliation.resolution_summary")

    def test_resolve_rejects_overlong_commit_message(self) -> None:
        """Overlong commit_message should return HTTP 400 with the exact detail."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(commit_message="x" * 121),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail, "Value too long in coordination_reconciliation.commit_message")

    # ------------------------------------------------------------------
    # Version conflicts
    # ------------------------------------------------------------------

    def test_resolve_rejects_stale_expected_version(self) -> None:
        """Stale expected_version should return HTTP 409 with exact detail."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload(version=2))
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(expected_version=1),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 409)
            self.assertEqual(ctx.exception.detail, "Reconciliation version conflict")

    def test_resolve_rejects_future_expected_version(self) -> None:
        """Future expected_version should also be rejected as a version conflict."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload(version=1))
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(expected_version=5),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 409)
            self.assertEqual(ctx.exception.detail, "Reconciliation version conflict")

    # ------------------------------------------------------------------
    # Replay idempotency
    # ------------------------------------------------------------------

    def test_resolve_replay_same_outcome_returns_updated_false(self) -> None:
        """Replay with same outcome and summary should return updated=false without a new commit."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            resolved_artifact = self._reconciliation_payload(
                status="resolved",
                resolution_outcome="conflicted",
                resolution_summary="Evidence is insufficient to reject either claim.",
                resolved_at=self._now(),
                resolved_by="peer-alpha",
                version=2,
            )
            self._write_reconciliation_artifact(repo_root, resolved_artifact)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(
                        expected_version=2,
                        outcome="conflicted",
                        resolution_summary="Evidence is insufficient to reject either claim.",
                    ),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertTrue(out["ok"])
            self.assertFalse(out["updated"])
            self.assertEqual(len(gm.commits), 0)

    def test_resolve_replay_different_outcome_returns_409(self) -> None:
        """Replay with a different outcome should return HTTP 409."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            resolved_artifact = self._reconciliation_payload(
                status="resolved",
                resolution_outcome="conflicted",
                resolution_summary="Evidence is insufficient.",
                resolved_at=self._now(),
                resolved_by="peer-alpha",
                version=2,
            )
            self._write_reconciliation_artifact(repo_root, resolved_artifact)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(
                            expected_version=2,
                            outcome="advisory_only",
                            resolution_summary="Evidence is insufficient.",
                        ),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 409)
            self.assertEqual(ctx.exception.detail, "Reconciliation has already been resolved")

    def test_resolve_replay_different_summary_returns_409(self) -> None:
        """Replay with a different resolution_summary should return HTTP 409."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            resolved_artifact = self._reconciliation_payload(
                status="resolved",
                resolution_outcome="conflicted",
                resolution_summary="Original summary.",
                resolved_at=self._now(),
                resolved_by="peer-alpha",
                version=2,
            )
            self._write_reconciliation_artifact(repo_root, resolved_artifact)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(
                            expected_version=2,
                            outcome="conflicted",
                            resolution_summary="Different summary.",
                        ),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(ctx.exception.status_code, 409)
            self.assertEqual(ctx.exception.detail, "Reconciliation has already been resolved")

    # ------------------------------------------------------------------
    # Rollback on commit failure
    # ------------------------------------------------------------------

    def test_resolve_restores_prior_bytes_on_commit_failure(self) -> None:
        """Commit failure during resolve should restore the prior artifact and return HTTP 500."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            original = self._reconciliation_payload()
            self._write_reconciliation_artifact(repo_root, original)
            path = repo_root / "memory" / "coordination" / "reconciliations" / "recon_cccccccccccccccccccccccccccccccc.json"
            original_bytes = path.read_bytes()
            settings = self._settings(repo_root)
            gm = _GitManagerStub(fail_message_prefix="coordination: resolve")

            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertEqual(ctx.exception.detail, "Failed to commit reconciliation resolve")
            # Verify prior bytes are restored
            self.assertTrue(path.exists())
            self.assertEqual(path.read_bytes(), original_bytes)

    # ------------------------------------------------------------------
    # Structural integrity
    # ------------------------------------------------------------------

    def test_resolve_does_not_change_non_resolve_fields(self) -> None:
        """Resolve should only change bounded resolve fields plus update metadata."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            original = self._reconciliation_payload()
            self._write_reconciliation_artifact(repo_root, original)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            resolved = out["reconciliation"]
            # Non-resolve fields should remain unchanged
            self.assertEqual(resolved["reconciliation_id"], original["reconciliation_id"])
            self.assertEqual(resolved["created_at"], original["created_at"])
            self.assertEqual(resolved["opened_by"], original["opened_by"])
            self.assertEqual(resolved["owner_peer"], original["owner_peer"])
            self.assertEqual(resolved["participant_peers"], original["participant_peers"])
            self.assertEqual(resolved["task_id"], original["task_id"])
            self.assertEqual(resolved["thread_id"], original["thread_id"])
            self.assertEqual(resolved["title"], original["title"])
            self.assertEqual(resolved["summary"], original["summary"])
            self.assertEqual(resolved["classification"], original["classification"])
            self.assertEqual(resolved["trigger"], original["trigger"])
            self.assertEqual(resolved["claims"], original["claims"])

    def test_resolve_version_increments_by_exactly_one(self) -> None:
        """Resolve should increment version by exactly 1."""
        for starting_version in (1, 3, 7):
            with tempfile.TemporaryDirectory() as td:
                repo_root = Path(td)
                artifact = self._reconciliation_payload(version=starting_version, status="open")
                self._write_reconciliation_artifact(repo_root, artifact)
                settings = self._settings(repo_root)
                gm = _GitManagerStub()

                with patch("app.main._services", return_value=(settings, gm)):
                    out = coordination_reconciliation_resolve(
                        reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                        req=self._resolve_request(expected_version=starting_version),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )

                self.assertEqual(out["reconciliation"]["version"], starting_version + 1)

    def test_read_after_resolve_observes_resolved_state(self) -> None:
        """A read after resolve should observe the resolved durable state."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )
                read_out = coordination_reconciliation_read(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertEqual(read_out["reconciliation"]["status"], "resolved")
            self.assertEqual(read_out["reconciliation"]["version"], 2)

    def test_resolve_default_commit_message_includes_outcome(self) -> None:
        """Default commit message should include reconciliation_id and outcome."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(outcome="rejected", resolution_summary="Claim rejected."),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertEqual(
                gm.commits[0][1],
                "coordination: resolve recon_cccccccccccccccccccccccccccccccc rejected",
            )

    def test_resolve_whitespace_commit_message_uses_default(self) -> None:
        """A whitespace-only commit message should fall back to the default."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                coordination_reconciliation_resolve(
                    reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                    req=self._resolve_request(commit_message="   "),
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertIn("coordination: resolve", gm.commits[0][1])


if __name__ == "__main__":
    unittest.main()
