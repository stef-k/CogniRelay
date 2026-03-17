"""Tests for Issue #38 Phase 1 reconciliation open/read/query behavior."""

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
    coordination_reconciliation_open,
    coordination_reconciliation_read,
    coordination_reconciliations_query,
)
from app.models import CoordinationReconciliationOpenRequest
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with optional scope and path denial for reconciliation tests."""

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
    """Git stub that records commit requests for reconciliation tests."""

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


class TestCoordination38Phase1(unittest.TestCase):
    """Validate reconciliation creation, visibility, and query rules."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for reconciliation tests."""
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

    def _write_handoff_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw handoff artifact fixture."""
        path = repo_root / "memory" / "coordination" / "handoffs" / f"{payload['handoff_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _write_shared_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw shared artifact fixture."""
        path = repo_root / "memory" / "coordination" / "shared" / f"{payload['shared_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _write_reconciliation_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw reconciliation artifact fixture."""
        path = repo_root / "memory" / "coordination" / "reconciliations" / f"{payload['reconciliation_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _handoff_payload(self, **overrides: object) -> dict:
        """Return one valid stored handoff artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "continuity_handoff",
            "schema_version": "1.0",
            "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "created_at": now,
            "created_by": "peer-alpha",
            "sender_peer": "peer-alpha",
            "recipient_peer": "peer-beta",
            "source_selector": {"subject_kind": "task", "subject_id": "task-123"},
            "source_summary": {
                "path": "memory/continuity/task-task-123.json",
                "updated_at": now,
                "verified_at": now,
                "verification_status": "peer_confirmed",
                "health_status": "healthy",
            },
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "note": "Carry the constraint forward.",
            "shared_continuity": {
                "active_constraints": ["Do not weaken the release freeze."],
                "drift_signals": ["Timing assumptions remain unstable."],
            },
            "recipient_status": "pending",
            "recipient_reason": None,
            "consumed_at": None,
            "consumed_by": None,
        }
        payload.update(overrides)
        return payload

    def _shared_payload(self, **overrides: object) -> dict:
        """Return one valid stored shared artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_shared_state",
            "schema_version": "1.0",
            "shared_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "created_at": now,
            "updated_at": now,
            "created_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta", "peer-gamma"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Release timing",
            "summary": "Bounded shared coordination state.",
            "shared_state": {
                "constraints": ["Do not lift the freeze without review."],
                "drift_signals": ["One participant sees stale context."],
                "coordination_alerts": ["Missing context remains possible."],
            },
            "version": 3,
            "last_updated_by": "peer-alpha",
        }
        payload.update(overrides)
        return payload

    def _reconciliation_payload(self, **overrides: object) -> dict:
        """Return one valid stored reconciliation artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_reconciliation_record",
            "schema_version": "1.0",
            "reconciliation_id": "recon_cccccccccccccccccccccccccccccccc",
            "created_at": now,
            "updated_at": now,
            "opened_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta", "peer-gamma"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Constraint disagreement on release timing",
            "summary": "Two visible coordination claims disagree about whether the release freeze still holds.",
            "classification": "contradictory",
            "trigger": "shared_vs_shared",
            "claims": [
                {
                    "source_kind": "shared",
                    "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "claimant_peer": "peer-alpha",
                    "claim_summary": "The freeze still applies until Friday.",
                    "epistemic_status": "frame_present",
                    "evidence_refs": ["msg_123"],
                    "observed_version": 2,
                },
                {
                    "source_kind": "shared",
                    "source_id": "shared_dddddddddddddddddddddddddddddddd",
                    "claimant_peer": "peer-beta",
                    "claim_summary": "The freeze was lifted in a later review thread.",
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

    def _open_request(self, **overrides: object) -> CoordinationReconciliationOpenRequest:
        """Return a valid reconciliation open request."""
        payload = {
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Constraint disagreement on release timing",
            "summary": "Two visible coordination claims disagree about whether the release freeze still holds.",
            "classification": "contradictory",
            "trigger": "shared_vs_shared",
            "claims": [
                {
                    "source_kind": "shared",
                    "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "claimant_peer": "peer-alpha",
                    "claim_summary": "The freeze still applies until Friday.",
                    "epistemic_status": "frame_present",
                    "evidence_refs": ["msg_123"],
                    "observed_version": 2,
                },
                {
                    "source_kind": "shared",
                    "source_id": "shared_dddddddddddddddddddddddddddddddd",
                    "claimant_peer": "peer-beta",
                    "claim_summary": "The freeze was lifted in a later review thread.",
                    "epistemic_status": "frame_status_unknown",
                    "evidence_refs": ["msg_456"],
                    "observed_version": 3,
                },
            ],
        }
        payload.update(overrides)
        return CoordinationReconciliationOpenRequest(**payload)

    def test_open_persists_reconciliation_artifact_and_derives_participants(self) -> None:
        """Open should persist the bounded artifact with owner-derived identity and first-seen participants."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-alpha", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-beta", trust_level="restricted")
            self._write_peer_registry(repo_root, "peer-gamma", trust_level="trusted")
            self._write_shared_artifact(
                repo_root,
                self._shared_payload(shared_id="shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", version=2),
            )
            self._write_shared_artifact(
                repo_root,
                self._shared_payload(
                    shared_id="shared_dddddddddddddddddddddddddddddddd",
                    owner_peer="peer-beta",
                    participant_peers=["peer-alpha", "peer-gamma"],
                    version=3,
                ),
            )
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                out = coordination_reconciliation_open(req=self._open_request(), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertTrue(out["ok"])
            self.assertTrue(out["created"])
            self.assertRegex(out["reconciliation"]["reconciliation_id"], r"^recon_[0-9a-f]{32}$")
            self.assertEqual(out["path"], f"memory/coordination/reconciliations/{out['reconciliation']['reconciliation_id']}.json")
            self.assertEqual(out["reconciliation"]["owner_peer"], "peer-alpha")
            self.assertEqual(out["reconciliation"]["opened_by"], "peer-alpha")
            self.assertEqual(out["reconciliation"]["participant_peers"], ["peer-beta"])
            stored_text = (repo_root / out["path"]).read_text(encoding="utf-8")
            self.assertEqual(stored_text, canonical_json(json.loads(stored_text)))

    def test_open_rejects_missing_task_thread_and_too_few_claims(self) -> None:
        """Open should reject the first required structural preconditions with exact details."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as task_ctx:
                    coordination_reconciliation_open(
                        req=self._open_request(task_id=None, thread_id=None),
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(task_ctx.exception.status_code, 400)
            self.assertEqual(task_ctx.exception.detail, "task_id or thread_id is required")

            one_claim = self._open_request().model_copy(update={"claims": self._open_request().claims[:1]})
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as claims_ctx:
                    coordination_reconciliation_open(req=one_claim, auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(claims_ctx.exception.status_code, 400)
            self.assertEqual(claims_ctx.exception.detail, "Reconciliation must include at least two claims")

    def test_open_rejects_duplicate_claims_and_text_bounds(self) -> None:
        """Open should enforce duplicate-claim and text-bound validation with exact details."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            duplicate_request = self._open_request().model_copy(
                update={"claims": [self._open_request().claims[0], self._open_request().claims[0]]}
            )
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as dup_ctx:
                    coordination_reconciliation_open(req=duplicate_request, auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(dup_ctx.exception.status_code, 400)
            self.assertEqual(dup_ctx.exception.detail, "Reconciliation claims must be unique")

            cases = [
                ({"title": ""}, "Value too short in coordination_reconciliation.title"),
                ({"title": "x" * 121}, "Value too long in coordination_reconciliation.title"),
                ({"summary": ""}, "Value too short in coordination_reconciliation.summary"),
                ({"summary": "x" * 241}, "Value too long in coordination_reconciliation.summary"),
                (
                    {
                        "claims": [
                            self._open_request().claims[0].model_copy(update={"claim_summary": ""}),
                            self._open_request().claims[1],
                        ]
                    },
                    "Value too short in coordination_reconciliation.claim_summary",
                ),
                (
                    {
                        "claims": [
                            self._open_request().claims[0].model_copy(update={"claim_summary": "x" * 241}),
                            self._open_request().claims[1],
                        ]
                    },
                    "Value too long in coordination_reconciliation.claim_summary",
                ),
                (
                    {
                        "claims": [
                            self._open_request().claims[0].model_copy(update={"evidence_refs": [""]}),
                            self._open_request().claims[1],
                        ]
                    },
                    "Value too short in coordination_reconciliation.evidence_refs",
                ),
                (
                    {
                        "claims": [
                            self._open_request().claims[0].model_copy(update={"evidence_refs": ["x" * 161]}),
                            self._open_request().claims[1],
                        ]
                    },
                    "Value too long in coordination_reconciliation.evidence_refs",
                ),
                ({"commit_message": "x" * 121}, "Value too long in coordination_reconciliation.commit_message"),
            ]
            for overrides, expected_detail in cases:
                with self.subTest(expected_detail=expected_detail):
                    with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                        with self.assertRaises(HTTPException) as ctx:
                            coordination_reconciliation_open(req=self._open_request(**overrides), auth=_AuthStub(peer_id="peer-alpha"))
                    self.assertEqual(ctx.exception.status_code, 400)
                    self.assertEqual(ctx.exception.detail, expected_detail)

    def test_open_rejects_invalid_claim_relationships_and_versions(self) -> None:
        """Open should reject claimant/source mismatches, trigger mismatches, and invalid observed-version rules."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            for peer_id, trust in [
                ("peer-alpha", "trusted"),
                ("peer-beta", "restricted"),
                ("peer-gamma", "trusted"),
                ("peer-other", "trusted"),
            ]:
                self._write_peer_registry(repo_root, peer_id, trust_level=trust)
            self._write_handoff_artifact(repo_root, self._handoff_payload())
            self._write_shared_artifact(repo_root, self._shared_payload())
            settings = self._settings(repo_root)

            cases: list[tuple[CoordinationReconciliationOpenRequest, str, int]] = [
                (
                    self._open_request(
                        trigger="handoff_vs_shared",
                        claims=[
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-alpha",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": None,
                            },
                            {
                                "source_kind": "handoff",
                                "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": None,
                            },
                        ],
                    ),
                    "observed_version is required for shared claims",
                    400,
                ),
                (
                    self._open_request(
                        trigger="handoff_vs_handoff",
                        claims=[
                            {
                                "source_kind": "handoff",
                                "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "claimant_peer": "peer-alpha",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 1,
                            },
                            {
                                "source_kind": "handoff",
                                "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": None,
                            },
                        ],
                    ),
                    "observed_version is not allowed for handoff claims",
                    400,
                ),
                (
                    self._open_request(
                        trigger="handoff_vs_handoff",
                        claims=[
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-alpha",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 2,
                            },
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 2,
                            },
                        ],
                    ),
                    "trigger does not match claim source kinds",
                    400,
                ),
                (
                    self._open_request(
                        claims=[
                            {
                                "source_kind": "handoff",
                                "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "claimant_peer": "peer-gamma",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": None,
                            },
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 2,
                            },
                        ],
                        trigger="handoff_vs_shared",
                    ),
                    "Claimant peer is not recognized on referenced handoff artifact",
                    400,
                ),
                (
                    self._open_request(
                        claims=[
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-other",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 2,
                            },
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 2,
                            },
                        ],
                    ),
                    "Claimant peer is not recognized on referenced shared artifact",
                    400,
                ),
                (
                    self._open_request(
                        claims=[
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-alpha",
                                "claim_summary": "A",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 4,
                            },
                            {
                                "source_kind": "shared",
                                "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                "claimant_peer": "peer-beta",
                                "claim_summary": "B",
                                "epistemic_status": "frame_present",
                                "evidence_refs": [],
                                "observed_version": 3,
                            },
                        ],
                    ),
                    "observed_version exceeds stored shared version",
                    400,
                ),
            ]

            for request, expected_detail, expected_status in cases:
                with self.subTest(expected_detail=expected_detail):
                    with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                        with self.assertRaises(HTTPException) as ctx:
                            coordination_reconciliation_open(req=request, auth=_AuthStub(peer_id="peer-alpha"))
                    self.assertEqual(ctx.exception.status_code, expected_status)
                    self.assertEqual(ctx.exception.detail, expected_detail)

    def test_open_rejects_unknown_untrusted_invisible_and_invalid_sources(self) -> None:
        """Open should fail deterministically for peer admission, visibility, and invalid source artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-alpha", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-beta", trust_level="restricted")
            self._write_peer_registry(repo_root, "peer-gamma", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-untrusted", trust_level="untrusted")
            self._write_handoff_artifact(repo_root, self._handoff_payload(sender_peer="peer-beta", recipient_peer="peer-gamma"))
            invalid_shared = repo_root / "memory" / "coordination" / "shared" / "shared_invalidxxxxxxxxxxxxxxxxxxxxxxxxx.json"
            invalid_shared.parent.mkdir(parents=True, exist_ok=True)
            invalid_shared.write_text("{bad json", encoding="utf-8")
            settings = self._settings(repo_root)

            unknown_request = self._open_request(
                trigger="handoff_vs_handoff",
                claims=[
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-missing",
                        "claim_summary": "A",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "B",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                ],
            )
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as unknown_ctx:
                    coordination_reconciliation_open(req=unknown_request, auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(unknown_ctx.exception.status_code, 404)
            self.assertEqual(unknown_ctx.exception.detail, "Peer not found: peer-missing")

            untrusted_request = self._open_request(
                trigger="handoff_vs_handoff",
                claims=[
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-untrusted",
                        "claim_summary": "A",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "B",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                ],
            )
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as untrusted_ctx:
                    coordination_reconciliation_open(req=untrusted_request, auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(untrusted_ctx.exception.status_code, 409)
            self.assertEqual(untrusted_ctx.exception.detail, "Peer is untrusted: peer-untrusted")

            invisible_request = self._open_request(
                trigger="handoff_vs_handoff",
                claims=[
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "A",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                    {
                        "source_kind": "handoff",
                        "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "claimant_peer": "peer-gamma",
                        "claim_summary": "B",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": None,
                    },
                ],
            )
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as invisible_ctx:
                    coordination_reconciliation_open(req=invisible_request, auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(invisible_ctx.exception.status_code, 403)
            self.assertEqual(invisible_ctx.exception.detail, "Referenced source artifact not visible to caller")

            invalid_request = self._open_request(
                claims=[
                    {
                        "source_kind": "shared",
                        "source_id": "shared_invalidxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "claimant_peer": "peer-alpha",
                        "claim_summary": "A",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": 1,
                    },
                    {
                        "source_kind": "shared",
                        "source_id": "shared_invalidxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "B",
                        "epistemic_status": "frame_present",
                        "evidence_refs": [],
                        "observed_version": 1,
                    },
                ],
            )
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as invalid_ctx:
                    coordination_reconciliation_open(req=invalid_request, auth=_AuthStub(peer_id="peer-alpha", scopes={"admin:peers"}))
            self.assertEqual(invalid_ctx.exception.status_code, 400)
            self.assertEqual(
                invalid_ctx.exception.detail,
                "Referenced source artifact is invalid: shared_invalidxxxxxxxxxxxxxxxxxxxxxxxxx",
            )

    def test_open_rolls_back_artifact_when_commit_fails(self) -> None:
        """Open should remove the newly written reconciliation artifact when the commit fails."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-alpha", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-beta", trust_level="restricted")
            self._write_shared_artifact(repo_root, self._shared_payload(shared_id="shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", version=2))
            self._write_shared_artifact(repo_root, self._shared_payload(shared_id="shared_dddddddddddddddddddddddddddddddd", owner_peer="peer-beta", participant_peers=["peer-alpha"], version=3))
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, _GitManagerStub(fail_message_prefix="coordination: open"))):
                with self.assertRaises(HTTPException) as ctx:
                    coordination_reconciliation_open(req=self._open_request(), auth=_AuthStub(peer_id="peer-alpha"))

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertEqual(ctx.exception.detail, "Failed to commit reconciliation artifact")
            recon_dir = repo_root / "memory" / "coordination" / "reconciliations"
            self.assertEqual(sorted(recon_dir.glob("*.json")), [])

    def test_read_allows_owner_and_participant(self) -> None:
        """Read should allow the owner, listed participants, and admins."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = self._reconciliation_payload()
            self._write_reconciliation_artifact(repo_root, payload)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                owner = coordination_reconciliation_read(reconciliation_id=payload["reconciliation_id"], auth=_AuthStub(peer_id="peer-alpha"))
                participant = coordination_reconciliation_read(reconciliation_id=payload["reconciliation_id"], auth=_AuthStub(peer_id="peer-beta"))
                admin = coordination_reconciliation_read(
                    reconciliation_id=payload["reconciliation_id"],
                    auth=_AuthStub(peer_id="peer-other", scopes={"admin:peers"}),
                )

            self.assertEqual(owner["reconciliation"]["reconciliation_id"], payload["reconciliation_id"])
            self.assertEqual(participant["reconciliation"]["reconciliation_id"], payload["reconciliation_id"])
            self.assertEqual(admin["reconciliation"]["reconciliation_id"], payload["reconciliation_id"])

    def test_read_rejects_missing_unrelated_malformed_and_invalid_artifacts(self) -> None:
        """Read should fail deterministically for missing, unauthorized, malformed, or invalid stored artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = self._reconciliation_payload()
            self._write_reconciliation_artifact(repo_root, payload)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as missing_ctx:
                    coordination_reconciliation_read(
                        reconciliation_id="recon_ffffffffffffffffffffffffffffffff",
                        auth=_AuthStub(peer_id="peer-alpha"),
                    )
            self.assertEqual(missing_ctx.exception.status_code, 404)
            self.assertEqual(missing_ctx.exception.detail, "Reconciliation artifact not found")

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as hidden_ctx:
                    coordination_reconciliation_read(
                        reconciliation_id=payload["reconciliation_id"],
                        auth=_AuthStub(peer_id="peer-other"),
                    )
            self.assertEqual(hidden_ctx.exception.status_code, 403)
            self.assertEqual(hidden_ctx.exception.detail, "Reconciliation artifact not visible to caller")

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as bad_id_ctx:
                    coordination_reconciliation_read(reconciliation_id="recon_bad", auth=_AuthStub(peer_id="peer-alpha"))
            self.assertEqual(bad_id_ctx.exception.status_code, 400)
            self.assertEqual(bad_id_ctx.exception.detail, "Invalid reconciliation artifact id")

            invalid_json_path = repo_root / "memory" / "coordination" / "reconciliations" / "recon_11111111111111111111111111111111.json"
            invalid_json_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_json_path.write_text("{bad json", encoding="utf-8")
            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as invalid_json_ctx:
                    coordination_reconciliation_read(
                        reconciliation_id="recon_11111111111111111111111111111111",
                        auth=_AuthStub(peer_id="peer-alpha", scopes={"admin:peers"}),
                    )
            self.assertEqual(invalid_json_ctx.exception.status_code, 400)
            self.assertEqual(invalid_json_ctx.exception.detail, "Invalid reconciliation artifact JSON")

            invalid_struct = self._reconciliation_payload(reconciliation_id="recon_22222222222222222222222222222222")
            del invalid_struct["owner_peer"]
            self._write_reconciliation_artifact(repo_root, invalid_struct)
            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as invalid_struct_ctx:
                    coordination_reconciliation_read(
                        reconciliation_id="recon_22222222222222222222222222222222",
                        auth=_AuthStub(peer_id="peer-alpha", scopes={"admin:peers"}),
                    )
            self.assertEqual(invalid_struct_ctx.exception.status_code, 400)
            self.assertEqual(invalid_struct_ctx.exception.detail, "Invalid reconciliation artifact structure")

    def test_query_requires_scope_filters_and_self_identity_for_non_admin(self) -> None:
        """Query should enforce read scope, minimum filters, and non-admin identity constraints."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as no_scope_ctx:
                    coordination_reconciliations_query(
                        owner_peer="peer-alpha",
                        auth=_AuthStub(peer_id="peer-alpha", deny_scope="read:files"),
                    )
            self.assertEqual(no_scope_ctx.exception.status_code, 403)

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as no_filter_ctx:
                    coordination_reconciliations_query(auth=_AuthStub(peer_id="peer-admin", scopes={"admin:peers"}))
            self.assertEqual(no_filter_ctx.exception.status_code, 400)
            self.assertIn("At least one reconciliation query filter is required", str(no_filter_ctx.exception.detail))

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as task_only_ctx:
                    coordination_reconciliations_query(task_id="task-123", auth=_AuthStub(peer_id="peer-beta"))
            self.assertEqual(task_only_ctx.exception.status_code, 403)
            self.assertEqual(task_only_ctx.exception.detail, "Non-admin callers may query only their own reconciliation identity")

            with patch("app.main._services", return_value=(settings, None)):
                with self.assertRaises(HTTPException) as mixed_ctx:
                    coordination_reconciliations_query(
                        owner_peer="peer-alpha",
                        claimant_peer="peer-beta",
                        auth=_AuthStub(peer_id="peer-beta"),
                    )
            self.assertEqual(mixed_ctx.exception.status_code, 403)
            self.assertEqual(mixed_ctx.exception.detail, "Non-admin callers may query only their own reconciliation identity")

    def test_query_filters_by_claimant_visibility_sorting_pagination_and_warnings(self) -> None:
        """Query should filter across claim records, sort deterministically, paginate, and skip invalid artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            newest = self._reconciliation_payload(
                reconciliation_id="recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                updated_at="2026-03-18T12:00:00Z",
                owner_peer="peer-alpha",
                opened_by="peer-alpha",
                claims=[
                    {
                        "source_kind": "shared",
                        "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "claimant_peer": "peer-alpha",
                        "claim_summary": "Owner still sees the freeze.",
                        "epistemic_status": "frame_present",
                        "evidence_refs": ["msg_1"],
                        "observed_version": 2,
                    },
                    {
                        "source_kind": "shared",
                        "source_id": "shared_dddddddddddddddddddddddddddddddd",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "Peer beta sees the freeze lifted.",
                        "epistemic_status": "frame_status_unknown",
                        "evidence_refs": ["msg_2"],
                        "observed_version": 3,
                    },
                ],
            )
            same_time_a = self._reconciliation_payload(
                reconciliation_id="recon_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                updated_at="2026-03-17T12:00:00Z",
                owner_peer="peer-alpha",
                opened_by="peer-alpha",
            )
            same_time_b = self._reconciliation_payload(
                reconciliation_id="recon_cccccccccccccccccccccccccccccccc",
                updated_at="2026-03-17T12:00:00Z",
                owner_peer="peer-alpha",
                opened_by="peer-alpha",
            )
            filtered = self._reconciliation_payload(
                reconciliation_id="recon_dddddddddddddddddddddddddddddddd",
                updated_at="2026-03-19T12:00:00Z",
                task_id="task-other",
                owner_peer="peer-alpha",
                opened_by="peer-alpha",
                claims=[
                    {
                        "source_kind": "shared",
                        "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "claimant_peer": "peer-gamma",
                        "claim_summary": "Peer gamma sees a different task scope.",
                        "epistemic_status": "frame_present",
                        "evidence_refs": ["msg_3"],
                        "observed_version": 2,
                    },
                    {
                        "source_kind": "shared",
                        "source_id": "shared_dddddddddddddddddddddddddddddddd",
                        "claimant_peer": "peer-beta",
                        "claim_summary": "Peer beta sees the same alternate scope.",
                        "epistemic_status": "frame_status_unknown",
                        "evidence_refs": ["msg_4"],
                        "observed_version": 3,
                    },
                ],
            )
            self._write_reconciliation_artifact(repo_root, newest)
            self._write_reconciliation_artifact(repo_root, same_time_a)
            self._write_reconciliation_artifact(repo_root, same_time_b)
            self._write_reconciliation_artifact(repo_root, filtered)
            invalid_path = repo_root / "memory" / "coordination" / "reconciliations" / "recon_invalidffffffffffffffffffffffff.json"
            invalid_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_path.write_text("{bad json", encoding="utf-8")
            settings = self._settings(repo_root)

            with patch("app.main._services", return_value=(settings, None)):
                owner_claimant = coordination_reconciliations_query(
                    claimant_peer="peer-alpha",
                    auth=_AuthStub(peer_id="peer-alpha"),
                )
                paged = coordination_reconciliations_query(
                    owner_peer="peer-alpha",
                    task_id="task-123",
                    offset=1,
                    limit=2,
                    auth=_AuthStub(peer_id="peer-alpha"),
                )

            self.assertEqual(owner_claimant["count"], 3)
            self.assertEqual(owner_claimant["reconciliations"][0]["reconciliation_id"], newest["reconciliation_id"])
            self.assertIn("coordination_reconciliation_artifact_skipped_invalid", owner_claimant["warnings"])
            self.assertEqual(paged["total_matches"], 3)
            self.assertEqual(paged["count"], 2)
            self.assertEqual(
                [item["reconciliation_id"] for item in paged["reconciliations"]],
                [
                    "recon_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "recon_cccccccccccccccccccccccccccccccc",
                ],
            )


if __name__ == "__main__":
    unittest.main()
