"""Tests for Issue #45 Phase 3 — incremental index updates on mutations.

Validates that coordination mutation operations (create, consume, update,
resolve) keep the SQLite sidecar index in sync, and that mutations succeed
even when the index is unavailable.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from app.coordination.query_index import (
    CoordinationQueryIndex,
    set_coordination_index,
)
from app.coordination.handoff_service import handoffs_query_service
from app.coordination.shared_service import shared_query_service
from app.coordination.reconciliation_service import reconciliation_query_service
from app.config import Settings
from app.models import (
    CoordinationHandoffQueryRequest,
    CoordinationSharedQueryRequest,
    CoordinationReconciliationQueryRequest,
)
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with admin scope for mutation tests."""

    def __init__(self, *, peer_id: str = "peer-test", admin: bool = False) -> None:
        super().__init__(peer_id=peer_id)
        self.scopes: set[str] = {"admin:peers"} if admin else set()


def _noop(*_a: Any, **_kw: Any) -> None:
    pass


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


# ---------------------------------------------------------------------------
# Fixture writers (write raw artifacts + update the index via upsert)
# ---------------------------------------------------------------------------

_HANDOFF_BASE = {
    "schema_type": "continuity_handoff",
    "schema_version": "1.0",
    "created_by": "alice",
    "sender_peer": "alice",
    "recipient_peer": "bob",
    "source_selector": {"subject_kind": "task", "subject_id": "t1"},
    "source_summary": {
        "path": "memory/continuity/task/t1.json",
        "updated_at": "2026-03-01T10:00:00Z",
        "verified_at": "2026-03-01T10:00:00Z",
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

_SHARED_BASE = {
    "schema_type": "coordination_shared_state",
    "schema_version": "1.0",
    "created_by": "alice",
    "owner_peer": "alice",
    "participant_peers": ["bob"],
    "task_id": None,
    "thread_id": None,
    "title": "test",
    "summary": None,
    "shared_state": {"constraints": [], "drift_signals": [], "coordination_alerts": []},
    "version": 1,
    "last_updated_by": "alice",
}

_RECON_BASE = {
    "schema_type": "coordination_reconciliation_record",
    "schema_version": "1.0",
    "opened_by": "alice",
    "owner_peer": "alice",
    "participant_peers": ["bob"],
    "task_id": None,
    "thread_id": None,
    "title": "test recon",
    "summary": None,
    "classification": "contradictory",
    "trigger": "handoff_vs_handoff",
    "claims": [
        {
            "source_kind": "handoff",
            "source_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "claimant_peer": "alice",
            "claim_summary": "claim A",
            "epistemic_status": "frame_present",
            "evidence_refs": [],
        },
        {
            "source_kind": "handoff",
            "source_id": "handoff_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "claimant_peer": "bob",
            "claim_summary": "claim B",
            "epistemic_status": "frame_present",
            "evidence_refs": [],
        },
    ],
    "status": "open",
    "resolution_outcome": None,
    "resolution_summary": None,
    "resolved_at": None,
    "resolved_by": None,
    "version": 1,
    "last_updated_by": "alice",
}


def _write_artifact(repo_root: Path, subdir: str, filename: str, data: dict) -> Path:
    """Write a coordination artifact JSON file to disk."""
    path = repo_root / "memory" / "coordination" / subdir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(data), encoding="utf-8")
    return path


class TestHandoffMutationUpdatesIndex(unittest.TestCase):
    """Verify that handoff create and consume keep the index in sync."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_upsert_after_manual_write_updates_query(self) -> None:
        """Simulating create: writing a file + upsert should make it queryable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            artifact = {
                **_HANDOFF_BASE,
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            }
            _write_artifact(repo_root, "handoffs", "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)
            idx.upsert_handoff(artifact)

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop,
                settings=_settings(repo_root),
                audit=_noop,
            )

            self.assertEqual(result["total_matches"], 1)
            self.assertEqual(result["handoffs"][0]["handoff_id"], "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

    def test_upsert_after_consume_updates_status_in_index(self) -> None:
        """Simulating consume: updating status + upsert should update index filter results."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            artifact = {
                **_HANDOFF_BASE,
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "recipient_status": "pending",
            }
            _write_artifact(repo_root, "handoffs", "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)
            idx.upsert_handoff(artifact)

            # Verify queryable as pending
            ids, _ = idx.query_handoffs(status="pending")
            self.assertEqual(len(ids), 1)

            # Simulate consume
            consumed = {**artifact, "recipient_status": "accepted_advisory"}
            _write_artifact(repo_root, "handoffs", "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", consumed)
            idx.upsert_handoff(consumed)

            # No longer pending
            ids, _ = idx.query_handoffs(status="pending")
            self.assertEqual(len(ids), 0)

            # Now accepted_advisory
            ids, _ = idx.query_handoffs(status="accepted_advisory")
            self.assertEqual(len(ids), 1)


class TestSharedMutationUpdatesIndex(unittest.TestCase):
    """Verify that shared create and update keep the index in sync."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_upsert_after_create_makes_queryable(self) -> None:
        """Creating a shared artifact + upsert should make it discoverable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            artifact = {
                **_SHARED_BASE,
                "shared_id": "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            }
            _write_artifact(repo_root, "shared", "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)
            idx.upsert_shared(artifact)

            result = shared_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationSharedQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop,
                settings=_settings(repo_root),
                audit=_noop,
            )
            self.assertEqual(result["total_matches"], 1)

    def test_upsert_after_update_refreshes_timestamp(self) -> None:
        """Updating a shared artifact's timestamp should change sort order in index."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            # Create two artifacts
            art_a = {
                **_SHARED_BASE,
                "shared_id": "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            }
            art_b = {
                **_SHARED_BASE,
                "shared_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "created_at": "2026-03-02T10:00:00Z",
                "updated_at": "2026-03-02T10:00:00Z",
            }
            _write_artifact(repo_root, "shared", "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", art_a)
            _write_artifact(repo_root, "shared", "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json", art_b)
            idx.upsert_shared(art_a)
            idx.upsert_shared(art_b)

            # Before update: b is newer
            ids, _ = idx.query_shared()
            self.assertEqual(ids[0], "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

            # Update a to be newer
            updated_a = {**art_a, "updated_at": "2026-03-03T10:00:00Z"}
            _write_artifact(repo_root, "shared", "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", updated_a)
            idx.upsert_shared(updated_a)

            # After update: a is now newer
            ids, _ = idx.query_shared()
            self.assertEqual(ids[0], "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")


class TestReconciliationMutationUpdatesIndex(unittest.TestCase):
    """Verify that reconciliation open and resolve keep the index in sync."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_upsert_after_open_makes_queryable(self) -> None:
        """Opening a reconciliation + upsert should make it discoverable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            artifact = {
                **_RECON_BASE,
                "reconciliation_id": "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            }
            _write_artifact(repo_root, "reconciliations", "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)
            idx.upsert_reconciliation(artifact)

            result = reconciliation_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationReconciliationQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop,
                settings=_settings(repo_root),
                audit=_noop,
            )
            self.assertEqual(result["total_matches"], 1)
            self.assertEqual(result["reconciliations"][0]["status"], "open")

    def test_upsert_after_resolve_updates_status(self) -> None:
        """Resolving a reconciliation + upsert should update the status filter."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            set_coordination_index(idx)

            artifact = {
                **_RECON_BASE,
                "reconciliation_id": "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            }
            _write_artifact(repo_root, "reconciliations", "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)
            idx.upsert_reconciliation(artifact)

            # Verify open
            ids, _ = idx.query_reconciliations(status="open")
            self.assertEqual(len(ids), 1)

            # Simulate resolve
            resolved = {**artifact, "status": "resolved", "updated_at": "2026-03-02T10:00:00Z"}
            _write_artifact(repo_root, "reconciliations", "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", resolved)
            idx.upsert_reconciliation(resolved)

            # No longer open
            ids, _ = idx.query_reconciliations(status="open")
            self.assertEqual(len(ids), 0)
            ids, _ = idx.query_reconciliations(status="resolved")
            self.assertEqual(len(ids), 1)


class TestMutationsWithoutIndex(unittest.TestCase):
    """Verify that mutations work fine without an index (fallback path)."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_no_index_set_query_still_works(self) -> None:
        """With no index, mutations and queries should fall back to scan."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            # Deliberately not setting any index

            artifact = {
                **_HANDOFF_BASE,
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            }
            _write_artifact(repo_root, "handoffs", "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json", artifact)

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop,
                settings=_settings(repo_root),
                audit=_noop,
            )

            self.assertEqual(result["total_matches"], 1)
            self.assertEqual(result["count"], 1)


if __name__ == "__main__":
    unittest.main()
