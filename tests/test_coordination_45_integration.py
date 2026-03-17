"""Tests for Issue #45 Phase 2 — index-integrated coordination query services.

Validates that query services use the SQLite index when available,
fall back to full scan when not, emit threshold warnings on large
directories, and handle artifacts disappearing between index and load.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from app.config import Settings
from app.coordination.handoff_service import (
    SCAN_THRESHOLD_WARNING,
    handoffs_query_service,
)
from app.coordination.query_index import (
    CoordinationQueryIndex,
    set_coordination_index,
)
from app.coordination.reconciliation_service import (
    reconciliation_query_service,
)
from app.coordination.shared_service import (
    shared_query_service,
)
from app.models import (
    CoordinationHandoffQueryRequest,
    CoordinationReconciliationQueryRequest,
    CoordinationSharedQueryRequest,
)
from app.storage import canonical_json
from tests.helpers import AllowAllAuthStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub with admin scope for coordination integration tests."""

    def __init__(self, *, peer_id: str = "peer-test", admin: bool = False) -> None:
        super().__init__(peer_id=peer_id)
        self.scopes: set[str] = {"admin:peers"} if admin else set()


def _noop_rate_limit(*_a: Any, **_kw: Any) -> None:
    pass


def _noop_audit(*_a: Any, **_kw: Any) -> None:
    pass


def _settings(repo_root: Path, *, threshold: int = 5000) -> Settings:
    """Build minimal settings for integration tests."""
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
        coordination_query_scan_threshold=threshold,
    )


# ---------------------------------------------------------------------------
# Handoff fixtures
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


def _write_handoff(repo_root: Path, overrides: dict) -> None:
    """Write one handoff artifact to disk."""
    artifact = {**_HANDOFF_BASE, **overrides}
    path = repo_root / "memory" / "coordination" / "handoffs" / f"{artifact['handoff_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(artifact), encoding="utf-8")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

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


def _write_shared(repo_root: Path, overrides: dict) -> None:
    """Write one shared artifact to disk."""
    artifact = {**_SHARED_BASE, **overrides}
    path = repo_root / "memory" / "coordination" / "shared" / f"{artifact['shared_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(artifact), encoding="utf-8")


# ---------------------------------------------------------------------------
# Reconciliation fixtures
# ---------------------------------------------------------------------------

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


def _write_recon(repo_root: Path, overrides: dict) -> None:
    """Write one reconciliation artifact to disk."""
    artifact = {**_RECON_BASE, **overrides}
    path = repo_root / "memory" / "coordination" / "reconciliations" / f"{artifact['reconciliation_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(artifact), encoding="utf-8")


# ===================================================================
# Test cases
# ===================================================================


class TestHandoffQueryWithIndex(unittest.TestCase):
    """Validate handoff query service using the SQLite sidecar index."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_index_path_returns_correct_results(self) -> None:
        """When the index is populated, query should use it and return correct artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_handoff(repo_root, {
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            })
            _write_handoff(repo_root, {
                "handoff_id": "handoff_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "created_at": "2026-03-02T10:00:00Z",
            })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_handoffs(repo_root / "memory" / "coordination" / "handoffs")
            set_coordination_index(idx)

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 2)
            self.assertEqual(result["count"], 2)
            # Newest first
            self.assertEqual(
                result["handoffs"][0]["handoff_id"],
                "handoff_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )

    def test_index_pagination(self) -> None:
        """Index path should respect offset/limit for pagination."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            for i in range(5):
                _write_handoff(repo_root, {
                    "handoff_id": f"handoff_{i:032x}",
                    "created_at": f"2026-03-{i + 1:02d}T10:00:00Z",
                })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_handoffs(repo_root / "memory" / "coordination" / "handoffs")
            set_coordination_index(idx)

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob", offset=1, limit=2),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertEqual(result["total_matches"], 5)
            self.assertEqual(result["count"], 2)

    def test_deleted_artifact_skipped_gracefully(self) -> None:
        """If an artifact file disappears after indexing, it should be skipped."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_handoff(repo_root, {
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_handoffs(repo_root / "memory" / "coordination" / "handoffs")
            set_coordination_index(idx)

            # Delete the file after indexing
            (repo_root / "memory" / "coordination" / "handoffs" / "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json").unlink()

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            # Index says 1 match but file is gone — should return 0 artifacts
            self.assertTrue(result["ok"])
            self.assertEqual(result["count"], 0)


class TestHandoffQueryFallback(unittest.TestCase):
    """Validate handoff query fallback path when index is unavailable."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_fallback_when_index_unavailable(self) -> None:
        """Without an index, query should fall back to full scan and still work."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_handoff(repo_root, {
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            })

            # No index set — fallback path
            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 1)
            self.assertEqual(result["count"], 1)

    def test_scan_threshold_warning(self) -> None:
        """Fallback path should emit warning when file count exceeds threshold."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            hdir = repo_root / "memory" / "coordination" / "handoffs"
            hdir.mkdir(parents=True)

            # Create 3 files with threshold=2
            for i in range(3):
                _write_handoff(repo_root, {
                    "handoff_id": f"handoff_{i:032x}",
                    "created_at": f"2026-03-{i + 1:02d}T10:00:00Z",
                })

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root, threshold=2),
                audit=_noop_audit,
            )

            self.assertIn(SCAN_THRESHOLD_WARNING, result["warnings"])

    def test_no_threshold_warning_when_below(self) -> None:
        """No warning should appear when file count is below threshold."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_handoff(repo_root, {
                "handoff_id": "handoff_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
            })

            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root, threshold=5000),
                audit=_noop_audit,
            )

            self.assertNotIn(SCAN_THRESHOLD_WARNING, result["warnings"])

    def test_no_threshold_warning_when_index_active(self) -> None:
        """Threshold warning should not appear when index is active (no scan)."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            for i in range(5):
                _write_handoff(repo_root, {
                    "handoff_id": f"handoff_{i:032x}",
                    "created_at": f"2026-03-{i + 1:02d}T10:00:00Z",
                })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_handoffs(repo_root / "memory" / "coordination" / "handoffs")
            set_coordination_index(idx)

            # Threshold=1 would trigger on fallback, but index path skips scan
            result = handoffs_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationHandoffQueryRequest(recipient_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root, threshold=1),
                audit=_noop_audit,
            )

            self.assertNotIn(SCAN_THRESHOLD_WARNING, result["warnings"])


class TestSharedQueryWithIndex(unittest.TestCase):
    """Validate shared query service using the SQLite sidecar index."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_index_path_returns_correct_results(self) -> None:
        """Shared query via index should return matching artifacts sorted correctly."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_shared(repo_root, {
                "shared_id": "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            })
            _write_shared(repo_root, {
                "shared_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "created_at": "2026-03-02T10:00:00Z",
                "updated_at": "2026-03-02T10:00:00Z",
            })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_shared(repo_root / "memory" / "coordination" / "shared")
            set_coordination_index(idx)

            result = shared_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationSharedQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 2)
            # Newest first
            self.assertEqual(
                result["shared_artifacts"][0]["shared_id"],
                "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )

    def test_fallback_when_no_index(self) -> None:
        """Shared query should fall back to scan when index is unavailable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_shared(repo_root, {
                "shared_id": "shared_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            })

            result = shared_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationSharedQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 1)


class TestReconciliationQueryWithIndex(unittest.TestCase):
    """Validate reconciliation query service using the SQLite sidecar index."""

    def tearDown(self) -> None:
        set_coordination_index(None)  # type: ignore[arg-type]

    def test_index_path_returns_correct_results(self) -> None:
        """Reconciliation query via index should return matching artifacts."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_recon(repo_root, {
                "reconciliation_id": "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_reconciliations(repo_root / "memory" / "coordination" / "reconciliations")
            set_coordination_index(idx)

            result = reconciliation_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationReconciliationQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 1)
            self.assertEqual(result["count"], 1)

    def test_claimant_filter_via_index(self) -> None:
        """Reconciliation query should filter by claimant_peer via the junction table."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_recon(repo_root, {
                "reconciliation_id": "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            })

            idx = CoordinationQueryIndex(Path(td) / ".query_index.db")
            idx.rebuild_reconciliations(repo_root / "memory" / "coordination" / "reconciliations")
            set_coordination_index(idx)

            # Query by claimant bob (from fixture claims)
            result = reconciliation_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="bob", admin=True),
                req=CoordinationReconciliationQueryRequest(claimant_peer="bob"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )
            self.assertEqual(result["total_matches"], 1)

            # Query by non-existent claimant
            result = reconciliation_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="charlie", admin=True),
                req=CoordinationReconciliationQueryRequest(claimant_peer="charlie"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )
            self.assertEqual(result["total_matches"], 0)

    def test_fallback_when_no_index(self) -> None:
        """Reconciliation query should fall back to scan without index."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_recon(repo_root, {
                "reconciliation_id": "recon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "created_at": "2026-03-01T10:00:00Z",
                "updated_at": "2026-03-01T10:00:00Z",
            })

            result = reconciliation_query_service(
                repo_root=repo_root,
                auth=_AuthStub(peer_id="alice", admin=True),
                req=CoordinationReconciliationQueryRequest(owner_peer="alice"),
                enforce_rate_limit=_noop_rate_limit,
                settings=_settings(repo_root),
                audit=_noop_audit,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["total_matches"], 1)


if __name__ == "__main__":
    unittest.main()
