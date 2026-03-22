"""Tests for Stage D cold-store/rehydrate crash recovery across all lifecycle services.

Each test simulates a crash state by creating the "both files exist" layout
that results from a process kill between mutation steps, then verifies that
the service recovers gracefully instead of bricking the unit.
"""

from __future__ import annotations

import gzip
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

from app.artifact_lifecycle.service import (
    HANDOFFS_DIR_REL,
    HANDOFFS_HISTORY_DIR_REL,
    artifact_history_cold_store_service,
    artifact_history_cold_rehydrate_service,
)
from app.models import (
    ArtifactHistoryColdRehydrateRequest,
    ArtifactHistoryColdStoreRequest,
)
from app.registry_lifecycle.service import (
    DELIVERY_HISTORY_DIR_REL,
    DELIVERY_STATE_REL,
    DELIVERY_STUB_DIR_REL,
    registry_history_cold_rehydrate_service,
)
from app.storage import safe_path, write_text_file, write_bytes_file
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _write_json(repo: Path, rel: str, obj: dict) -> Path:
    path = safe_path(repo, rel)
    write_text_file(path, json.dumps(obj, ensure_ascii=False, indent=2))
    return path


def _noop_audit(*_args, **_kwargs):
    pass


# ===================================================================
# Artifact cold-store crash recovery (Fix 1)
# ===================================================================


class TestArtifactColdStoreCrashRecovery(unittest.TestCase):
    """Crash recovery for artifact_history_cold_store_service."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.gm = SimpleGitManagerStub(self.repo)
        self.auth = AllowAllAuthStub(peer_id="peer-test")

    def _create_handoff_unit(self, *, stub_payload_path: str | None = None):
        """Create a handoff history unit with payload and stub."""
        payload_rel = f"{HANDOFFS_HISTORY_DIR_REL}/handoff__20260319T120000Z__0001.json"
        stub_rel = f"{HANDOFFS_HISTORY_DIR_REL}/index/handoff__20260319T120000Z__0001.json"
        cold_rel = f"{HANDOFFS_HISTORY_DIR_REL}/cold/handoff__20260319T120000Z__0001.json.gz"
        payload = {
            "schema_type": "handoff_history_unit",
            "schema_version": "1.0",
            "family": "handoff",
            "history_id": "handoff__20260319T120000Z__0001",
            "artifact_id": "handoff_aaaa",
            "source_path": f"{HANDOFFS_DIR_REL}/handoff_aaaa.json",
            "cut_at": _now().isoformat(),
            "artifact": {
                "handoff_id": "handoff_aaaa",
                "sender_peer": "peer-test",
                "recipient_peer": "peer-beta",
                "recipient_status": "accepted_advisory",
                "task_id": "task-1",
                "thread_id": "thread-1",
                "created_at": "2026-03-01T10:00:00+00:00",
                "updated_at": "2026-03-02T10:00:00+00:00",
            },
            "summary": {
                "sender_peer": "peer-test",
                "recipient_peer": "peer-beta",
                "recipient_status": "accepted_advisory",
                "task_id": "task-1",
                "thread_id": "thread-1",
                "created_at": "2026-03-01T10:00:00+00:00",
                "terminal_at": "2026-03-02T10:00:00+00:00",
            },
        }
        _write_json(self.repo, payload_rel, payload)
        stub = {
            "schema_type": "artifact_history_stub",
            "schema_version": "1.0",
            "family": "handoff",
            "history_id": "handoff__20260319T120000Z__0001",
            "payload_path": stub_payload_path or payload_rel,
            "created_at": _now().isoformat(),
            "source_path": f"{HANDOFFS_DIR_REL}/handoff_aaaa.json",
            "summary": payload["summary"],
        }
        _write_json(self.repo, stub_rel, stub)
        return payload_rel, stub_rel, cold_rel, payload

    def test_crash_recovery_stub_points_cold(self):
        """Crash after stub mutation: stub points to cold, both files exist.

        Recovery should delete orphaned hot, return success.
        """
        payload_rel, stub_rel, cold_rel, payload = self._create_handoff_unit(
            stub_payload_path=f"{HANDOFFS_HISTORY_DIR_REL}/cold/handoff__20260319T120000Z__0001.json.gz",
        )
        # Simulate: cold gzip already written
        cold_path = safe_path(self.repo, cold_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(gzip.compress(
            json.dumps(payload).encode(), mtime=0,
        ))

        result = artifact_history_cold_store_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ArtifactHistoryColdStoreRequest(source_payload_path=payload_rel),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertFalse(safe_path(self.repo, payload_rel).exists())
        self.assertTrue(cold_path.exists())
        warnings = [w["code"] for w in result.get("warnings", [])]
        self.assertIn("artifact_history_cold_store_crash_recovery", warnings)

    def test_crash_recovery_stub_points_hot(self):
        """Crash before stub mutation: stub still points to hot, both files exist.

        Recovery should delete orphaned cold and proceed with a fresh cold-store.
        """
        payload_rel, stub_rel, cold_rel, _payload = self._create_handoff_unit()
        # Simulate: orphaned cold file from a prior partial cold-store
        cold_path = safe_path(self.repo, cold_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"orphaned-partial-data")

        result = artifact_history_cold_store_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ArtifactHistoryColdStoreRequest(source_payload_path=payload_rel),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["artifact_state"], "cold")
        # Hot file removed, cold file now contains valid gzip
        self.assertFalse(safe_path(self.repo, payload_rel).exists())
        decompressed = gzip.decompress(cold_path.read_bytes())
        self.assertIn(b"handoff__20260319T120000Z__0001", decompressed)

    def test_crash_recovery_unreadable_stub(self):
        """Crash with corrupted stub: conservative default deletes cold, proceeds."""
        payload_rel, stub_rel, cold_rel, _payload = self._create_handoff_unit()
        # Corrupt the stub
        safe_path(self.repo, stub_rel).write_text("NOT JSON", encoding="utf-8")
        # Create orphaned cold file
        cold_path = safe_path(self.repo, cold_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"orphaned")

        # Should not crash — cold deleted, fresh cold-store proceeds
        # (will fail on stub validation during normal flow, but that's
        # expected behavior — the stub is genuinely corrupt)
        with self.assertRaises(HTTPException) as ctx:
            artifact_history_cold_store_service(
                repo_root=self.repo, gm=self.gm, auth=self.auth,
                req=ArtifactHistoryColdStoreRequest(source_payload_path=payload_rel),
                audit=_noop_audit,
            )
        # Should fail during normal flow stub load, not during crash recovery
        self.assertIn("400", str(ctx.exception.status_code) + str(ctx.exception.detail))


# ===================================================================
# Artifact cold-rehydrate crash recovery (Fix 2)
# ===================================================================


class TestArtifactColdRehydrateCrashRecovery(unittest.TestCase):
    """Crash recovery for artifact_history_cold_rehydrate_service."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.gm = SimpleGitManagerStub(self.repo)
        self.auth = AllowAllAuthStub(peer_id="peer-test")

    def _create_cold_stored_unit(self, *, stub_payload_path: str | None = None):
        """Create a cold-stored artifact history unit."""
        payload_rel = f"{HANDOFFS_HISTORY_DIR_REL}/handoff__20260319T120000Z__0001.json"
        stub_rel = f"{HANDOFFS_HISTORY_DIR_REL}/index/handoff__20260319T120000Z__0001.json"
        cold_rel = f"{HANDOFFS_HISTORY_DIR_REL}/cold/handoff__20260319T120000Z__0001.json.gz"
        payload = {
            "schema_type": "handoff_history_unit",
            "schema_version": "1.0",
            "family": "handoff",
            "history_id": "handoff__20260319T120000Z__0001",
            "artifact_id": "handoff_aaaa",
            "source_path": f"{HANDOFFS_DIR_REL}/handoff_aaaa.json",
            "cut_at": _now().isoformat(),
            "artifact": {
                "handoff_id": "handoff_aaaa",
                "sender_peer": "peer-test",
                "recipient_peer": "peer-beta",
                "recipient_status": "accepted_advisory",
                "task_id": "task-1",
                "thread_id": "thread-1",
                "created_at": "2026-03-01T10:00:00+00:00",
                "updated_at": "2026-03-02T10:00:00+00:00",
            },
            "summary": {
                "sender_peer": "peer-test",
                "recipient_peer": "peer-beta",
                "recipient_status": "accepted_advisory",
                "task_id": "task-1",
                "thread_id": "thread-1",
                "created_at": "2026-03-01T10:00:00+00:00",
                "terminal_at": "2026-03-02T10:00:00+00:00",
            },
        }
        # Write cold gzip
        cold_path = safe_path(self.repo, cold_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(cold_path, gzip.compress(
            json.dumps(payload).encode(), mtime=0,
        ))
        # Write stub pointing to the specified path
        effective_payload_path = stub_payload_path or cold_rel
        stub = {
            "schema_type": "artifact_history_stub",
            "schema_version": "1.0",
            "family": "handoff",
            "history_id": "handoff__20260319T120000Z__0001",
            "payload_path": effective_payload_path,
            "created_at": _now().isoformat(),
            "source_path": f"{HANDOFFS_DIR_REL}/handoff_aaaa.json",
            "summary": payload["summary"],
        }
        _write_json(self.repo, stub_rel, stub)
        return payload_rel, stub_rel, cold_rel, payload

    def test_crash_recovery_stub_points_hot(self):
        """Crash after stub mutation to hot: both exist, stub points to hot.

        Recovery should delete orphaned cold and return success.
        """
        payload_rel, stub_rel, cold_rel, payload = self._create_cold_stored_unit(
            stub_payload_path=f"{HANDOFFS_HISTORY_DIR_REL}/handoff__20260319T120000Z__0001.json",
        )
        # Simulate: hot payload written by prior rehydrate before crash
        hot_path = safe_path(self.repo, payload_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())

        result = artifact_history_cold_rehydrate_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ArtifactHistoryColdRehydrateRequest(source_payload_path=payload_rel),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(hot_path.exists())
        self.assertFalse(safe_path(self.repo, cold_rel).exists())
        warnings = [w["code"] for w in result.get("warnings", [])]
        self.assertIn("artifact_history_cold_rehydrate_crash_recovery", warnings)

    def test_crash_recovery_stub_points_cold(self):
        """Crash before stub mutation: stub still points to cold, both exist.

        Recovery should delete orphaned hot and retry normal rehydrate.
        """
        payload_rel, stub_rel, cold_rel, payload = self._create_cold_stored_unit()
        # Simulate: hot payload written by prior partial rehydrate
        hot_path = safe_path(self.repo, payload_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())

        result = artifact_history_cold_rehydrate_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ArtifactHistoryColdRehydrateRequest(source_payload_path=payload_rel),
            audit=_noop_audit,
        )

        # Should recover: delete orphan hot, redo rehydrate
        self.assertTrue(result["ok"])
        self.assertEqual(result["artifact_state"], "hot")
        self.assertTrue(hot_path.exists())

    def test_crash_recovery_unreadable_stub_raises_before_lock(self):
        """Corrupted stub causes pre-lock load failure (400), not a brick."""
        payload_rel, stub_rel, cold_rel, payload = self._create_cold_stored_unit()
        # Simulate: hot exists from partial rehydrate
        hot_path = safe_path(self.repo, payload_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())
        # Corrupt the stub — pre-lock load will fail before reaching crash recovery
        safe_path(self.repo, stub_rel).write_text("NOT JSON", encoding="utf-8")

        with self.assertRaises(HTTPException) as ctx:
            artifact_history_cold_rehydrate_service(
                repo_root=self.repo, gm=self.gm, auth=self.auth,
                req=ArtifactHistoryColdRehydrateRequest(source_payload_path=payload_rel),
                audit=_noop_audit,
            )
        # Fails during pre-lock stub load, not during crash recovery
        self.assertEqual(ctx.exception.status_code, 400)


# ===================================================================
# Registry cold-rehydrate crash recovery (Fix 3)
# ===================================================================


def _create_registry_shard_and_stub(repo, family, shard_id, summary, *,
                                     schema_type="delivery_history_shard",
                                     source_head_path=DELIVERY_STATE_REL):
    history_dir = DELIVERY_HISTORY_DIR_REL
    stub_dir = DELIVERY_STUB_DIR_REL
    shard_payload = {
        "schema_type": schema_type,
        "schema_version": "1.0",
        "family": family,
        "shard_id": shard_id,
        "summary": summary,
    }
    shard_rel = f"{history_dir}/{shard_id}.json"
    stub_rel = f"{stub_dir}/{shard_id}.json"
    _write_json(repo, shard_rel, shard_payload)
    _write_json(repo, stub_rel, {
        "schema_type": "registry_history_stub",
        "schema_version": "1.0",
        "family": family,
        "shard_id": shard_id,
        "payload_path": shard_rel,
        "created_at": _now().isoformat(),
        "source_head_path": source_head_path,
        "summary": summary,
    })
    return shard_rel, stub_rel


class TestRegistryColdRehydrateCrashRecovery(unittest.TestCase):
    """Crash recovery for registry_history_cold_rehydrate_service."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.gm = SimpleGitManagerStub(self.repo)
        self.now = _now()

    def _create_cold_stored_shard(self, *, stub_payload_path: str | None = None):
        shard_id = "delivery__20260101T000000Z__0001"
        summary = {
            "record_count": 1,
            "oldest_retention_timestamp": "2025-11-01T00:00:00+00:00",
            "newest_retention_timestamp": "2025-12-01T00:00:00+00:00",
        }
        shard_payload = {
            "schema_type": "delivery_history_shard",
            "schema_version": "1.0",
            "family": "delivery",
            "shard_id": shard_id,
            "summary": summary,
        }
        shard_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"
        cold_rel = f"{DELIVERY_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        stub_rel = f"{DELIVERY_STUB_DIR_REL}/{shard_id}.json"

        # Write cold gzip
        cold_path = safe_path(self.repo, cold_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(cold_path, gzip.compress(
            json.dumps(shard_payload).encode(), mtime=0,
        ))
        # Write stub
        effective_payload_path = stub_payload_path or cold_rel
        _write_json(self.repo, stub_rel, {
            "schema_type": "registry_history_stub",
            "schema_version": "1.0",
            "family": "delivery",
            "shard_id": shard_id,
            "payload_path": effective_payload_path,
            "created_at": self.now.isoformat(),
            "source_head_path": DELIVERY_STATE_REL,
            "summary": summary,
        })
        return shard_rel, stub_rel, cold_rel, shard_payload

    def test_crash_recovery_stub_points_hot(self):
        """Stub already points to hot (rehydrate completed), cold is orphan."""
        shard_rel, stub_rel, cold_rel, payload = self._create_cold_stored_shard(
            stub_payload_path=f"{DELIVERY_HISTORY_DIR_REL}/delivery__20260101T000000Z__0001.json",
        )
        # Simulate: hot shard written by prior rehydrate
        hot_path = safe_path(self.repo, shard_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())

        result = registry_history_cold_rehydrate_service(
            repo_root=self.repo, gm=self.gm, auth=None,
            req=type("Req", (), {"source_payload_path": shard_rel, "cold_stub_path": None})(),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(hot_path.exists())
        self.assertFalse(safe_path(self.repo, cold_rel).exists())
        warnings = [w["code"] for w in result.get("warnings", [])]
        self.assertIn("registry_history_cold_rehydrate_crash_recovery", warnings)

    def test_crash_recovery_stub_points_cold(self):
        """Stub still points to cold (rehydrate incomplete), hot is orphan."""
        shard_rel, stub_rel, cold_rel, payload = self._create_cold_stored_shard()
        # Simulate: hot shard written by prior partial rehydrate
        hot_path = safe_path(self.repo, shard_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())

        result = registry_history_cold_rehydrate_service(
            repo_root=self.repo, gm=self.gm, auth=None,
            req=type("Req", (), {"source_payload_path": shard_rel, "cold_stub_path": None})(),
            audit=_noop_audit,
        )

        # Should recover: delete orphan hot, redo rehydrate
        self.assertTrue(result["ok"])
        self.assertEqual(result["shard_state"], "hot")

    def test_crash_recovery_unreadable_stub_raises_before_lock(self):
        """Corrupted stub causes pre-lock load failure (400), not a brick."""
        shard_rel, stub_rel, cold_rel, payload = self._create_cold_stored_shard()
        # Simulate: hot shard from partial rehydrate
        hot_path = safe_path(self.repo, shard_rel)
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, json.dumps(payload).encode())
        # Corrupt stub — pre-lock load will fail before reaching crash recovery
        safe_path(self.repo, stub_rel).write_text("NOT JSON", encoding="utf-8")

        with self.assertRaises(HTTPException) as ctx:
            registry_history_cold_rehydrate_service(
                repo_root=self.repo, gm=self.gm, auth=None,
                req=type("Req", (), {"source_payload_path": shard_rel, "cold_stub_path": None})(),
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 400)


# ===================================================================
# Continuity cold-store crash recovery (Fix 4)
# ===================================================================


def _continuity_capsule(*, subject_kind: str, subject_id: str, now_iso: str) -> dict:
    """Build a valid continuity capsule for testing."""
    return {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now_iso,
        "verified_at": now_iso,
        "verification_kind": "system_check",
        "source": {
            "producer": "crash-recovery-test",
            "update_reason": "manual",
            "inputs": ["memory/core/identity.md"],
        },
        "continuity": {
            "top_priorities": ["test recovery"],
            "active_constraints": [],
            "active_concerns": [],
            "open_loops": [],
            "stance_summary": "Testing crash recovery.",
            "drift_signals": [],
            "session_trajectory": [],
            "trailing_notes": [],
            "curiosity_queue": [],
            "negative_decisions": [],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.0},
        "freshness": {"freshness_class": "durable"},
        "verification_state": {
            "status": "system_confirmed",
            "last_revalidated_at": now_iso,
            "strongest_signal": "system_check",
            "evidence_refs": [],
        },
        "capsule_health": {
            "status": "healthy",
            "reasons": [],
            "last_checked_at": now_iso,
        },
    }


class TestContinuityColdStoreCrashRecovery(unittest.TestCase):
    """Crash recovery for continuity_cold_store_service when archive is missing."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.gm = SimpleGitManagerStub(self.repo)
        self.auth = AllowAllAuthStub(peer_id="peer-test")

    def _create_cold_stored_state(self):
        """Create valid cold files (no archive) simulating post-archive-delete crash."""
        from app.continuity.service import (
            continuity_cold_storage_rel_path,
            continuity_cold_stub_rel_path,
            CONTINUITY_DIR_REL,
            _build_cold_stub_text,
        )
        from app.timestamps import format_iso, format_compact
        now = _now()

        subject_kind = "user"
        subject_id = "alpha"
        archived_at = format_iso(now)
        timestamp = format_compact(now)
        archive_rel = f"{CONTINUITY_DIR_REL}/archive/{subject_kind}-{subject_id}-{timestamp}.json"
        capsule = _continuity_capsule(
            subject_kind=subject_kind, subject_id=subject_id, now_iso=archived_at,
        )
        envelope = {
            "schema_type": "continuity_archive_envelope",
            "schema_version": "1.0",
            "archived_at": archived_at,
            "archived_by": "peer-admin",
            "reason": "retention",
            "active_path": f"{CONTINUITY_DIR_REL}/{subject_kind}-{subject_id}.json",
            "capsule": capsule,
        }
        cold_storage_rel = continuity_cold_storage_rel_path(archive_rel)
        cold_stub_rel = continuity_cold_stub_rel_path(archive_rel)

        # Write cold gzip (compressed envelope)
        cold_path = safe_path(self.repo, cold_storage_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(cold_path, gzip.compress(
            json.dumps(envelope).encode(), mtime=0,
        ))
        # Write cold stub
        stub_text = _build_cold_stub_text(
            envelope=envelope,
            source_archive_path=archive_rel,
            cold_storage_path=cold_storage_rel,
            cold_stored_at=archived_at,
            now=now,
        )
        cold_stub_path = safe_path(self.repo, cold_stub_rel)
        cold_stub_path.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(cold_stub_path, stub_text)

        return archive_rel, cold_storage_rel, cold_stub_rel, envelope

    def test_crash_recovery_archive_missing_cold_valid(self):
        """Archive deleted, valid cold files exist → recovery commit, return success."""
        from app.continuity.service import continuity_cold_store_service
        from app.models import ContinuityColdStoreRequest

        archive_rel, cold_storage_rel, cold_stub_rel, _envelope = self._create_cold_stored_state()
        # Archive intentionally NOT created — simulates post-delete crash

        result = continuity_cold_store_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ContinuityColdStoreRequest(source_archive_path=archive_rel),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["artifact_state"], "cold")
        warnings = [w["code"] for w in result.get("warnings", [])]
        self.assertIn("continuity_cold_store_crash_recovery", warnings)

    def test_crash_recovery_archive_missing_cold_invalid(self):
        """Archive deleted, invalid cold stub → falls through to 404."""
        from app.continuity.service import (
            continuity_cold_store_service,
            continuity_cold_storage_rel_path,
            continuity_cold_stub_rel_path,
            CONTINUITY_DIR_REL,
        )
        from app.models import ContinuityColdStoreRequest
        from app.timestamps import format_compact

        now = _now()
        timestamp = format_compact(now)
        archive_rel = f"{CONTINUITY_DIR_REL}/archive/agent-agent-alpha-{timestamp}.json"
        cold_storage_rel = continuity_cold_storage_rel_path(archive_rel)
        cold_stub_rel = continuity_cold_stub_rel_path(archive_rel)

        # Write cold gzip (valid)
        cold_path = safe_path(self.repo, cold_storage_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"some-data")
        # Write invalid cold stub
        stub_path = safe_path(self.repo, cold_stub_rel)
        stub_path.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(stub_path, "NOT VALID FRONTMATTER")

        with self.assertRaises(HTTPException) as ctx:
            continuity_cold_store_service(
                repo_root=self.repo, gm=self.gm, auth=self.auth,
                req=ContinuityColdStoreRequest(source_archive_path=archive_rel),
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 404)

    def test_crash_recovery_archive_missing_cold_partial(self):
        """Archive deleted, only cold payload (no stub) → falls through to 404."""
        from app.continuity.service import (
            continuity_cold_store_service,
            continuity_cold_storage_rel_path,
            CONTINUITY_DIR_REL,
        )
        from app.models import ContinuityColdStoreRequest
        from app.timestamps import format_compact

        now = _now()
        timestamp = format_compact(now)
        archive_rel = f"{CONTINUITY_DIR_REL}/archive/agent-agent-beta-{timestamp}.json"
        cold_storage_rel = continuity_cold_storage_rel_path(archive_rel)

        # Write cold gzip only (no stub)
        cold_path = safe_path(self.repo, cold_storage_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"some-data")

        with self.assertRaises(HTTPException) as ctx:
            continuity_cold_store_service(
                repo_root=self.repo, gm=self.gm, auth=self.auth,
                req=ContinuityColdStoreRequest(source_archive_path=archive_rel),
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 404)


# ===================================================================
# Continuity cold-rehydrate crash recovery (Fix 5)
# ===================================================================


class TestContinuityColdRehydrateCrashRecovery(unittest.TestCase):
    """Crash recovery for continuity_cold_rehydrate_service."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Path(self.tmp)
        self.gm = SimpleGitManagerStub(self.repo)
        self.auth = AllowAllAuthStub(peer_id="peer-test")

    def _create_rehydrate_crash_state(self):
        """Create state simulating crash after archive write but before cold cleanup."""
        from app.continuity.service import (
            continuity_cold_storage_rel_path,
            continuity_cold_stub_rel_path,
            CONTINUITY_DIR_REL,
            CONTINUITY_ARCHIVE_SCHEMA_TYPE,
            CONTINUITY_ARCHIVE_SCHEMA_VERSION,
            _build_cold_stub_text,
        )
        from app.timestamps import format_iso, format_compact
        now = _now()

        subject_kind = "user"
        subject_id = "gamma"
        archived_at = format_iso(now)
        timestamp = format_compact(now)
        archive_rel = f"{CONTINUITY_DIR_REL}/archive/{subject_kind}-{subject_id}-{timestamp}.json"
        capsule = _continuity_capsule(
            subject_kind=subject_kind, subject_id=subject_id, now_iso=archived_at,
        )
        envelope = {
            "schema_type": CONTINUITY_ARCHIVE_SCHEMA_TYPE,
            "schema_version": CONTINUITY_ARCHIVE_SCHEMA_VERSION,
            "archived_at": archived_at,
            "archived_by": "peer-admin",
            "reason": "retention",
            "active_path": f"{CONTINUITY_DIR_REL}/{subject_kind}-{subject_id}.json",
            "capsule": capsule,
        }
        cold_storage_rel = continuity_cold_storage_rel_path(archive_rel)
        cold_stub_rel = continuity_cold_stub_rel_path(archive_rel)

        # Write archive (simulating successful rehydrate write)
        archive_path = safe_path(self.repo, archive_rel)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(archive_path, json.dumps(envelope).encode())
        # Write cold gzip (orphan from pre-rehydrate)
        cold_path = safe_path(self.repo, cold_storage_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(cold_path, gzip.compress(
            json.dumps(envelope).encode(), mtime=0,
        ))
        # Write cold stub (orphan from pre-rehydrate)
        stub_text = _build_cold_stub_text(
            envelope=envelope,
            source_archive_path=archive_rel,
            cold_storage_path=cold_storage_rel,
            cold_stored_at=archived_at,
            now=now,
        )
        cold_stub_path = safe_path(self.repo, cold_stub_rel)
        cold_stub_path.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(cold_stub_path, stub_text)

        return archive_rel, cold_storage_rel, cold_stub_rel

    def test_crash_recovery_archive_and_cold_exist(self):
        """Valid archive + cold files → recovery cleans cold, returns success."""
        from app.continuity.service import continuity_cold_rehydrate_service
        from app.models import ContinuityColdRehydrateRequest

        archive_rel, cold_storage_rel, cold_stub_rel = self._create_rehydrate_crash_state()

        result = continuity_cold_rehydrate_service(
            repo_root=self.repo, gm=self.gm, auth=self.auth,
            req=ContinuityColdRehydrateRequest(cold_stub_path=cold_stub_rel),
            audit=_noop_audit,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["artifact_state"], "archived")
        # Cold files should be cleaned up
        self.assertFalse(safe_path(self.repo, cold_storage_rel).exists())
        self.assertFalse(safe_path(self.repo, cold_stub_rel).exists())
        # Archive should still exist
        self.assertTrue(safe_path(self.repo, archive_rel).exists())
        warnings = [w["code"] for w in result.get("warnings", [])]
        self.assertIn("continuity_cold_rehydrate_crash_recovery", warnings)

    def test_crash_recovery_invalid_archive(self):
        """Corrupt archive + cold files → falls through to 409."""
        from app.continuity.service import (
            continuity_cold_rehydrate_service,
            continuity_cold_storage_rel_path,
            continuity_cold_stub_rel_path,
            CONTINUITY_DIR_REL,
            _build_cold_stub_text,
        )
        from app.models import ContinuityColdRehydrateRequest
        from app.timestamps import format_iso, format_compact
        now = _now()

        subject_kind = "user"
        subject_id = "delta"
        archived_at = format_iso(now)
        timestamp = format_compact(now)
        archive_rel = f"{CONTINUITY_DIR_REL}/archive/{subject_kind}-{subject_id}-{timestamp}.json"
        cold_storage_rel = continuity_cold_storage_rel_path(archive_rel)
        cold_stub_rel = continuity_cold_stub_rel_path(archive_rel)

        capsule = _continuity_capsule(
            subject_kind=subject_kind, subject_id=subject_id, now_iso=archived_at,
        )
        envelope = {
            "schema_type": "continuity_archive_envelope",
            "schema_version": "1.0",
            "archived_at": archived_at,
            "archived_by": "peer-admin",
            "reason": "retention",
            "active_path": f"{CONTINUITY_DIR_REL}/{subject_kind}-{subject_id}.json",
            "capsule": capsule,
        }

        # Write corrupt archive
        archive_path = safe_path(self.repo, archive_rel)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(archive_path, b"NOT VALID JSON")
        # Write cold gzip
        cold_path = safe_path(self.repo, cold_storage_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(cold_path, b"cold-data")
        # Write cold stub
        stub_text = _build_cold_stub_text(
            envelope=envelope,
            source_archive_path=archive_rel,
            cold_storage_path=cold_storage_rel,
            cold_stored_at=archived_at,
            now=now,
        )
        stub_path = safe_path(self.repo, cold_stub_rel)
        stub_path.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(stub_path, stub_text)

        with self.assertRaises(HTTPException) as ctx:
            continuity_cold_rehydrate_service(
                repo_root=self.repo, gm=self.gm, auth=self.auth,
                req=ContinuityColdRehydrateRequest(cold_stub_path=cold_stub_rel),
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
