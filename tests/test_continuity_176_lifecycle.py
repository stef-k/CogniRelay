"""Tests for #176 Move C: standalone lifecycle transition endpoint.

Covers the full lifecycle state machine, guard rails, field immutability,
stale-write protection, response shape, and durability artefacts.
"""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_lifecycle_service, continuity_upsert_service
from app.models import ContinuityLifecycleRequest, ContinuityUpsertRequest

_CALL_COUNTER = 0


# ---------------------------------------------------------------------------
# Stubs & helpers
# ---------------------------------------------------------------------------


class _AuthStub:
    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")
        self.commits: list[tuple[str, str]] = []

    def latest_commit(self) -> str:
        return "test-sha"

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


def _noop_audit(*_args: Any, **_kw: Any) -> None:
    return None


def _now_iso() -> str:
    """Return a unique, monotonically increasing UTC timestamp per call."""
    global _CALL_COUNTER
    _CALL_COUNTER += 1
    dt = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(
        seconds=_CALL_COUNTER
    )
    return dt.isoformat().replace("+00:00", "Z")


def _make_dirs(root: Path) -> None:
    """Create the directory skeleton required by the continuity service."""
    (root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
    (root / "memory" / "continuity" / "fallback").mkdir(parents=True, exist_ok=True)
    (root / ".locks").mkdir(parents=True, exist_ok=True)


def _seed_thread_capsule(
    repo_root: Path,
    gm: _GitManagerStub,
    auth: _AuthStub,
    audit: Any = _noop_audit,
    subject_id: str = "test-thread",
    lifecycle: str = "active",
) -> tuple[dict[str, Any], str]:
    """Create a thread capsule via upsert and optionally transition it."""
    now = _now_iso()
    capsule: dict[str, Any] = {
        "schema_version": "1.0",
        "subject_kind": "thread",
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "source": {
            "producer": "test",
            "update_reason": "manual",
            "inputs": [],
        },
        "continuity": {
            "top_priorities": ["p1"],
            "active_concerns": ["c1"],
            "active_constraints": ["k1"],
            "open_loops": ["loop1"],
            "stance_summary": "Thread orientation for testing purposes here",
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.8},
        "thread_descriptor": {
            "label": "Test Thread",
            "keywords": ["test"],
        },
    }
    req = ContinuityUpsertRequest.model_validate(
        {
            "subject_kind": "thread",
            "subject_id": subject_id,
            "capsule": capsule,
        }
    )
    result = continuity_upsert_service(
        repo_root=repo_root, gm=gm, auth=auth, req=req, audit=audit
    )

    if lifecycle != "active":
        time.sleep(0.01)
        transition_ts = _now_iso()
        if lifecycle == "suspended":
            transition = "suspend"
        elif lifecycle == "concluded":
            transition = "conclude"
        elif lifecycle == "superseded":
            transition = "supersede"
        else:
            raise ValueError(f"Unknown lifecycle target: {lifecycle}")
        lc_req = ContinuityLifecycleRequest.model_validate(
            {
                "subject_kind": "thread",
                "subject_id": subject_id,
                "transition": transition,
                "updated_at": transition_ts,
                **(
                    {"superseded_by": "other-thread"}
                    if transition == "supersede"
                    else {}
                ),
            }
        )
        continuity_lifecycle_service(
            repo_root=repo_root, gm=gm, auth=auth, req=lc_req, audit=audit
        )

    return result, now


def _read_stored_capsule(repo_root: Path, subject_id: str = "test-thread") -> dict[str, Any]:
    """Load the persisted capsule JSON from disk."""
    path = repo_root / "memory" / "continuity" / f"thread-{subject_id}.json"
    return json.loads(path.read_bytes())


def _lifecycle_request(
    subject_id: str = "test-thread",
    transition: str = "suspend",
    updated_at: str | None = None,
    superseded_by: str | None = None,
) -> ContinuityLifecycleRequest:
    payload: dict[str, Any] = {
        "subject_kind": "thread",
        "subject_id": subject_id,
        "transition": transition,
        "updated_at": updated_at or _now_iso(),
    }
    if superseded_by is not None:
        payload["superseded_by"] = superseded_by
    return ContinuityLifecycleRequest.model_validate(payload)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestLifecycleActiveTransitions(unittest.TestCase):
    """Tests 1-3: transitions from active state."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_active_to_suspended(self) -> None:
        """Test 1: active -> suspend works."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "suspended")
        self.assertEqual(result["previous_lifecycle"], "active")

    def test_active_to_concluded(self) -> None:
        """Test 2: active -> conclude works."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="conclude")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "concluded")
        self.assertEqual(result["previous_lifecycle"], "active")

    def test_active_to_superseded(self) -> None:
        """Test 3: active -> supersede works (with superseded_by)."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="supersede", superseded_by="new-thread")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "superseded")
        self.assertEqual(result["previous_lifecycle"], "active")
        # Verify superseded_by persisted
        stored = _read_stored_capsule(self.root)
        self.assertEqual(stored["thread_descriptor"]["superseded_by"], "new-thread")


class TestLifecycleSuspendedTransitions(unittest.TestCase):
    """Tests 4-6: transitions from suspended state."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth, lifecycle="suspended")

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_suspended_to_active(self) -> None:
        """Test 4: suspended -> resume works."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="resume")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "active")
        self.assertEqual(result["previous_lifecycle"], "suspended")

    def test_suspended_to_concluded(self) -> None:
        """Test 5: suspended -> conclude works."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="conclude")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "concluded")
        self.assertEqual(result["previous_lifecycle"], "suspended")

    def test_suspended_to_superseded(self) -> None:
        """Test 6: suspended -> supersede works (with superseded_by)."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="supersede", superseded_by="replacement-thread")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["lifecycle"], "superseded")
        self.assertEqual(result["previous_lifecycle"], "suspended")
        stored = _read_stored_capsule(self.root)
        self.assertEqual(
            stored["thread_descriptor"]["superseded_by"], "replacement-thread"
        )


class TestLifecycleTerminalStates(unittest.TestCase):
    """Tests 7-8: concluded and superseded are terminal."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_concluded_rejects_all_transitions(self) -> None:
        """Test 7: concluded -> any transition -> 400."""
        _seed_thread_capsule(
            self.root, self.gm, self.auth, lifecycle="concluded"
        )
        for transition in ("suspend", "resume", "conclude", "supersede"):
            with self.subTest(transition=transition):
                time.sleep(0.01)
                kwargs: dict[str, Any] = {"transition": transition}
                if transition == "supersede":
                    kwargs["superseded_by"] = "x"
                req = _lifecycle_request(**kwargs)
                with self.assertRaises(HTTPException) as ctx:
                    continuity_lifecycle_service(
                        repo_root=self.root,
                        gm=self.gm,
                        auth=self.auth,
                        req=req,
                        audit=_noop_audit,
                    )
                self.assertEqual(ctx.exception.status_code, 400)
                self.assertIn("terminal", ctx.exception.detail)

    def test_superseded_rejects_all_transitions(self) -> None:
        """Test 8: superseded -> any transition -> 400."""
        _seed_thread_capsule(
            self.root, self.gm, self.auth, lifecycle="superseded"
        )
        for transition in ("suspend", "resume", "conclude", "supersede"):
            with self.subTest(transition=transition):
                time.sleep(0.01)
                kwargs: dict[str, Any] = {"transition": transition}
                if transition == "supersede":
                    kwargs["superseded_by"] = "y"
                req = _lifecycle_request(**kwargs)
                with self.assertRaises(HTTPException) as ctx:
                    continuity_lifecycle_service(
                        repo_root=self.root,
                        gm=self.gm,
                        auth=self.auth,
                        req=req,
                        audit=_noop_audit,
                    )
                self.assertEqual(ctx.exception.status_code, 400)
                self.assertIn("terminal", ctx.exception.detail)


class TestSupersededByValidation(unittest.TestCase):
    """Tests 9-10: superseded_by field guard rails."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_supersede_without_superseded_by_rejected(self) -> None:
        """Test 9: superseded_by required when transition='supersede'; 400 otherwise."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="supersede")
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("superseded_by", ctx.exception.detail)

    def test_superseded_by_rejected_on_non_supersede_transition(self) -> None:
        """Test 10: superseded_by rejected when transition != 'supersede'; 400."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend", superseded_by="wrong")
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("superseded_by", ctx.exception.detail)


class TestLifecycleErrorCases(unittest.TestCase):
    """Tests 11-13: nonexistent capsule, missing thread_descriptor, stale-write."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_nonexistent_capsule_returns_404(self) -> None:
        """Test 11: nonexistent capsule -> 404."""
        req = _lifecycle_request(subject_id="ghost-thread", transition="suspend")
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 404)

    def test_capsule_without_thread_descriptor_returns_400(self) -> None:
        """Test 12: capsule without thread_descriptor -> 400."""
        # Create a task capsule (no thread_descriptor) to exercise this guard.
        # We write a minimal valid capsule directly to disk.
        now = _now_iso()
        capsule: dict[str, Any] = {
            "schema_version": "1.0",
            "subject_kind": "task",
            "subject_id": "bare-task",
            "updated_at": now,
            "verified_at": now,
            "source": {
                "producer": "test",
                "update_reason": "manual",
                "inputs": [],
            },
            "continuity": {
                "top_priorities": ["p1"],
                "active_concerns": ["c1"],
                "active_constraints": ["k1"],
                "open_loops": ["loop1"],
                "stance_summary": "Task orientation for testing purposes here.",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.9, "relationship_model": 0.8},
        }
        upsert_req = ContinuityUpsertRequest.model_validate(
            {
                "subject_kind": "task",
                "subject_id": "bare-task",
                "capsule": capsule,
            }
        )
        continuity_upsert_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=upsert_req, audit=_noop_audit
        )

        time.sleep(0.01)
        lc_req = ContinuityLifecycleRequest.model_validate(
            {
                "subject_kind": "task",
                "subject_id": "bare-task",
                "transition": "suspend",
                "updated_at": _now_iso(),
            }
        )
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=lc_req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("thread_descriptor", ctx.exception.detail)

    def test_stale_write_guard_rejects_old_timestamp(self) -> None:
        """Test 13: stale-write guard (updated_at must be > stored)."""
        _seed_thread_capsule(self.root, self.gm, self.auth)
        stored = _read_stored_capsule(self.root)
        # Derive old_ts from the actual stored timestamp to avoid wall-clock fragility.
        stored_dt = datetime.fromisoformat(stored["updated_at"].replace("Z", "+00:00"))
        old_ts = (stored_dt - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        req = _lifecycle_request(transition="suspend", updated_at=old_ts)
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 409)

    def test_stale_write_guard_rejects_equal_timestamp(self) -> None:
        """Stale-write guard also rejects equal timestamp (conflict)."""
        _seed_thread_capsule(self.root, self.gm, self.auth)
        stored = _read_stored_capsule(self.root)
        req = _lifecycle_request(transition="suspend", updated_at=stored["updated_at"])
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=req,
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 409)


class TestFieldImmutability(unittest.TestCase):
    """Test 14: only lifecycle, superseded_by, and updated_at change."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_non_lifecycle_fields_unchanged_after_transition(self) -> None:
        """After suspend, every field except lifecycle/superseded_by/updated_at is identical."""
        before = _read_stored_capsule(self.root)
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        after = _read_stored_capsule(self.root)

        # These fields SHOULD change
        self.assertNotEqual(before["updated_at"], after["updated_at"])
        self.assertEqual(
            after["thread_descriptor"]["lifecycle"], "suspended"
        )

        # Strip mutable fields for comparison
        mutable_keys = {"lifecycle", "superseded_by"}
        before_td = {
            k: v
            for k, v in before.get("thread_descriptor", {}).items()
            if k not in mutable_keys
        }
        after_td = {
            k: v
            for k, v in after.get("thread_descriptor", {}).items()
            if k not in mutable_keys
        }
        self.assertEqual(before_td, after_td)

        # Compare all top-level fields except updated_at and thread_descriptor
        skip = {"updated_at", "thread_descriptor"}
        for key in set(before.keys()) | set(after.keys()):
            if key in skip:
                continue
            self.assertEqual(
                before.get(key),
                after.get(key),
                f"field '{key}' changed unexpectedly",
            )

    def test_supersede_only_changes_lifecycle_and_superseded_by(self) -> None:
        """Supersede transition sets superseded_by but nothing else extra."""
        before = _read_stored_capsule(self.root)
        time.sleep(0.01)
        req = _lifecycle_request(transition="supersede", superseded_by="new-thread")
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        after = _read_stored_capsule(self.root)

        self.assertEqual(after["thread_descriptor"]["lifecycle"], "superseded")
        self.assertEqual(after["thread_descriptor"]["superseded_by"], "new-thread")

        # Continuity block must be identical
        self.assertEqual(before["continuity"], after["continuity"])
        self.assertEqual(before["confidence"], after["confidence"])
        self.assertEqual(before["source"], after["source"])


class TestDurabilityArtefacts(unittest.TestCase):
    """Test 15: fallback snapshot created, git commit issued."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)
        self.gm.commits.clear()  # reset to only observe lifecycle commits

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_git_commit_created_on_transition(self) -> None:
        """A lifecycle transition triggers a git commit."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        # At least one commit for the active capsule write
        self.assertTrue(
            len(self.gm.commits) >= 1,
            f"Expected at least 1 commit, got {len(self.gm.commits)}",
        )

    def test_fallback_snapshot_written_on_transition(self) -> None:
        """A fallback snapshot file is created/updated after lifecycle transition."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        fallback_path = (
            self.root / "memory" / "continuity" / "fallback" / "thread-test-thread.json"
        )
        self.assertTrue(
            fallback_path.exists(),
            "Fallback snapshot should exist after lifecycle transition",
        )
        fb_data = json.loads(fallback_path.read_bytes())
        # Fallback is an envelope with a nested capsule
        capsule = fb_data.get("capsule", fb_data)
        self.assertEqual(
            capsule.get("thread_descriptor", {}).get("lifecycle"), "suspended"
        )

    def test_custom_commit_message_used(self) -> None:
        """Custom commit_message from the request is used in git commit."""
        time.sleep(0.01)
        lc_req = ContinuityLifecycleRequest.model_validate(
            {
                "subject_kind": "thread",
                "subject_id": "test-thread",
                "transition": "suspend",
                "updated_at": _now_iso(),
                "commit_message": "custom: freeze thread for review",
            }
        )
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=lc_req, audit=_noop_audit
        )
        commit_messages = [msg for _, msg in self.gm.commits]
        self.assertTrue(
            any("custom: freeze thread for review" in m for m in commit_messages),
            f"Expected custom commit message in {commit_messages}",
        )


class TestResponseShape(unittest.TestCase):
    """Test 16: response contains all required keys with correct types."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_response_has_all_required_keys(self) -> None:
        """Response must include ok, path, lifecycle, previous_lifecycle, durable,
        latest_commit, capsule_sha256, warnings, recovery_warnings."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        required_keys = {
            "ok",
            "path",
            "lifecycle",
            "previous_lifecycle",
            "durable",
            "latest_commit",
            "capsule_sha256",
            "warnings",
            "recovery_warnings",
        }
        self.assertTrue(
            required_keys.issubset(result.keys()),
            f"Missing keys: {required_keys - result.keys()}",
        )

    def test_response_value_types(self) -> None:
        """Verify the types of each response field."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="conclude")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertIsInstance(result["ok"], bool)
        self.assertIsInstance(result["path"], str)
        self.assertIsInstance(result["lifecycle"], str)
        self.assertIsInstance(result["previous_lifecycle"], str)
        self.assertIsInstance(result["durable"], bool)
        self.assertIsInstance(result["latest_commit"], str)
        self.assertIsInstance(result["capsule_sha256"], str)
        self.assertIsInstance(result["warnings"], list)
        self.assertIsInstance(result["recovery_warnings"], list)

    def test_response_ok_is_true(self) -> None:
        """Successful transition returns ok=True and durable=True."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["durable"])

    def test_capsule_sha256_is_hex(self) -> None:
        """capsule_sha256 should be a valid hex-encoded SHA-256 digest."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        sha = result["capsule_sha256"]
        self.assertEqual(len(sha), 64)
        # Must be valid hex
        int(sha, 16)

    def test_warnings_empty_on_success(self) -> None:
        """On a clean transition with working git stub, no warnings."""
        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        result = continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_noop_audit
        )
        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["recovery_warnings"], [])


class TestAuditEvent(unittest.TestCase):
    """Verify the audit callback is invoked with the correct event data."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_audit_emits_lifecycle_event(self) -> None:
        events: list[tuple[str, dict[str, Any]]] = []

        def _capture_audit(_auth: Any, event: str, detail: dict[str, Any]) -> None:
            events.append((event, detail))

        time.sleep(0.01)
        req = _lifecycle_request(transition="suspend")
        continuity_lifecycle_service(
            repo_root=self.root, gm=self.gm, auth=self.auth, req=req, audit=_capture_audit
        )
        self.assertEqual(len(events), 1)
        event_name, detail = events[0]
        self.assertEqual(event_name, "continuity_lifecycle")
        self.assertEqual(detail["transition"], "suspend")
        self.assertEqual(detail["lifecycle"], "suspended")
        self.assertEqual(detail["previous_lifecycle"], "active")
        self.assertIn("capsule_sha256", detail)
        self.assertTrue(detail["committed"])


class TestLifecycleRoundTrip(unittest.TestCase):
    """Multi-step lifecycle round-trips: suspend -> resume -> conclude."""

    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        _make_dirs(self.root)
        self.gm = _GitManagerStub(self.root)
        self.auth = _AuthStub()
        _seed_thread_capsule(self.root, self.gm, self.auth)

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_suspend_resume_conclude_chain(self) -> None:
        """Full chain: active -> suspend -> resume -> conclude."""
        # active -> suspended
        time.sleep(0.01)
        r1 = continuity_lifecycle_service(
            repo_root=self.root,
            gm=self.gm,
            auth=self.auth,
            req=_lifecycle_request(transition="suspend"),
            audit=_noop_audit,
        )
        self.assertEqual(r1["lifecycle"], "suspended")

        # suspended -> active
        time.sleep(0.01)
        r2 = continuity_lifecycle_service(
            repo_root=self.root,
            gm=self.gm,
            auth=self.auth,
            req=_lifecycle_request(transition="resume"),
            audit=_noop_audit,
        )
        self.assertEqual(r2["lifecycle"], "active")
        self.assertEqual(r2["previous_lifecycle"], "suspended")

        # active -> concluded
        time.sleep(0.01)
        r3 = continuity_lifecycle_service(
            repo_root=self.root,
            gm=self.gm,
            auth=self.auth,
            req=_lifecycle_request(transition="conclude"),
            audit=_noop_audit,
        )
        self.assertEqual(r3["lifecycle"], "concluded")
        self.assertEqual(r3["previous_lifecycle"], "active")

        # concluded -> any should fail
        time.sleep(0.01)
        with self.assertRaises(HTTPException) as ctx:
            continuity_lifecycle_service(
                repo_root=self.root,
                gm=self.gm,
                auth=self.auth,
                req=_lifecycle_request(transition="resume"),
                audit=_noop_audit,
            )
        self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
