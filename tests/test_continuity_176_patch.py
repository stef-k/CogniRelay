"""Comprehensive tests for the partial list-field patch feature (issue #176, Move B).

Covers all acceptance criteria from spec section 11:
 - append / remove / replace_at on string-list and structured-list targets
 - max-length enforcement, 404 for missing items, atomicity
 - stale-write rejection, nonexistent capsule, normalization, validation
 - fallback snapshot + git commit, thread_descriptor gating
 - invalid operation parameter combinations, multi-op batches
 - response shape verification
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import continuity_patch_service, continuity_upsert_service
from app.models import ContinuityPatchRequest, ContinuityUpsertRequest


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class _AuthStub:
    """Auth stub that permits all scopes."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    """Git manager stub that records committed files."""

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


_TS_COUNTER = 0
_TS_BASE = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def _next_ts() -> str:
    """Return a monotonically increasing UTC timestamp for deterministic testing."""
    global _TS_COUNTER  # noqa: PLW0603
    _TS_COUNTER += 1
    dt = _TS_BASE + timedelta(seconds=_TS_COUNTER)
    return dt.isoformat().replace("+00:00", "Z")


def _now_iso() -> str:
    return _next_ts()


def _later_iso() -> str:
    """Return a timestamp guaranteed to be strictly after any prior call."""
    return _next_ts()


def _noop_audit(*_args: Any, **_kw: Any) -> None:
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_dirs(root: Path) -> None:
    """Create the directory structure expected by the continuity service."""
    (root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
    (root / "memory" / "continuity" / "fallback").mkdir(parents=True, exist_ok=True)
    (root / ".locks").mkdir(parents=True, exist_ok=True)


def _seed_capsule(
    repo_root: Path,
    gm: _GitManagerStub,
    auth: _AuthStub,
    *,
    subject_kind: str = "user",
    subject_id: str = "test-agent",
    extra_continuity: dict[str, Any] | None = None,
    extra_capsule: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    """Create and persist a baseline capsule. Returns (upsert result, updated_at)."""
    now = _now_iso()
    continuity: dict[str, Any] = {
        "top_priorities": ["p1", "p2"],
        "active_concerns": ["c1"],
        "active_constraints": ["k1"],
        "open_loops": ["loop1", "loop2"],
        "stance_summary": "Current orientation for testing purposes here",
        "drift_signals": ["d1"],
        "working_hypotheses": ["h1"],
    }
    if extra_continuity:
        continuity.update(extra_continuity)
    capsule: dict[str, Any] = {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": continuity,
        "confidence": {"continuity": 0.9, "relationship_model": 0.8},
    }
    if extra_capsule:
        capsule.update(extra_capsule)
    req = ContinuityUpsertRequest.model_validate({
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "capsule": capsule,
    })
    result = continuity_upsert_service(
        repo_root=repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_noop_audit,
    )
    return result, now


def _patch(
    repo_root: Path,
    gm: _GitManagerStub,
    auth: _AuthStub,
    operations: list[dict[str, Any]],
    *,
    subject_kind: str = "user",
    subject_id: str = "test-agent",
    updated_at: str | None = None,
    audit: Any = None,
) -> dict[str, Any]:
    """Build a ContinuityPatchRequest and call the service."""
    ts = updated_at or _later_iso()
    req = ContinuityPatchRequest.model_validate({
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": ts,
        "operations": operations,
    })
    return continuity_patch_service(
        repo_root=repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=audit or _noop_audit,
    )


def _read_persisted_capsule(repo_root: Path, subject_kind: str = "user", subject_id: str = "test-agent") -> dict[str, Any]:
    """Read the on-disk capsule JSON."""
    path = repo_root / "memory" / "continuity" / f"{subject_kind}-{subject_id}.json"
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPatchAppendStringList(unittest.TestCase):
    """AC-1: append to string-list target."""

    def test_append_to_open_loops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "new-loop"},
            ])

            self.assertTrue(result["ok"])
            self.assertTrue(result["updated"])
            capsule = _read_persisted_capsule(root)
            self.assertIn("new-loop", capsule["continuity"]["open_loops"])
            self.assertEqual(capsule["continuity"]["open_loops"][-1], "new-loop")


class TestPatchAppendStructuredList(unittest.TestCase):
    """AC-2: append to structured-list target."""

    def test_append_negative_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {
                    "target": "continuity.negative_decisions",
                    "action": "append",
                    "value": {"decision": "Do not use library X", "rationale": "Licensing conflict"},
                },
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            decisions = capsule["continuity"]["negative_decisions"]
            self.assertTrue(any(d["decision"] == "Do not use library X" for d in decisions))

    def test_append_rationale_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            entry = {
                "tag": "arch-choice-1",
                "kind": "decision",
                "status": "active",
                "summary": "Use FastAPI for the new service",
                "reasoning": "Better async support and type hints",
                "last_confirmed_at": _now_iso(),
            }
            result = _patch(root, gm, auth, [
                {"target": "continuity.rationale_entries", "action": "append", "value": entry},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            tags = [e["tag"] for e in capsule["continuity"]["rationale_entries"]]
            self.assertIn("arch-choice-1", tags)

    def test_append_stable_preference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            pref = {"tag": "editor-pref", "content": "Use dark mode", "last_confirmed_at": _now_iso()}
            result = _patch(root, gm, auth, [
                {"target": "stable_preferences", "action": "append", "value": pref},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            tags = [p["tag"] for p in capsule.get("stable_preferences", [])]
            self.assertIn("editor-pref", tags)


class TestPatchAppendMaxLength(unittest.TestCase):
    """AC-3: append rejects when max-length would be exceeded."""

    def test_append_exceeds_open_loops_max(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            # Seed with 8 open_loops (max)
            _seed_capsule(root, gm, auth, extra_continuity={
                "open_loops": [f"loop-{index}" for index in range(8)],
            })

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "overflow"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(
                ctx.exception.detail,
                "append would exceed max length (8) for continuity.open_loops",
            )

    def test_append_exceeds_negative_decisions_max(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            decisions = [
                {"decision": f"d{i}", "rationale": f"r{i}"}
                for i in range(4)  # max is 4
            ]
            _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": decisions,
            })

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {
                        "target": "continuity.negative_decisions",
                        "action": "append",
                        "value": {"decision": "overflow", "rationale": "too many"},
                    },
                ])
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("max length", ctx.exception.detail)


class TestPatch217RebalancedCoreListLimits(unittest.TestCase):
    """Lock the widened #217 patch-path bounds for the three rebalanced core lists."""

    def test_append_allows_growth_from_five_to_six(self) -> None:
        fields = (
            ("top_priorities", "continuity.top_priorities", "priority"),
            ("open_loops", "continuity.open_loops", "loop"),
            ("active_constraints", "continuity.active_constraints", "constraint"),
        )

        for field_name, target, prefix in fields:
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    _setup_dirs(root)
                    gm = _GitManagerStub(root)
                    auth = _AuthStub()
                    _seed_capsule(
                        root,
                        gm,
                        auth,
                        extra_continuity={field_name: [f"{prefix}-{index}" for index in range(5)]},
                    )

                    result = _patch(root, gm, auth, [
                        {"target": target, "action": "append", "value": f"{prefix}-5"},
                    ])

                    self.assertTrue(result["ok"])
                    capsule = _read_persisted_capsule(root)
                    self.assertEqual(len(capsule["continuity"][field_name]), 6)
                    self.assertEqual(capsule["continuity"][field_name][-1], f"{prefix}-5")

    def test_append_allows_growth_up_to_eight(self) -> None:
        fields = (
            ("top_priorities", "continuity.top_priorities", "priority"),
            ("open_loops", "continuity.open_loops", "loop"),
            ("active_constraints", "continuity.active_constraints", "constraint"),
        )

        for field_name, target, prefix in fields:
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    _setup_dirs(root)
                    gm = _GitManagerStub(root)
                    auth = _AuthStub()
                    _seed_capsule(
                        root,
                        gm,
                        auth,
                        extra_continuity={field_name: [f"{prefix}-{index}" for index in range(7)]},
                    )

                    result = _patch(root, gm, auth, [
                        {"target": target, "action": "append", "value": f"{prefix}-7"},
                    ])

                    self.assertTrue(result["ok"])
                    capsule = _read_persisted_capsule(root)
                    self.assertEqual(len(capsule["continuity"][field_name]), 8)
                    self.assertEqual(capsule["continuity"][field_name][-1], f"{prefix}-7")

    def test_append_rejects_growth_from_eight_to_nine(self) -> None:
        fields = (
            ("top_priorities", "continuity.top_priorities", "priority"),
            ("open_loops", "continuity.open_loops", "loop"),
            ("active_constraints", "continuity.active_constraints", "constraint"),
        )

        for field_name, target, prefix in fields:
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    _setup_dirs(root)
                    gm = _GitManagerStub(root)
                    auth = _AuthStub()
                    _seed_capsule(
                        root,
                        gm,
                        auth,
                        extra_continuity={field_name: [f"{prefix}-{index}" for index in range(8)]},
                    )

                    with self.assertRaises(HTTPException) as ctx:
                        _patch(root, gm, auth, [
                            {"target": target, "action": "append", "value": f"{prefix}-8"},
                        ])

                    self.assertEqual(ctx.exception.status_code, 400)
                    self.assertEqual(
                        ctx.exception.detail,
                        f"append would exceed max length (8) for {target}",
                    )


class TestPatchRemoveStringList(unittest.TestCase):
    """AC-4: remove by exact string on string-list target."""

    def test_remove_existing_open_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "remove", "match": "loop1"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            self.assertNotIn("loop1", capsule["continuity"]["open_loops"])
            self.assertIn("loop2", capsule["continuity"]["open_loops"])


class TestPatchRemoveStructuredList(unittest.TestCase):
    """AC-5: remove by key match on structured-list targets."""

    def test_remove_negative_decision_by_decision_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": [
                    {"decision": "avoid X", "rationale": "license risk"},
                    {"decision": "skip Y", "rationale": "too complex"},
                ],
            })

            result = _patch(root, gm, auth, [
                {"target": "continuity.negative_decisions", "action": "remove", "match": "avoid X"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            decisions = capsule["continuity"]["negative_decisions"]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["decision"], "skip Y")

    def test_remove_rationale_entry_by_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            now = _now_iso()
            _seed_capsule(root, gm, auth, extra_continuity={
                "rationale_entries": [
                    {"tag": "re-1", "kind": "decision", "status": "active",
                     "summary": "s", "reasoning": "r", "last_confirmed_at": now},
                ],
            })

            result = _patch(root, gm, auth, [
                {"target": "continuity.rationale_entries", "action": "remove", "match": "re-1"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            self.assertEqual(len(capsule["continuity"]["rationale_entries"]), 0)

    def test_remove_stable_preference_by_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth, extra_capsule={
                "stable_preferences": [
                    {"tag": "pref-a", "content": "Dark mode", "last_confirmed_at": _now_iso()},
                    {"tag": "pref-b", "content": "Vim keys", "last_confirmed_at": _now_iso()},
                ],
            })

            result = _patch(root, gm, auth, [
                {"target": "stable_preferences", "action": "remove", "match": "pref-a"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            tags = [p["tag"] for p in capsule.get("stable_preferences", [])]
            self.assertNotIn("pref-a", tags)
            self.assertIn("pref-b", tags)


class TestPatchRemoveNotFound(unittest.TestCase):
    """AC-6: remove returns 404 when no matching item exists."""

    def test_remove_nonexistent_string_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "remove", "match": "nonexistent"},
                ])
            self.assertEqual(ctx.exception.status_code, 404)

    def test_remove_nonexistent_structured_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": [
                    {"decision": "avoid X", "rationale": "reason"},
                ],
            })

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.negative_decisions", "action": "remove", "match": "no-such-decision"},
                ])
            self.assertEqual(ctx.exception.status_code, 404)


class TestPatchReplaceAtStringList(unittest.TestCase):
    """AC-7: replace_at by index on string-list target."""

    def test_replace_at_valid_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "replace_at", "index": 0, "value": "replaced-loop"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            self.assertEqual(capsule["continuity"]["open_loops"][0], "replaced-loop")
            self.assertIn("loop2", capsule["continuity"]["open_loops"])


class TestPatchReplaceAtStructuredList(unittest.TestCase):
    """AC-8: replace_at by key match on structured-list target."""

    def test_replace_at_negative_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _, seeded_updated_at = _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": [
                    {"decision": "avoid X", "rationale": "old reason"},
                ],
            })

            patch_updated_at = _later_iso()
            result = _patch(root, gm, auth, [
                {
                    "target": "continuity.negative_decisions",
                    "action": "replace_at",
                    "match": "avoid X",
                    "value": {"decision": "avoid X", "rationale": "updated reason"},
                },
            ], updated_at=patch_updated_at)

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            decisions = capsule["continuity"]["negative_decisions"]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["rationale"], "updated reason")
            self.assertEqual(decisions[0]["created_at"], seeded_updated_at)
            self.assertEqual(decisions[0]["updated_at"], patch_updated_at)


class TestPatchReplaceAtOutOfBounds(unittest.TestCase):
    """AC-9: replace_at returns 404 for out-of-bounds index."""

    def test_index_too_large(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "replace_at", "index": 99, "value": "x"},
                ])
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertIn("out of bounds", ctx.exception.detail)

    def test_negative_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "replace_at", "index": -1, "value": "x"},
                ])
            self.assertEqual(ctx.exception.status_code, 404)


class TestPatchReplaceAtNoMatchKey(unittest.TestCase):
    """AC-10: replace_at returns 404 when no matching key on structured target."""

    def test_no_matching_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": [
                    {"decision": "avoid X", "rationale": "reason"},
                ],
            })

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {
                        "target": "continuity.negative_decisions",
                        "action": "replace_at",
                        "match": "nonexistent-decision",
                        "value": {"decision": "nonexistent-decision", "rationale": "r"},
                    },
                ])
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertIn("no matching item", ctx.exception.detail)


class TestPatchAtomicity(unittest.TestCase):
    """AC-11: multi-op where a later op fails rolls back all changes."""

    def test_second_op_fails_no_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            # Read original bytes for comparison
            original_bytes = (root / "memory" / "continuity" / "user-test-agent.json").read_bytes()

            # First op would succeed (append), second op fails (remove nonexistent)
            with self.assertRaises(HTTPException):
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "transient"},
                    {"target": "continuity.open_loops", "action": "remove", "match": "nonexistent-item"},
                ])

            # File must be unchanged
            after_bytes = (root / "memory" / "continuity" / "user-test-agent.json").read_bytes()
            self.assertEqual(original_bytes, after_bytes)


class TestPatchStaleWrite(unittest.TestCase):
    """AC-12: stale-write rejection when updated_at is not newer than stored."""

    def test_same_timestamp_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _, seed_ts = _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "x"},
                ], updated_at=seed_ts)
            self.assertIn(ctx.exception.status_code, (400, 409))

    def test_older_timestamp_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException):
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "x"},
                ], updated_at="2020-01-01T00:00:00Z")


class TestPatchNonexistentCapsule(unittest.TestCase):
    """AC-13: patching a capsule that does not exist returns 404."""

    def test_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "x"},
                ], subject_id="no-such-agent")
            self.assertEqual(ctx.exception.status_code, 404)
            self.assertIn("not found", ctx.exception.detail)


class TestPatchNormalization(unittest.TestCase):
    """AC-14: post-patch normalization fires; normalizations_applied reported."""

    def test_whitespace_gets_stripped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "  padded  "},
            ])

            self.assertTrue(result["ok"])
            # Normalization should have stripped whitespace
            capsule = _read_persisted_capsule(root)
            self.assertIn("padded", capsule["continuity"]["open_loops"])
            self.assertNotIn("  padded  ", capsule["continuity"]["open_loops"])
            # normalizations_applied should be a list in the response
            self.assertIsInstance(result["normalizations_applied"], list)
            self.assertTrue(len(result["normalizations_applied"]) > 0)


class TestPatchSizeLimit(unittest.TestCase):
    """AC-15: post-patch validation rejects capsule exceeding 12 KB."""

    def test_oversized_capsule_rejected(self) -> None:
        """Post-patch validation catches capsules that exceed field-level or size limits."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            # Append a string that exceeds the per-item length limit (160 chars).
            huge_value = "x" * 200
            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": huge_value},
                ])
            self.assertEqual(ctx.exception.status_code, 400)


class TestPatchFallbackAndGit(unittest.TestCase):
    """AC-16: fallback snapshot is updated and git commit is created."""

    def test_fallback_written_and_committed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)
            commits_before = len(gm.commits)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "new-loop"},
            ])

            self.assertTrue(result["ok"])
            self.assertTrue(result["durable"])
            self.assertEqual(result["latest_commit"], "test-sha")

            # At least one new commit should have been made
            self.assertGreater(len(gm.commits), commits_before)

            # Fallback snapshot should exist
            fallback_path = root / "memory" / "continuity" / "fallback" / "user-test-agent.json"
            self.assertTrue(fallback_path.exists())


class TestPatchThreadDescriptorGating(unittest.TestCase):
    """AC-17: thread_descriptor targets only accepted when capsule has thread_descriptor."""

    def test_thread_descriptor_target_on_capsule_without_td_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            # User capsule has no thread_descriptor
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "thread_descriptor.keywords", "action": "append", "value": "kw1"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("thread_descriptor", ctx.exception.detail)

    def test_thread_descriptor_target_on_thread_capsule_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(
                root, gm, auth,
                subject_kind="thread",
                subject_id="thread-1",
                extra_capsule={
                    "thread_descriptor": {
                        "label": "Test thread",
                        "keywords": ["existing"],
                        "scope_anchors": [],
                        "identity_anchors": [],
                    },
                },
            )

            result = _patch(root, gm, auth, [
                {"target": "thread_descriptor.keywords", "action": "append", "value": "newkw"},
            ], subject_kind="thread", subject_id="thread-1")

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root, "thread", "thread-1")
            self.assertIn("newkw", capsule["thread_descriptor"]["keywords"])


class TestPatchInvalidOperationCombinations(unittest.TestCase):
    """AC-18: invalid parameter combinations raise 400."""

    def test_append_with_match_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "x", "match": "bad"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_append_with_index_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append", "value": "x", "index": 0},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_remove_with_value_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "remove", "match": "loop1", "value": "bad"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_remove_string_with_index_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "remove", "match": "loop1", "index": 0},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_replace_at_string_with_match_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "replace_at", "index": 0, "value": "x", "match": "bad"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_replace_at_structured_with_index_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth, extra_continuity={
                "negative_decisions": [
                    {"decision": "d1", "rationale": "r1"},
                ],
            })

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {
                        "target": "continuity.negative_decisions",
                        "action": "replace_at",
                        "match": "d1",
                        "index": 0,
                        "value": {"decision": "d1", "rationale": "r2"},
                    },
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_append_without_value_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "append"},
                ])
            self.assertEqual(ctx.exception.status_code, 400)

    def test_replace_at_without_value_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            with self.assertRaises(HTTPException) as ctx:
                _patch(root, gm, auth, [
                    {"target": "continuity.open_loops", "action": "replace_at", "index": 0},
                ])
            self.assertEqual(ctx.exception.status_code, 400)


class TestPatchMultipleOperations(unittest.TestCase):
    """AC-19: multiple operations in one request."""

    def test_multiple_ops_all_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "new-loop"},
                {"target": "continuity.top_priorities", "action": "replace_at", "index": 0, "value": "updated-p1"},
                {"target": "continuity.active_concerns", "action": "remove", "match": "c1"},
            ])

            self.assertTrue(result["ok"])
            self.assertEqual(result["operations_applied"], 3)
            capsule = _read_persisted_capsule(root)
            self.assertIn("new-loop", capsule["continuity"]["open_loops"])
            self.assertEqual(capsule["continuity"]["top_priorities"][0], "updated-p1")
            self.assertNotIn("c1", capsule["continuity"]["active_concerns"])

    def test_ordered_ops_build_on_each_other(self) -> None:
        """Operations execute in order, so an append then remove on the same list works."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "temp-loop"},
                {"target": "continuity.open_loops", "action": "remove", "match": "temp-loop"},
            ])

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root)
            self.assertNotIn("temp-loop", capsule["continuity"]["open_loops"])


class TestPatchResponseShape(unittest.TestCase):
    """AC-20: response shape verification."""

    def test_response_has_required_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "check-shape"},
            ])

            required_keys = {
                "ok", "path", "updated", "durable", "latest_commit",
                "capsule_sha256", "operations_applied", "normalizations_applied",
                "warnings", "recovery_warnings",
            }
            self.assertTrue(required_keys.issubset(set(result.keys())),
                            f"Missing keys: {required_keys - set(result.keys())}")
            self.assertIsInstance(result["ok"], bool)
            self.assertIsInstance(result["path"], str)
            self.assertIsInstance(result["updated"], bool)
            self.assertIsInstance(result["durable"], bool)
            self.assertIsInstance(result["latest_commit"], str)
            self.assertIsInstance(result["capsule_sha256"], str)
            self.assertIsInstance(result["operations_applied"], int)
            self.assertIsInstance(result["normalizations_applied"], list)
            self.assertIsInstance(result["warnings"], list)
            self.assertIsInstance(result["recovery_warnings"], list)

    def test_operations_applied_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "a"},
                {"target": "continuity.drift_signals", "action": "append", "value": "b"},
            ])

            self.assertEqual(result["operations_applied"], 2)


class TestPatchIdentityAnchors(unittest.TestCase):
    """Remove/replace_at for identity_anchors using kind:value match format."""

    def test_remove_identity_anchor_by_kind_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(
                root, gm, auth,
                subject_kind="thread",
                subject_id="thread-2",
                extra_capsule={
                    "thread_descriptor": {
                        "label": "Test",
                        "keywords": [],
                        "scope_anchors": [],
                        "identity_anchors": [
                            {"kind": "repo", "value": "my-repo"},
                            {"kind": "service", "value": "api"},
                        ],
                    },
                },
            )

            result = _patch(root, gm, auth, [
                {"target": "thread_descriptor.identity_anchors", "action": "remove", "match": "repo:my-repo"},
            ], subject_kind="thread", subject_id="thread-2")

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root, "thread", "thread-2")
            anchors = capsule["thread_descriptor"]["identity_anchors"]
            self.assertEqual(len(anchors), 1)
            self.assertEqual(anchors[0]["kind"], "service")

    def test_replace_at_identity_anchor_by_kind_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(
                root, gm, auth,
                subject_kind="thread",
                subject_id="thread-3",
                extra_capsule={
                    "thread_descriptor": {
                        "label": "Test",
                        "keywords": [],
                        "scope_anchors": [],
                        "identity_anchors": [
                            {"kind": "repo", "value": "old-repo"},
                        ],
                    },
                },
            )

            result = _patch(root, gm, auth, [
                {
                    "target": "thread_descriptor.identity_anchors",
                    "action": "replace_at",
                    "match": "repo:old-repo",
                    "value": {"kind": "repo", "value": "new-repo"},
                },
            ], subject_kind="thread", subject_id="thread-3")

            self.assertTrue(result["ok"])
            capsule = _read_persisted_capsule(root, "thread", "thread-3")
            anchors = capsule["thread_descriptor"]["identity_anchors"]
            self.assertEqual(anchors[0]["value"], "new-repo")


class TestPatchAuditCallback(unittest.TestCase):
    """Verify the audit callback is invoked with correct detail."""

    def test_audit_called_with_patch_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            events: list[tuple[str, dict]] = []

            def capture_audit(_auth: Any, event: str, detail: dict) -> None:
                events.append((event, detail))

            _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "audited"},
            ], audit=capture_audit)

            self.assertEqual(len(events), 1)
            event_name, detail = events[0]
            self.assertEqual(event_name, "continuity_patch")
            self.assertEqual(detail["subject_kind"], "user")
            self.assertEqual(detail["subject_id"], "test-agent")
            self.assertIn("capsule_sha256", detail)
            self.assertTrue(detail["committed"])


class TestPatchUpdatedAtWritten(unittest.TestCase):
    """Verify the patch timestamp is persisted as the capsule updated_at."""

    def test_updated_at_reflects_patch_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            patch_ts = _later_iso()
            _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "ts-test"},
            ], updated_at=patch_ts)

            capsule = _read_persisted_capsule(root)
            self.assertEqual(capsule["updated_at"], patch_ts)

    def test_patch_repairs_legacy_blank_top_level_timestamps(self) -> None:
        """Patch should recover from legacy capsules whose stored top-level timestamps are unusable."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            legacy_payload = {
                "schema_version": "1.0",
                "subject_kind": "user",
                "subject_id": "test-agent",
                "updated_at": "",
                "verified_at": "",
                "source": {"producer": "legacy", "update_reason": "manual", "inputs": []},
                "continuity": {
                    "top_priorities": ["p1"],
                    "active_concerns": ["c1"],
                    "active_constraints": ["k1"],
                    "open_loops": ["loop1"],
                    "stance_summary": "Legacy continuity payload",
                    "drift_signals": ["d1"],
                    "negative_decisions": [{"decision": "avoid x", "rationale": "legacy reason"}],
                },
                "confidence": {"continuity": 0.9, "relationship_model": 0.8},
            }
            (root / "memory" / "continuity" / "user-test-agent.json").write_text(
                json.dumps(legacy_payload),
                encoding="utf-8",
            )

            patch_ts = _later_iso()
            _patch(
                root,
                gm,
                auth,
                [{"target": "continuity.open_loops", "action": "append", "value": "loop2"}],
                updated_at=patch_ts,
            )

            capsule = _read_persisted_capsule(root)
            self.assertEqual(capsule["updated_at"], patch_ts)
            self.assertEqual(capsule["verified_at"], "1970-01-01T00:00:00Z")
            self.assertEqual(capsule["continuity"]["open_loops"], ["loop1", "loop2"])


class TestPatchNoOp(unittest.TestCase):
    """M5: append then remove same item yields no content change beyond updated_at."""

    def test_append_then_remove_preserves_content(self) -> None:
        """Appending and removing the same item leaves list content unchanged."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _setup_dirs(root)
            gm = _GitManagerStub(root)
            auth = _AuthStub()
            _seed_capsule(root, gm, auth)

            before = _read_persisted_capsule(root)
            result = _patch(root, gm, auth, [
                {"target": "continuity.open_loops", "action": "append", "value": "noop-item"},
                {"target": "continuity.open_loops", "action": "remove", "match": "noop-item"},
            ])
            self.assertTrue(result["ok"])
            after = _read_persisted_capsule(root)
            # List content should be unchanged despite append+remove.
            self.assertEqual(
                before["continuity"]["open_loops"],
                after["continuity"]["open_loops"],
            )
            # updated_at will differ (patch always advances it), so
            # updated=True and a commit are expected — the no-op is only
            # at the list-content level, not at the capsule level.


if __name__ == "__main__":
    unittest.main()
