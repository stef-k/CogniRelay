"""Tests for #167: session-end snapshot merge on the continuity upsert path."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import continuity_upsert
from app.models import ContinuityUpsertRequest


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _base_capsule(*, updated_at: str | None = None) -> dict:
    """Return a valid baseline capsule with stale continuity fields."""
    now = updated_at or _now_iso()
    return {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "test-agent",
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "test-hook",
            "update_reason": "pre_compaction",
            "inputs": [],
        },
        "continuity": {
            "top_priorities": ["stale priority"],
            "active_concerns": ["concern from base"],
            "active_constraints": ["stale constraint"],
            "open_loops": ["stale loop"],
            "stance_summary": "Stale stance from earlier in the session.",
            "drift_signals": ["drift-a"],
            "working_hypotheses": ["hypothesis-a"],
            "long_horizon_commitments": ["commitment-a"],
            "session_trajectory": ["stale trajectory step"],
            "negative_decisions": [
                {"decision": "stale decision", "rationale": "stale rationale"},
            ],
            "trailing_notes": ["trailing note"],
            "curiosity_queue": ["curiosity item"],
        },
        "confidence": {"continuity": 0.80, "relationship_model": 0.50},
    }


def _snapshot_payload(
    *,
    open_loops: list[str] | None = None,
    top_priorities: list[str] | None = None,
    active_constraints: list[str] | None = None,
    stance_summary: str | None = None,
    negative_decisions: list[dict] | None = None,
    session_trajectory: list[str] | None = None,
    include_p1: bool = True,
) -> dict:
    """Build a session_end_snapshot dict with sensible defaults."""
    snap: dict = {
        "open_loops": open_loops or ["fresh loop A", "fresh loop B"],
        "top_priorities": top_priorities or ["fresh priority A"],
        "active_constraints": active_constraints or ["fresh constraint A"],
        "stance_summary": stance_summary or "Fresh approach: focusing on resume capture quality improvements.",
    }
    if include_p1:
        if negative_decisions is not None:
            snap["negative_decisions"] = negative_decisions
        else:
            snap["negative_decisions"] = [
                {"decision": "Rejected smart inference", "rationale": "Out of scope per spec"},
            ]
        if session_trajectory is not None:
            snap["session_trajectory"] = session_trajectory
        else:
            snap["session_trajectory"] = ["Implemented merge logic", "Wrote tests"]
    return snap


def _do_upsert(repo_root: Path, capsule: dict, snapshot: dict | None = None) -> dict:
    """Execute a continuity upsert through the main endpoint."""
    gm = _GitManagerStub()
    s = _settings(repo_root)
    req_data: dict = {
        "subject_kind": "user",
        "subject_id": "test-agent",
        "capsule": capsule,
    }
    if snapshot is not None:
        req_data["session_end_snapshot"] = snapshot
    req = ContinuityUpsertRequest(**req_data)  # type: ignore[arg-type]
    with patch("app.main._services", return_value=(s, gm)):
        return continuity_upsert(req=req, auth=_AuthStub())


class TestSessionEndSnapshot(unittest.TestCase):
    """Validate session-end snapshot merge on the continuity upsert path (#167)."""

    def test_upsert_snapshot_omitted_unchanged_behavior(self) -> None:
        """When session_end_snapshot is omitted, response has no snapshot keys."""
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule())
        self.assertTrue(out["ok"])
        self.assertNotIn("session_end_snapshot_applied", out)
        self.assertNotIn("resume_quality", out)

    def test_upsert_snapshot_null_unchanged_behavior(self) -> None:
        """Explicitly passing null snapshot is the same as omitting it."""
        with tempfile.TemporaryDirectory() as td:
            # Build request with explicit session_end_snapshot=None in the dict
            # to exercise the Pydantic explicit-null path.
            gm = _GitManagerStub()
            s = _settings(Path(td))
            req_data: dict = {
                "subject_kind": "user",
                "subject_id": "test-agent",
                "capsule": _base_capsule(),
                "session_end_snapshot": None,
            }
            req = ContinuityUpsertRequest(**req_data)  # type: ignore[arg-type]
            with patch("app.main._services", return_value=(s, gm)):
                out = continuity_upsert(req=req, auth=_AuthStub())
        self.assertTrue(out["ok"])
        self.assertNotIn("session_end_snapshot_applied", out)
        self.assertNotIn("resume_quality", out)

    def test_upsert_snapshot_p0_overrides_capsule(self) -> None:
        """P0 snapshot fields override their capsule.continuity counterparts."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            out = _do_upsert(repo, _base_capsule(), _snapshot_payload())
            self.assertTrue(out["ok"])
            self.assertTrue(out["session_end_snapshot_applied"])

            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            cont = written["continuity"]
            self.assertEqual(cont["open_loops"], ["fresh loop A", "fresh loop B"])
            self.assertEqual(cont["top_priorities"], ["fresh priority A"])
            self.assertEqual(cont["active_constraints"], ["fresh constraint A"])
            self.assertIn("Fresh approach", cont["stance_summary"])

    def test_upsert_snapshot_p1_override_when_present(self) -> None:
        """P1 fields override capsule values when explicitly provided."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            snap = _snapshot_payload(
                negative_decisions=[
                    {"decision": "Skip schema redesign", "rationale": "Out of scope"},
                ],
                session_trajectory=["Step one", "Step two"],
            )
            _do_upsert(repo, _base_capsule(), snap)
            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            cont = written["continuity"]
            self.assertEqual(len(cont["negative_decisions"]), 1)
            self.assertEqual(cont["negative_decisions"][0]["decision"], "Skip schema redesign")
            self.assertEqual(cont["session_trajectory"], ["Step one", "Step two"])

    def test_upsert_snapshot_p1_preserve_when_none(self) -> None:
        """P1 fields are preserved from capsule when snapshot has None."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            snap = _snapshot_payload(include_p1=False)
            _do_upsert(repo, _base_capsule(), snap)
            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            cont = written["continuity"]
            # Should still have the base capsule's values
            self.assertEqual(len(cont["negative_decisions"]), 1)
            self.assertEqual(cont["negative_decisions"][0]["decision"], "stale decision")
            self.assertEqual(cont["session_trajectory"], ["stale trajectory step"])

    def test_upsert_snapshot_non_snapshot_fields_preserved(self) -> None:
        """Non-snapshot ContinuityState fields remain from the base capsule."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _do_upsert(repo, _base_capsule(), _snapshot_payload())
            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            cont = written["continuity"]
            self.assertEqual(cont["active_concerns"], ["concern from base"])
            self.assertEqual(cont["drift_signals"], ["drift-a"])
            self.assertEqual(cont["working_hypotheses"], ["hypothesis-a"])
            self.assertEqual(cont["long_horizon_commitments"], ["commitment-a"])
            self.assertEqual(cont["trailing_notes"], ["trailing note"])
            self.assertEqual(cont["curiosity_queue"], ["curiosity item"])

    def test_upsert_snapshot_merged_result_passes_validation(self) -> None:
        """Merged capsule passes standard _validate_capsule rules."""
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule(), _snapshot_payload())
        self.assertTrue(out["ok"])
        self.assertTrue(out["durable"])

    def test_upsert_snapshot_applied_response_fields(self) -> None:
        """Response contains session_end_snapshot_applied and resume_quality."""
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule(), _snapshot_payload())
        self.assertTrue(out["session_end_snapshot_applied"])
        self.assertIn("resume_quality", out)
        self.assertIn("adequate", out["resume_quality"])

    def test_upsert_snapshot_resume_quality_adequate_true(self) -> None:
        """adequate is True when all P0 fields are non-empty and stance >= 30 chars."""
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule(), _snapshot_payload())
        self.assertTrue(out["resume_quality"]["adequate"])

    def test_upsert_snapshot_resume_quality_adequate_false(self) -> None:
        """adequate is False when a P0 field is empty or stance_summary is too short."""
        with tempfile.TemporaryDirectory() as td:
            snap = _snapshot_payload(open_loops=[], stance_summary="Short stance.")
            out = _do_upsert(Path(td), _base_capsule(), snap)
        self.assertFalse(out["resume_quality"]["adequate"])

    def test_upsert_snapshot_field_constraints_match_continuity_state(self) -> None:
        """Snapshot enforces the same max-length constraints as ContinuityState."""
        from pydantic import ValidationError
        from app.models import SessionEndSnapshot

        # session_trajectory items are limited to max 5
        with self.assertRaises(ValidationError):
            SessionEndSnapshot(
                open_loops=["a" * 20],
                top_priorities=["b" * 20],
                active_constraints=["c" * 20],
                stance_summary="d" * 30,
                session_trajectory=["x"] * 6,
            )

        # stance_summary max 240
        with self.assertRaises(ValidationError):
            SessionEndSnapshot(
                open_loops=["a" * 20],
                top_priorities=["b" * 20],
                active_constraints=["c" * 20],
                stance_summary="d" * 241,
            )

    def test_upsert_snapshot_per_item_length_rejected(self) -> None:
        """Per-item string length limits are enforced via _validate_capsule after merge."""
        with tempfile.TemporaryDirectory() as td:
            snap = _snapshot_payload(open_loops=["x" * 161])
            with self.assertRaises(Exception) as ctx:
                _do_upsert(Path(td), _base_capsule(), snap)
            self.assertIn("400", str(ctx.exception.status_code))

    def test_upsert_snapshot_empty_p0_persists_with_adequate_false(self) -> None:
        """Empty P0 lists are accepted (matching ContinuityState) but adequate is False."""
        with tempfile.TemporaryDirectory() as td:
            snap = _snapshot_payload(
                open_loops=[],
                top_priorities=[],
                active_constraints=[],
                stance_summary="Short.",
            )
            out = _do_upsert(Path(td), _base_capsule(), snap)
        self.assertTrue(out["ok"])
        self.assertFalse(out["resume_quality"]["adequate"])

    def test_upsert_snapshot_empty_stance_persists_with_adequate_false(self) -> None:
        """Empty stance_summary is accepted (matching ContinuityState) but adequate is False."""
        with tempfile.TemporaryDirectory() as td:
            snap = _snapshot_payload()
            snap["stance_summary"] = ""
            out = _do_upsert(Path(td), _base_capsule(), snap)
        self.assertTrue(out["ok"])
        self.assertFalse(out["resume_quality"]["adequate"])

    def test_upsert_snapshot_no_change_still_reports_applied(self) -> None:
        """When snapshot merge produces byte-identical capsule, response still reports applied."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # First write: create the capsule via snapshot
            snap = _snapshot_payload()
            out1 = _do_upsert(repo, _base_capsule(), snap)
            self.assertTrue(out1["created"])
            sha1 = out1["capsule_sha256"]

            # Second write: same base capsule + same snapshot = byte-identical
            out2 = _do_upsert(repo, _base_capsule(), snap)
            self.assertTrue(out2["session_end_snapshot_applied"])
            self.assertIn("resume_quality", out2)
            # No change: updated should be False
            self.assertFalse(out2["updated"])
            self.assertEqual(out2["capsule_sha256"], sha1)

    def test_upsert_snapshot_p1_empty_list_overrides(self) -> None:
        """Explicit empty list for P1 field overrides capsule value (not preserved like None)."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            snap = _snapshot_payload(
                session_trajectory=[],
                negative_decisions=[],
            )
            _do_upsert(repo, _base_capsule(), snap)
            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            cont = written["continuity"]
            self.assertEqual(cont["session_trajectory"], [])
            self.assertEqual(cont["negative_decisions"], [])


if __name__ == "__main__":
    unittest.main()
