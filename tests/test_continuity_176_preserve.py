"""Tests for #176 Move A: preserve-by-default upsert merge mode.

Validates that ``merge_mode="preserve"`` on ``ContinuityUpsertRequest``
merges the incoming capsule with the stored capsule based on raw JSON body
inspection, following the field-level semantics defined in spec section 11.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import Settings
from app.continuity.service import continuity_upsert_service
from app.models import ContinuityUpsertRequest


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


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STORED_TS = "2025-06-01T00:00:00Z"
_NEW_TS = "2025-06-02T00:00:00Z"


def _base_capsule(*, updated_at: str | None = None) -> dict[str, Any]:
    """Return a fully populated stored capsule for seeding disk."""
    now = updated_at or _STORED_TS
    return {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "test-agent",
        "updated_at": now,
        "verified_at": now,
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": {
            "top_priorities": ["stored-p1"],
            "active_concerns": ["stored-concern"],
            "active_constraints": ["stored-constraint"],
            "open_loops": ["stored-loop"],
            "stance_summary": "Stored stance summary for testing purposes here",
            "drift_signals": ["stored-drift"],
            "working_hypotheses": ["stored-hypothesis"],
            "long_horizon_commitments": ["stored-commitment"],
            "session_trajectory": ["stored-trajectory"],
            "trailing_notes": ["stored-note"],
            "curiosity_queue": ["stored-curiosity"],
            "negative_decisions": [
                {"decision": "stored-decision", "rationale": "stored-rationale"},
            ],
            "relationship_model": {
                "trust_level": "normal",
                "preferred_style": ["concise"],
            },
            "retrieval_hints": {"must_include": ["summary"]},
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.8},
        "attention_policy": {"early_load": ["passive"]},
        "freshness": {"freshness_class": "durable"},
        "canonical_sources": ["memory/source-a.json", "memory/source-b.json"],
        "metadata": {"env": "test", "version": 1},
        "stable_preferences": [
            {"tag": "pref-a", "content": "Always use dark mode", "last_confirmed_at": now},
        ],
    }


def _seed_capsule(repo_root: Path, capsule: dict[str, Any] | None = None) -> Path:
    """Write a stored capsule to disk and return its path."""
    cap = capsule or _base_capsule()
    cont_dir = repo_root / "memory" / "continuity"
    cont_dir.mkdir(parents=True, exist_ok=True)
    (cont_dir / "fallback").mkdir(exist_ok=True)
    (repo_root / ".locks").mkdir(exist_ok=True)
    path = cont_dir / "user-test-agent.json"
    path.write_text(json.dumps(cap, sort_keys=True), encoding="utf-8")
    return path


def _prepare_dirs(repo_root: Path) -> None:
    """Ensure all required directories exist."""
    (repo_root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
    (repo_root / "memory" / "continuity" / "fallback").mkdir(exist_ok=True)
    (repo_root / ".locks").mkdir(exist_ok=True)


def _do_upsert(
    repo_root: Path,
    capsule_dict: dict[str, Any],
    *,
    merge_mode: str = "replace",
    raw_body: dict[str, Any] | None = None,
    session_end_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a continuity upsert through the service function."""
    req_data: dict[str, Any] = {
        "subject_kind": "user",
        "subject_id": "test-agent",
        "capsule": capsule_dict,
        "merge_mode": merge_mode,
    }
    if session_end_snapshot is not None:
        req_data["session_end_snapshot"] = session_end_snapshot
    req = ContinuityUpsertRequest(**req_data)
    return continuity_upsert_service(
        repo_root=repo_root,
        gm=_GitManagerStub(repo_root),
        auth=_AuthStub(),
        req=req,
        raw_body=raw_body,
        audit=lambda *a: None,
    )


def _incoming_capsule(*, updated_at: str | None = None, **continuity_overrides: Any) -> dict[str, Any]:
    """Build a minimal valid incoming capsule dict."""
    now = updated_at or _NEW_TS
    cont: dict[str, Any] = {
        "top_priorities": [],
        "active_concerns": [],
        "active_constraints": [],
        "open_loops": [],
        "stance_summary": "New stance summary for the incoming capsule test",
        "drift_signals": [],
    }
    cont.update(continuity_overrides)
    return {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "test-agent",
        "updated_at": now,
        "verified_at": now,
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": cont,
        "confidence": {"continuity": 0.8, "relationship_model": 0.7},
    }


def _build_raw_body(
    capsule_raw: dict[str, Any],
    *,
    merge_mode: str = "preserve",
    session_end_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a raw_body dict that mirrors the JSON request."""
    body: dict[str, Any] = {
        "subject_kind": "user",
        "subject_id": "test-agent",
        "merge_mode": merge_mode,
        "capsule": capsule_raw,
    }
    if session_end_snapshot is not None:
        body["session_end_snapshot"] = session_end_snapshot
    return body


def _read_stored(repo_root: Path) -> dict[str, Any]:
    """Read the persisted capsule from disk."""
    path = repo_root / "memory" / "continuity" / "user-test-agent.json"
    return json.loads(path.read_text("utf-8"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPreserveMergeReplace(unittest.TestCase):
    """merge_mode='replace' (default) must behave identically to current behavior."""

    def test_replace_mode_overwrites_all_fields(self) -> None:
        """Replace mode writes the incoming capsule as-is, ignoring stored values."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw = _build_raw_body(cap, merge_mode="replace")
            out = _do_upsert(repo, cap, merge_mode="replace", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            # Required list fields should be empty (from incoming), not stored
            self.assertEqual(written["continuity"]["top_priorities"], [])
            self.assertEqual(written["continuity"]["open_loops"], [])
            # Optional list fields default to empty list in replace mode
            self.assertEqual(written["continuity"]["working_hypotheses"], [])
            # Capsule-level optional object fields absent default to None (excluded by exclude_none)
            self.assertNotIn("attention_policy", written)

    def test_replace_mode_is_default(self) -> None:
        """When merge_mode is not specified, it defaults to replace."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            # No merge_mode or raw_body at all
            out = _do_upsert(repo, cap)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["top_priorities"], [])


class TestPreserveNoStoredCapsule(unittest.TestCase):
    """Preserve mode with no stored capsule behaves like replace."""

    def test_preserve_no_stored_acts_as_replace(self) -> None:
        """When there is no stored capsule, preserve mode creates a new one."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _prepare_dirs(repo)
            cap = _incoming_capsule()
            raw = _build_raw_body(cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            self.assertTrue(out["created"])
            written = _read_stored(repo)
            # Fields should match incoming exactly since no stored capsule
            self.assertEqual(written["continuity"]["top_priorities"], [])
            self.assertEqual(
                written["continuity"]["stance_summary"],
                "New stance summary for the incoming capsule test",
            )


class TestPreserveRequiredListFields(unittest.TestCase):
    """Required list fields: [] preserves stored; non-empty overrides."""

    def test_empty_list_preserves_stored_value(self) -> None:
        """Sending [] for a required list field preserves the stored value."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()  # all required lists are []
            raw = _build_raw_body(cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["top_priorities"], ["stored-p1"])
            self.assertEqual(written["continuity"]["active_concerns"], ["stored-concern"])
            self.assertEqual(written["continuity"]["active_constraints"], ["stored-constraint"])
            self.assertEqual(written["continuity"]["open_loops"], ["stored-loop"])
            self.assertEqual(written["continuity"]["drift_signals"], ["stored-drift"])

    def test_nonempty_list_overrides_stored_value(self) -> None:
        """Sending a non-empty list overrides the stored value."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule(
                top_priorities=["new-p1", "new-p2"],
                open_loops=["new-loop"],
            )
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["top_priorities"], ["new-p1", "new-p2"])
            self.assertEqual(written["continuity"]["open_loops"], ["new-loop"])
            # Other required fields sent as [] should preserve
            self.assertEqual(written["continuity"]["active_concerns"], ["stored-concern"])


class TestPreserveOptionalListFields(unittest.TestCase):
    """Optional list fields: absent preserves; [] overrides to empty; null clears; non-empty overrides."""

    def test_absent_preserves_stored(self) -> None:
        """When an optional list field is absent from raw JSON, stored value is preserved."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            # Build incoming without optional list fields
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = {
                "top_priorities": [],
                "active_concerns": [],
                "active_constraints": [],
                "open_loops": [],
                "stance_summary": "New stance summary for the incoming capsule test",
                "drift_signals": [],
                # working_hypotheses, trailing_notes, etc. are ABSENT
            }
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["working_hypotheses"], ["stored-hypothesis"])
            self.assertEqual(written["continuity"]["long_horizon_commitments"], ["stored-commitment"])
            self.assertEqual(written["continuity"]["session_trajectory"], ["stored-trajectory"])
            self.assertEqual(written["continuity"]["trailing_notes"], ["stored-note"])
            self.assertEqual(written["continuity"]["curiosity_queue"], ["stored-curiosity"])
            self.assertEqual(len(written["continuity"]["negative_decisions"]), 1)
            stored_decision = written["continuity"]["negative_decisions"][0]
            self.assertEqual(stored_decision["decision"], "stored-decision")
            self.assertEqual(stored_decision["rationale"], "stored-rationale")
            self.assertEqual(stored_decision["created_at"], _STORED_TS)
            self.assertEqual(stored_decision["updated_at"], _STORED_TS)

    def test_absent_preserves_stored_related_documents(self) -> None:
        """When related_documents is absent from raw JSON, preserve mode keeps the stored list."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            stored = _base_capsule()
            stored["continuity"]["related_documents"] = [
                {
                    "path": "docs/stored-spec.md",
                    "kind": "spec",
                    "label": "Stored spec",
                    "relevance": "primary",
                }
            ]
            _seed_capsule(repo, stored)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = {
                "top_priorities": [],
                "active_concerns": [],
                "active_constraints": [],
                "open_loops": [],
                "stance_summary": "New stance summary for the incoming capsule test",
                "drift_signals": [],
            }
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(
                written["continuity"]["related_documents"],
                stored["continuity"]["related_documents"],
            )

    def test_empty_list_overrides_to_empty(self) -> None:
        """Sending [] for an optional list field overrides to empty list."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule(working_hypotheses=[], trailing_notes=[])
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            # Ensure these are explicitly present as [] in raw
            raw_cap["continuity"]["working_hypotheses"] = []
            raw_cap["continuity"]["trailing_notes"] = []
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            # These should NOT be in the output (exclude_none strips empty defaults)
            # or should be empty lists
            wh = written["continuity"].get("working_hypotheses", [])
            tn = written["continuity"].get("trailing_notes", [])
            self.assertEqual(wh, [])
            self.assertEqual(tn, [])

    def test_null_clears_to_empty_list(self) -> None:
        """Sending null for an optional list field clears it to []."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw_cap["continuity"]["working_hypotheses"] = None
            raw_cap["continuity"]["curiosity_queue"] = None
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            wh = written["continuity"].get("working_hypotheses", [])
            cq = written["continuity"].get("curiosity_queue", [])
            self.assertEqual(wh, [])
            self.assertEqual(cq, [])

    def test_nonempty_overrides(self) -> None:
        """Sending a non-empty value overrides the stored value."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule(
                working_hypotheses=["new-hyp"],
                trailing_notes=["new-note"],
            )
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["working_hypotheses"], ["new-hyp"])
            self.assertEqual(written["continuity"]["trailing_notes"], ["new-note"])


class TestPreserveOptionalObjectFields(unittest.TestCase):
    """Optional object fields: absent preserves; null clears; present overrides."""

    def test_absent_preserves_stored(self) -> None:
        """When relationship_model/retrieval_hints are absent, stored values preserved."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            # Do NOT include relationship_model or retrieval_hints in raw
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertIsNotNone(written["continuity"].get("relationship_model"))
            self.assertEqual(
                written["continuity"]["relationship_model"]["trust_level"],
                "normal",
            )
            self.assertIsNotNone(written["continuity"].get("retrieval_hints"))

    def test_null_clears_to_none(self) -> None:
        """Sending null for an optional object field clears it."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw_cap["continuity"]["relationship_model"] = None
            raw_cap["continuity"]["retrieval_hints"] = None
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertNotIn("relationship_model", written["continuity"])
            self.assertNotIn("retrieval_hints", written["continuity"])

    def test_present_overrides(self) -> None:
        """Sending a new object value overrides the stored one."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            new_rm = {"trust_level": "high", "preferred_style": ["verbose"]}
            cap = _incoming_capsule(relationship_model=new_rm)
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(
                written["continuity"]["relationship_model"]["trust_level"],
                "high",
            )


class TestPreserveCapsuleLevelFields(unittest.TestCase):
    """Capsule-level fields: absent preserves; null clears; present overrides."""

    def test_absent_preserves_stored_capsule_fields(self) -> None:
        """Capsule-level fields absent from raw preserve stored values."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            # raw_body capsule does NOT include attention_policy, freshness,
            # canonical_sources, metadata, stable_preferences
            raw = _build_raw_body(cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            ap = written.get("attention_policy")
            self.assertIsNotNone(ap)
            self.assertEqual(ap["early_load"], ["passive"])
            fr = written.get("freshness")
            self.assertIsNotNone(fr)
            self.assertEqual(fr["freshness_class"], "durable")
            self.assertEqual(written.get("canonical_sources"), ["memory/source-a.json", "memory/source-b.json"])
            self.assertEqual(written.get("metadata"), {"env": "test", "version": 1})
            self.assertEqual(len(written.get("stable_preferences", [])), 1)
            self.assertEqual(written["stable_preferences"][0]["tag"], "pref-a")

    def test_null_clears_capsule_level_fields(self) -> None:
        """Sending null for capsule-level fields clears them."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["attention_policy"] = None
            raw_cap["freshness"] = None
            raw_cap["canonical_sources"] = None
            raw_cap["metadata"] = None
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertNotIn("attention_policy", written)
            self.assertNotIn("freshness", written)
            # canonical_sources null → [] (list field)
            cs = written.get("canonical_sources", [])
            self.assertEqual(cs, [])
            # metadata null → {} (dict field)
            md = written.get("metadata", {})
            self.assertEqual(md, {})

    def test_present_overrides_capsule_level_fields(self) -> None:
        """Sending new values for capsule-level fields overrides stored."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["canonical_sources"] = ["memory/new-source.json"]
            raw_cap["metadata"] = {"env": "prod"}
            raw_cap["stable_preferences"] = [
                {"tag": "new-pref", "content": "Always show line numbers", "last_confirmed_at": _NEW_TS},
            ]
            raw = _build_raw_body(raw_cap)
            # Also add these to the capsule dict for Pydantic parsing
            cap["canonical_sources"] = ["memory/new-source.json"]
            cap["metadata"] = {"env": "prod"}
            cap["stable_preferences"] = [
                {"tag": "new-pref", "content": "Always show line numbers", "last_confirmed_at": _NEW_TS},
            ]
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["canonical_sources"], ["memory/new-source.json"])
            self.assertEqual(written["metadata"], {"env": "prod"})
            self.assertEqual(len(written["stable_preferences"]), 1)
            self.assertEqual(written["stable_preferences"][0]["tag"], "new-pref")


class TestPreserveThreadDescriptor(unittest.TestCase):
    """thread_descriptor: absent preserves; null clears; present merges sub-fields."""

    def _seed_with_descriptor(self, repo_root: Path) -> None:
        """Seed a stored capsule that has a thread_descriptor."""
        cap = _base_capsule()
        cap["subject_kind"] = "thread"
        cap["subject_id"] = "test-agent"
        cap.pop("stable_preferences", None)
        cap["thread_descriptor"] = {
            "label": "Main thread",
            "keywords": ["stored-kw1", "stored-kw2"],
            "scope_anchors": ["user:stored-scope"],
            "identity_anchors": [{"kind": "email", "value": "test@example.com"}],
            "lifecycle": "active",
        }
        cont_dir = repo_root / "memory" / "continuity"
        cont_dir.mkdir(parents=True, exist_ok=True)
        (cont_dir / "fallback").mkdir(exist_ok=True)
        (repo_root / ".locks").mkdir(exist_ok=True)
        path = cont_dir / "thread-test-agent.json"
        path.write_text(json.dumps(cap, sort_keys=True), encoding="utf-8")

    def _do_thread_upsert(
        self, repo_root: Path, capsule_dict: dict[str, Any], raw_body: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute an upsert for a thread subject."""
        req_data: dict[str, Any] = {
            "subject_kind": "thread",
            "subject_id": "test-agent",
            "capsule": capsule_dict,
            "merge_mode": "preserve",
        }
        req = ContinuityUpsertRequest(**req_data)
        return continuity_upsert_service(
            repo_root=repo_root,
            gm=_GitManagerStub(repo_root),
            auth=_AuthStub(),
            req=req,
            raw_body=raw_body,
            audit=lambda *a: None,
        )

    def test_absent_preserves_entire_descriptor(self) -> None:
        """When thread_descriptor is absent from raw, stored descriptor preserved."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            self._seed_with_descriptor(repo)
            cap = _incoming_capsule()
            cap["subject_kind"] = "thread"
            # Include a thread_descriptor in the Pydantic capsule (required for thread)
            cap["thread_descriptor"] = {
                "label": "Main thread",
                "keywords": ["stored-kw1", "stored-kw2"],
                "scope_anchors": ["user:stored-scope"],
                "identity_anchors": [{"kind": "email", "value": "test@example.com"}],
            }
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            # Do NOT include thread_descriptor in raw → absent
            raw_cap_no_td = {k: v for k, v in raw_cap.items() if k != "thread_descriptor"}
            raw = {
                "subject_kind": "thread",
                "subject_id": "test-agent",
                "merge_mode": "preserve",
                "capsule": raw_cap_no_td,
            }
            out = self._do_thread_upsert(repo, cap, raw)
            self.assertTrue(out["ok"])
            written = json.loads(
                (repo / "memory" / "continuity" / "thread-test-agent.json").read_text("utf-8")
            )
            self.assertIsNotNone(written.get("thread_descriptor"))
            self.assertEqual(written["thread_descriptor"]["label"], "Main thread")
            self.assertEqual(written["thread_descriptor"]["keywords"], ["stored-kw1", "stored-kw2"])

    def test_null_clears_descriptor(self) -> None:
        """Sending null for thread_descriptor clears it."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            self._seed_with_descriptor(repo)
            cap = _incoming_capsule()
            cap["subject_kind"] = "thread"
            # Pydantic capsule with no descriptor
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw_cap["thread_descriptor"] = None
            raw = {
                "subject_kind": "thread",
                "subject_id": "test-agent",
                "merge_mode": "preserve",
                "capsule": raw_cap,
            }
            out = self._do_thread_upsert(repo, cap, raw)
            self.assertTrue(out["ok"])
            written = json.loads(
                (repo / "memory" / "continuity" / "thread-test-agent.json").read_text("utf-8")
            )
            self.assertNotIn("thread_descriptor", written)

    def test_present_merges_subfields(self) -> None:
        """Sending a descriptor with some sub-fields absent merges from stored."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            self._seed_with_descriptor(repo)
            cap = _incoming_capsule()
            cap["subject_kind"] = "thread"
            cap["thread_descriptor"] = {
                "label": "Updated thread",
                "keywords": ["new-kw"],
                # scope_anchors and identity_anchors absent in raw → preserve
            }
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw_cap["thread_descriptor"] = {
                "label": "Updated thread",
                "keywords": ["new-kw"],
                # scope_anchors absent → preserve
                # identity_anchors absent → preserve
            }
            raw = {
                "subject_kind": "thread",
                "subject_id": "test-agent",
                "merge_mode": "preserve",
                "capsule": raw_cap,
            }
            out = self._do_thread_upsert(repo, cap, raw)
            self.assertTrue(out["ok"])
            written = json.loads(
                (repo / "memory" / "continuity" / "thread-test-agent.json").read_text("utf-8")
            )
            td_written = written["thread_descriptor"]
            self.assertEqual(td_written["label"], "Updated thread")
            self.assertEqual(td_written["keywords"], ["new-kw"])
            # Preserved from stored
            self.assertEqual(td_written["scope_anchors"], ["user:stored-scope"])
            self.assertEqual(
                td_written["identity_anchors"],
                [{"kind": "email", "value": "test@example.com"}],
            )


class TestPreserveWithSessionEndSnapshot(unittest.TestCase):
    """session_end_snapshot interaction: snapshot P0/P1 fields override even in preserve mode."""

    def test_snapshot_fields_not_merged_from_stored(self) -> None:
        """Fields touched by snapshot are treated as explicitly provided."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            snapshot = {
                "open_loops": ["snap-loop"],
                "top_priorities": ["snap-priority"],
                "active_constraints": ["snap-constraint"],
                "stance_summary": "Snapshot stance summary that is long enough for validation",
            }
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap, session_end_snapshot=snapshot)
            req_data: dict[str, Any] = {
                "subject_kind": "user",
                "subject_id": "test-agent",
                "capsule": cap,
                "merge_mode": "preserve",
                "session_end_snapshot": snapshot,
            }
            req = ContinuityUpsertRequest(**req_data)
            out = continuity_upsert_service(
                repo_root=repo,
                gm=_GitManagerStub(repo),
                auth=_AuthStub(),
                req=req,
                raw_body=raw,
                audit=lambda *a: None,
            )
            self.assertTrue(out["ok"])
            self.assertTrue(out["session_end_snapshot_applied"])
            written = _read_stored(repo)
            # Snapshot P0 fields override, NOT preserved from stored
            self.assertEqual(written["continuity"]["open_loops"], ["snap-loop"])
            self.assertEqual(written["continuity"]["top_priorities"], ["snap-priority"])
            self.assertEqual(written["continuity"]["active_constraints"], ["snap-constraint"])
            # Non-snapshot fields sent as [] should preserve stored
            self.assertEqual(written["continuity"]["active_concerns"], ["stored-concern"])
            self.assertEqual(written["continuity"]["drift_signals"], ["stored-drift"])

    def test_snapshot_p1_override_in_preserve_mode(self) -> None:
        """P1 snapshot fields override even in preserve mode when provided."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            snapshot = {
                "open_loops": ["snap-loop"],
                "top_priorities": ["snap-priority"],
                "active_constraints": ["snap-constraint"],
                "stance_summary": "Snapshot stance that is long enough for the test",
                "session_trajectory": ["snap-step-1"],
                "negative_decisions": [
                    {"decision": "snap-decision", "rationale": "snap-rationale"},
                ],
            }
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap, session_end_snapshot=snapshot)
            req_data: dict[str, Any] = {
                "subject_kind": "user",
                "subject_id": "test-agent",
                "capsule": cap,
                "merge_mode": "preserve",
                "session_end_snapshot": snapshot,
            }
            req = ContinuityUpsertRequest(**req_data)
            out = continuity_upsert_service(
                repo_root=repo,
                gm=_GitManagerStub(repo),
                auth=_AuthStub(),
                req=req,
                raw_body=raw,
                audit=lambda *a: None,
            )
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["session_trajectory"], ["snap-step-1"])
            self.assertEqual(
                written["continuity"]["negative_decisions"][0]["decision"],
                "snap-decision",
            )
            # Optional list fields NOT in snapshot and absent in raw should preserve
            self.assertEqual(written["continuity"]["working_hypotheses"], ["stored-hypothesis"])
            self.assertEqual(written["continuity"]["trailing_notes"], ["stored-note"])


class TestPreserveWithLifecycleTransition(unittest.TestCase):
    """lifecycle_transition runs correctly after preserve merge."""

    def test_lifecycle_after_preserve_merge(self) -> None:
        """Lifecycle transition applies after preserve merge completes."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Seed a thread capsule with active lifecycle
            cap = _base_capsule()
            cap["subject_kind"] = "thread"
            cap["subject_id"] = "test-thread"
            cap.pop("stable_preferences", None)
            cap["thread_descriptor"] = {
                "label": "Work thread",
                "keywords": ["stored-kw"],
                "lifecycle": "active",
            }
            cont_dir = repo / "memory" / "continuity"
            cont_dir.mkdir(parents=True, exist_ok=True)
            (cont_dir / "fallback").mkdir(exist_ok=True)
            (repo / ".locks").mkdir(exist_ok=True)
            path = cont_dir / "thread-test-thread.json"
            path.write_text(json.dumps(cap, sort_keys=True), encoding="utf-8")

            # Upsert with preserve + lifecycle transition
            new_cap = _incoming_capsule(updated_at=_NEW_TS)
            new_cap["subject_kind"] = "thread"
            new_cap["subject_id"] = "test-thread"
            new_cap["thread_descriptor"] = {"label": "Work thread"}
            raw_cap = dict(new_cap)
            raw_cap["continuity"] = dict(new_cap["continuity"])
            raw = {
                "subject_kind": "thread",
                "subject_id": "test-thread",
                "merge_mode": "preserve",
                "capsule": raw_cap,
            }
            req_data: dict[str, Any] = {
                "subject_kind": "thread",
                "subject_id": "test-thread",
                "capsule": new_cap,
                "merge_mode": "preserve",
                "lifecycle_transition": "suspend",
            }
            req = ContinuityUpsertRequest(**req_data)
            out = continuity_upsert_service(
                repo_root=repo,
                gm=_GitManagerStub(repo),
                auth=_AuthStub(),
                req=req,
                raw_body=raw,
                audit=lambda *a: None,
            )
            self.assertTrue(out["ok"])
            written = json.loads(path.read_text("utf-8"))
            self.assertEqual(written["thread_descriptor"]["lifecycle"], "suspended")
            # Preserved required list fields
            self.assertEqual(written["continuity"]["top_priorities"], ["stored-p1"])


class TestPreserveRequiredFieldsAlwaysPresent(unittest.TestCase):
    """stance_summary, source, confidence are always required even in preserve mode."""

    def test_stance_summary_required(self) -> None:
        """stance_summary must always be present in the incoming capsule."""
        from pydantic import ValidationError

        cap_data = _incoming_capsule()
        del cap_data["continuity"]["stance_summary"]
        with self.assertRaises(ValidationError):
            ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="test-agent",
                capsule=cap_data,
                merge_mode="preserve",
            )

    def test_source_required(self) -> None:
        """source must always be present in the incoming capsule."""
        from pydantic import ValidationError

        cap_data = _incoming_capsule()
        del cap_data["source"]
        with self.assertRaises(ValidationError):
            ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="test-agent",
                capsule=cap_data,
                merge_mode="preserve",
            )

    def test_confidence_required(self) -> None:
        """confidence must always be present in the incoming capsule."""
        from pydantic import ValidationError

        cap_data = _incoming_capsule()
        del cap_data["confidence"]
        with self.assertRaises(ValidationError):
            ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="test-agent",
                capsule=cap_data,
                merge_mode="preserve",
            )


class TestPreservePostMergeValidation(unittest.TestCase):
    """Post-merge capsule passes full validation."""

    def test_merged_capsule_passes_validation(self) -> None:
        """A preserve-merged capsule that combines stored + incoming passes all checks."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            self.assertTrue(out["durable"])

    def test_preserve_merge_multiple_field_types(self) -> None:
        """Exercise a mix of preserved and overridden fields across all categories."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule(
                top_priorities=["new-p1"],          # required list: override
                working_hypotheses=["new-hyp"],     # optional list: override
            )
            cap["canonical_sources"] = ["memory/new-src.json"]  # capsule list: override
            raw_cap = dict(cap)
            raw_cap["continuity"] = dict(cap["continuity"])
            # open_loops [] → preserve, trailing_notes absent → preserve
            # relationship_model absent → preserve
            # attention_policy absent → preserve
            # metadata absent → preserve
            raw = _build_raw_body(raw_cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            # Overridden
            self.assertEqual(written["continuity"]["top_priorities"], ["new-p1"])
            self.assertEqual(written["continuity"]["working_hypotheses"], ["new-hyp"])
            self.assertEqual(written["canonical_sources"], ["memory/new-src.json"])
            # Preserved
            self.assertEqual(written["continuity"]["open_loops"], ["stored-loop"])
            self.assertEqual(written["continuity"]["trailing_notes"], ["stored-note"])
            self.assertIsNotNone(written["continuity"].get("relationship_model"))
            ap = written.get("attention_policy")
            self.assertIsNotNone(ap)
            self.assertEqual(ap["early_load"], ["passive"])
            self.assertEqual(written.get("metadata"), {"env": "test", "version": 1})

    def test_preserve_without_raw_body_acts_as_replace(self) -> None:
        """If raw_body is None (e.g. internal call), preserve mode degrades to replace."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            # No raw_body provided
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=None)
            self.assertTrue(out["ok"])
            written = _read_stored(repo)
            # Without raw_body, merge cannot inspect intent, so incoming wins
            self.assertEqual(written["continuity"]["top_priorities"], [])

    def test_preserve_idempotent_no_change(self) -> None:
        """Preserve upsert with identical content reports no update."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            # First upsert with preserve
            cap = _incoming_capsule()
            raw = _build_raw_body(cap)
            out1 = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            sha1 = out1["capsule_sha256"]
            # Second identical upsert
            out2 = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            self.assertFalse(out2["updated"])
            self.assertEqual(out2["capsule_sha256"], sha1)


class TestPreserveStanceSummaryAlwaysIncoming(unittest.TestCase):
    """M3: stance_summary is never preserved — always taken from incoming capsule."""

    def test_stance_summary_always_from_incoming(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _seed_capsule(repo)
            cap = _incoming_capsule()
            raw = _build_raw_body(cap)
            _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            written = _read_stored(repo)
            # Incoming stance_summary must override stored, not preserve.
            self.assertEqual(
                written["continuity"]["stance_summary"],
                "New stance summary for the incoming capsule test",
            )
            self.assertNotEqual(
                written["continuity"]["stance_summary"],
                "Stored stance summary for testing purposes here",
            )


class TestPreserveFieldNormalizationReporting(unittest.TestCase):
    """M4: preserved fields that need normalization report accurately."""

    def test_preserved_field_with_whitespace_reports_normalization(self) -> None:
        """Stored value with trailing space is restored then normalized."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            # Seed a capsule with whitespace-padded open_loops
            stored = _base_capsule()
            stored["continuity"]["open_loops"] = ["stored-loop ", " stored-loop"]
            _seed_capsule(repo, stored)
            # Incoming sends [] for required list → preserve stored
            cap = _incoming_capsule()
            raw = _build_raw_body(cap)
            out = _do_upsert(repo, cap, merge_mode="preserve", raw_body=raw)
            # Normalization should have stripped and deduped
            norms = out.get("normalizations_applied", [])
            self.assertTrue(
                any("strip:continuity.open_loops" in n for n in norms),
                f"Expected strip normalization in {norms}",
            )
            self.assertTrue(
                any("dedup:continuity.open_loops" in n for n in norms),
                f"Expected dedup normalization in {norms}",
            )
            written = _read_stored(repo)
            self.assertEqual(written["continuity"]["open_loops"], ["stored-loop"])


if __name__ == "__main__":
    unittest.main()
