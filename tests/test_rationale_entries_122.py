"""Tests for #122: rationale and decision continuity capture on continuity capsules."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.continuity.cold import _build_cold_stub_text, _render_cold_rationale_entries
from app.continuity.constants import CONTINUITY_COLD_STUB_FRONTMATTER_ORDER, CONTINUITY_COLD_STUB_SECTION_ORDER
from app.continuity.paths import continuity_cold_storage_rel_path, continuity_cold_stub_rel_path
from app.continuity.service import continuity_compare_service, continuity_revalidate_service
from app.continuity.trimming import _estimated_tokens, _render_value, _trim_capsule
from app.continuity.trust import _build_startup_summary
from app.continuity.validation import _validate_capsule
from app.main import continuity_list, continuity_read, continuity_upsert
from app.models import (
    ContinuityCapsule,
    ContinuityCompareRequest,
    ContinuityListRequest,
    ContinuityReadRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
    RationaleEntry,
)
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


class _AuthStub(AllowAllAuthStub):
    """Auth stub permitting all scopes for rationale-entries tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub recording committed files."""

    def __init__(self, repo_root: Path | None = None) -> None:
        super().__init__(repo_root)
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


def _sample_entry(
    *,
    tag: str = "auth_choice",
    kind: str = "decision",
    status: str = "active",
    summary: str = "Chose JWT over sessions",
    reasoning: str = "Stateless, simpler to scale horizontally",
    alternatives: list[str] | None = None,
    depends_on: list[str] | None = None,
    supersedes: str | None = None,
) -> dict:
    """Return a single sample rationale entry dict."""
    entry: dict = {
        "tag": tag,
        "kind": kind,
        "status": status,
        "summary": summary,
        "reasoning": reasoning,
        "last_confirmed_at": _now_iso(),
    }
    if alternatives is not None:
        entry["alternatives_considered"] = alternatives
    if depends_on is not None:
        entry["depends_on"] = depends_on
    if supersedes is not None:
        entry["supersedes"] = supersedes
    return entry


def _sample_entries(n: int = 2) -> list[dict]:
    """Return n sample rationale entry dicts with unique tags."""
    tags = ["auth_choice", "db_engine", "cache_strategy", "deploy_target", "api_style", "test_framework"]
    summaries = [
        "Chose JWT over sessions",
        "PostgreSQL over MongoDB",
        "Redis caching layer added",
        "Deploy to k8s over bare metal",
        "REST over GraphQL",
        "pytest over unittest",
    ]
    reasonings = [
        "Stateless, simpler to scale horizontally",
        "Relational model fits domain, strong ecosystem",
        "Reduces DB load for hot-path reads",
        "Team already has k8s expertise",
        "Simpler for current API surface",
        "Better fixture support and plugin ecosystem",
    ]
    return [
        _sample_entry(tag=tags[i], summary=summaries[i], reasoning=reasonings[i])
        for i in range(n)
    ]


def _base_capsule_payload(
    *,
    subject_kind: str = "user",
    subject_id: str = "test-agent",
    rationale_entries: list[dict] | None = None,
) -> dict:
    """Return a valid baseline capsule dict."""
    now = _now_iso()
    continuity: dict = {
        "top_priorities": ["priority one"],
        "active_concerns": ["concern one"],
        "active_constraints": ["constraint one"],
        "open_loops": ["loop one"],
        "stance_summary": "Current stance text for testing purposes.",
        "drift_signals": [],
    }
    if rationale_entries is not None:
        continuity["rationale_entries"] = rationale_entries
    payload: dict = {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "test-hook",
            "update_reason": "pre_compaction",
            "inputs": [],
        },
        "continuity": continuity,
        "confidence": {"continuity": 0.85, "relationship_model": 0.0},
    }
    return payload


def _write_capsule(repo_root: Path, payload: dict) -> None:
    kind = payload["subject_kind"]
    sid = payload["subject_id"].strip().lower().replace(" ", "-")
    d = repo_root / "memory" / "continuity"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{kind}-{sid}.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_fallback(repo_root: Path, capsule: dict) -> None:
    kind = capsule["subject_kind"]
    sid = capsule["subject_id"].strip().lower().replace(" ", "-")
    d = repo_root / "memory" / "continuity" / "fallback"
    d.mkdir(parents=True, exist_ok=True)
    envelope = {
        "schema_type": "continuity_fallback_snapshot",
        "schema_version": "1.0",
        "captured_at": capsule["updated_at"],
        "source_path": f"memory/continuity/{kind}-{sid}.json",
        "verification_status": "unverified",
        "health_status": "unknown",
        "capsule": capsule,
    }
    (d / f"{kind}-{sid}.json").write_text(json.dumps(envelope), encoding="utf-8")


def _write_cold_stub(repo_root: Path, *, subject_id: str = "test-agent") -> str:
    """Write a valid cold stub and its referenced archive, return the stub rel path."""
    import gzip

    now = _now_iso()
    source_archive_path = f"memory/continuity/archive/user-{subject_id}-20260328T120000Z.json"
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    stub_rel = continuity_cold_stub_rel_path(source_archive_path)
    stub_path = repo_root / stub_rel
    stub_path.parent.mkdir(parents=True, exist_ok=True)
    frontmatter_values = {
        "type": "continuity_cold_stub",
        "schema_version": '"1.0"',
        "artifact_state": "cold",
        "subject_kind": "user",
        "subject_id": subject_id,
        "source_archive_path": source_archive_path,
        "cold_storage_path": cold_storage_path,
        "archived_at": now,
        "cold_stored_at": now,
        "verification_kind": "self_review",
        "verification_status": "self_attested",
        "health_status": "healthy",
        "freshness_class": "current",
        "phase": "current",
        "update_reason": "pre_compaction",
    }
    lines = ["---"]
    for key in CONTINUITY_COLD_STUB_FRONTMATTER_ORDER:
        lines.append(f"{key}: {frontmatter_values[key]}")
    lines.append("---")
    lines.append("## top_priorities")
    lines.append("- priority one")
    lines.append("")
    stub_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    gz_path = repo_root / cold_storage_path
    gz_path.parent.mkdir(parents=True, exist_ok=True)
    gz_path.write_bytes(gzip.compress(b"{}"))
    return stub_rel


# ---------------------------------------------------------------------------
# Unit tests: RationaleEntry model
# ---------------------------------------------------------------------------

class TestRationaleEntryModel(unittest.TestCase):
    """Validate RationaleEntry Pydantic model constraints."""

    def test_valid_construction(self) -> None:
        e = RationaleEntry(
            tag="auth_choice", kind="decision", status="active",
            summary="Chose JWT", reasoning="Stateless scaling", last_confirmed_at=_now_iso(),
        )
        self.assertEqual(e.tag, "auth_choice")
        self.assertEqual(e.kind, "decision")
        self.assertEqual(e.status, "active")

    def test_all_kinds_accepted(self) -> None:
        for kind in ("decision", "assumption", "tension"):
            e = RationaleEntry(
                tag="t", kind=kind, status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )
            self.assertEqual(e.kind, kind)

    def test_all_statuses_accepted(self) -> None:
        for status in ("active", "superseded", "retired"):
            e = RationaleEntry(
                tag="t", kind="decision", status=status,
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )
            self.assertEqual(e.status, status)

    def test_invalid_kind_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="t", kind="unknown", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )

    def test_invalid_status_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="t", kind="decision", status="archived",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )

    def test_tag_too_long(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="x" * 81, kind="decision", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )

    def test_empty_tag_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="", kind="decision", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            )

    def test_max_length_tag_accepted(self) -> None:
        e = RationaleEntry(
            tag="t" * 80, kind="decision", status="active",
            summary="s", reasoning="r", last_confirmed_at=_now_iso(),
        )
        self.assertEqual(len(e.tag), 80)

    def test_alternatives_max_3_accepted(self) -> None:
        e = RationaleEntry(
            tag="t", kind="decision", status="active",
            summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            alternatives_considered=["a", "b", "c"],
        )
        self.assertEqual(len(e.alternatives_considered), 3)

    def test_alternatives_4_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="t", kind="decision", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
                alternatives_considered=["a", "b", "c", "d"],
            )

    def test_depends_on_max_3_accepted(self) -> None:
        e = RationaleEntry(
            tag="t", kind="assumption", status="active",
            summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            depends_on=["x", "y", "z"],
        )
        self.assertEqual(len(e.depends_on), 3)

    def test_depends_on_4_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="t", kind="assumption", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
                depends_on=["a", "b", "c", "d"],
            )

    def test_supersedes_max_length_accepted(self) -> None:
        e = RationaleEntry(
            tag="t", kind="decision", status="active",
            summary="s", reasoning="r", last_confirmed_at=_now_iso(),
            supersedes="s" * 80,
        )
        self.assertEqual(len(e.supersedes), 80)

    def test_supersedes_too_long_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            RationaleEntry(
                tag="t", kind="decision", status="active",
                summary="s", reasoning="r", last_confirmed_at=_now_iso(),
                supersedes="s" * 81,
            )

    def test_defaults_empty_lists(self) -> None:
        e = RationaleEntry(
            tag="t", kind="decision", status="active",
            summary="s", reasoning="r", last_confirmed_at=_now_iso(),
        )
        self.assertEqual(e.alternatives_considered, [])
        self.assertEqual(e.depends_on, [])
        self.assertIsNone(e.supersedes)


# ---------------------------------------------------------------------------
# Unit tests: ContinuityState.rationale_entries field
# ---------------------------------------------------------------------------

class TestContinuityStateRationaleField(unittest.TestCase):
    """Validate rationale_entries field on ContinuityState."""

    def test_default_empty_list(self) -> None:
        payload = _base_capsule_payload()
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(capsule.continuity.rationale_entries, [])

    def test_max_6_accepted(self) -> None:
        payload = _base_capsule_payload(rationale_entries=_sample_entries(6))
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(len(capsule.continuity.rationale_entries), 6)

    def test_7_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            ContinuityCapsule(**_base_capsule_payload(rationale_entries=_sample_entries(6) + [_sample_entry(tag="extra")]))

    def test_backward_compat_missing_key(self) -> None:
        """Capsule dict without rationale_entries loads with empty list default."""
        payload = _base_capsule_payload()
        payload["continuity"].pop("rationale_entries", None)
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(capsule.continuity.rationale_entries, [])

    def test_all_subject_kinds_allowed(self) -> None:
        """Unlike stable_preferences, rationale_entries is allowed on all subject_kinds."""
        for kind in ("user", "peer", "thread", "task"):
            with tempfile.TemporaryDirectory() as td:
                payload = _base_capsule_payload(
                    subject_kind=kind,
                    rationale_entries=_sample_entries(1),
                )
                capsule = ContinuityCapsule(**payload)
                result, _ = _validate_capsule(Path(td), capsule)
                self.assertEqual(len(result["continuity"]["rationale_entries"]), 1)


# ---------------------------------------------------------------------------
# Unit tests: _validate_capsule — rationale entries
# ---------------------------------------------------------------------------

class TestValidateRationaleEntries(unittest.TestCase):
    """Validate _validate_capsule checks for rationale_entries."""

    def test_valid_entries_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=_sample_entries(3))
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"]), 3)

    def test_duplicate_tags_rejected(self) -> None:
        entries = [
            _sample_entry(tag="dup"),
            _sample_entry(tag="dup", summary="different"),
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("Duplicate", str(ctx.exception.detail))

    def test_summary_too_long_rejected(self) -> None:
        payload = _base_capsule_payload(rationale_entries=[
            _sample_entry(summary="x" * 321),
        ])
        with self.assertRaises(ValidationError):
            ContinuityCapsule(**payload)

    def test_summary_empty_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(summary=""),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too short", str(ctx.exception.detail).lower())

    def test_reasoning_too_long_rejected(self) -> None:
        payload = _base_capsule_payload(rationale_entries=[
            _sample_entry(reasoning="x" * 561),
        ])
        with self.assertRaises(ValidationError):
            ContinuityCapsule(**payload)

    def test_reasoning_empty_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(reasoning=""),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too short", str(ctx.exception.detail).lower())

    def test_alternative_too_long_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(alternatives=["x" * 161]),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too long", str(ctx.exception.detail).lower())

    def test_alternative_empty_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(alternatives=[""]),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too short", str(ctx.exception.detail).lower())

    def test_depends_on_too_long_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(depends_on=["x" * 121]),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too long", str(ctx.exception.detail).lower())

    def test_depends_on_empty_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(depends_on=[""]),
            ])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too short", str(ctx.exception.detail).lower())

    def test_tag_too_long_service_layer(self) -> None:
        """Service-layer guard rejects tag > 80 chars independently of Pydantic."""
        entry = _sample_entry()
        # Bypass Pydantic by constructing a valid capsule then mutating the model
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[entry])
            capsule = ContinuityCapsule(**payload)
            # Force an overlong tag past Pydantic
            capsule.continuity.rationale_entries[0].tag = "x" * 81
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too long", ctx.exception.detail.lower())
            self.assertIn("tag", ctx.exception.detail.lower())

    def test_tag_empty_service_layer(self) -> None:
        """Service-layer guard rejects empty tag independently of Pydantic."""
        entry = _sample_entry()
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[entry])
            capsule = ContinuityCapsule(**payload)
            capsule.continuity.rationale_entries[0].tag = ""
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("too short", ctx.exception.detail.lower())
            self.assertIn("tag", ctx.exception.detail.lower())

    def test_invalid_last_confirmed_at_rejected(self) -> None:
        entry = _sample_entry()
        entry["last_confirmed_at"] = "not-a-timestamp"
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[entry])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_non_utc_last_confirmed_at_rejected(self) -> None:
        entry = _sample_entry()
        entry["last_confirmed_at"] = "2026-03-20T10:00:00+02:00"
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[entry])
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_valid_supersedes_accepted(self) -> None:
        """An entry superseding another with status superseded must be accepted."""
        entries = [
            _sample_entry(tag="old_choice", status="superseded"),
            _sample_entry(tag="new_choice", supersedes="old_choice"),
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"]), 2)

    def test_supersedes_missing_tag_rejected(self) -> None:
        """supersedes referencing a non-existent tag must be rejected."""
        entries = [
            _sample_entry(tag="new_choice", supersedes="nonexistent"),
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("supersedes", str(ctx.exception.detail))

    def test_supersedes_non_superseded_status_rejected(self) -> None:
        """supersedes referencing a tag with status active must be rejected."""
        entries = [
            _sample_entry(tag="old_choice", status="active"),
            _sample_entry(tag="new_choice", supersedes="old_choice"),
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("supersedes", str(ctx.exception.detail))

    def test_supersedes_retired_status_rejected(self) -> None:
        """supersedes referencing a tag with status retired must be rejected."""
        entries = [
            _sample_entry(tag="old_choice", status="retired"),
            _sample_entry(tag="new_choice", supersedes="old_choice"),
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_max_length_summary_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(summary="s" * 320),
            ])
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"][0]["summary"]), 320)

    def test_max_length_reasoning_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(reasoning="r" * 560),
            ])
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"][0]["reasoning"]), 560)

    def test_max_length_alternative_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(alternatives=["a" * 160]),
            ])
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"][0]["alternatives_considered"][0]), 160)

    def test_max_length_depends_on_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(rationale_entries=[
                _sample_entry(depends_on=["d" * 120]),
            ])
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"][0]["depends_on"][0]), 120)


# ---------------------------------------------------------------------------
# Unit tests: _trim_capsule
# ---------------------------------------------------------------------------

class TestTrimRationaleEntries(unittest.TestCase):
    """Validate rationale_entries trim behaviour."""

    def _capsule_with_entries(self, n: int = 4) -> dict:
        payload = _base_capsule_payload(rationale_entries=_sample_entries(n))
        capsule = ContinuityCapsule(**payload)
        return capsule.model_dump(mode="json", exclude_none=True)

    def test_generous_budget_keeps_entries(self) -> None:
        capsule = self._capsule_with_entries()
        trimmed, dropped = _trim_capsule(capsule, 4000)
        self.assertIsNotNone(trimmed)
        entries = trimmed["continuity"]["rationale_entries"]
        self.assertEqual(len(entries), 4)
        self.assertNotIn("continuity.rationale_entries", dropped)

    def test_tight_budget_drops_entries_as_unit(self) -> None:
        """rationale_entries must be dropped entirely (all-or-nothing)."""
        capsule = self._capsule_with_entries()
        full_tokens = _estimated_tokens(_render_value(capsule))
        found_drop = False
        for budget in range(full_tokens, 0, -5):
            trimmed, dropped = _trim_capsule(capsule, budget)
            if trimmed is None:
                break
            if "continuity.rationale_entries" in dropped:
                self.assertNotIn("rationale_entries", trimmed.get("continuity", {}))
                found_drop = True
                break
            self.assertEqual(
                len(trimmed.get("continuity", {}).get("rationale_entries", [])), 4,
            )
        self.assertTrue(found_drop, "rationale_entries was never dropped during budget walk")

    def test_drop_order_after_curiosity_queue(self) -> None:
        """rationale_entries must be trimmed after curiosity_queue."""
        capsule = self._capsule_with_entries()
        capsule["continuity"]["curiosity_queue"] = ["curious " * 15] * 5
        full_tokens = _estimated_tokens(_render_value(capsule))
        cq_dropped_at = None
        re_dropped_at = None
        for budget in range(full_tokens, 0, -5):
            trimmed, dropped = _trim_capsule(capsule, budget)
            if trimmed is None:
                break
            if "continuity.curiosity_queue" in dropped and cq_dropped_at is None:
                cq_dropped_at = budget
            if "continuity.rationale_entries" in dropped and re_dropped_at is None:
                re_dropped_at = budget
            if cq_dropped_at is not None and re_dropped_at is not None:
                break
        self.assertIsNotNone(cq_dropped_at, "curiosity_queue was never dropped")
        self.assertIsNotNone(re_dropped_at, "rationale_entries was never dropped")
        self.assertGreaterEqual(cq_dropped_at, re_dropped_at)

    def test_drop_order_before_negative_decisions(self) -> None:
        """rationale_entries must be trimmed before negative_decisions."""
        capsule = self._capsule_with_entries()
        capsule["continuity"]["negative_decisions"] = [
            {"decision": "reject " * 20, "rationale": "reason " * 30}
        ] * 4
        full_tokens = _estimated_tokens(_render_value(capsule))
        re_dropped_at = None
        nd_dropped_at = None
        for budget in range(full_tokens, 0, -5):
            trimmed, dropped = _trim_capsule(capsule, budget)
            if trimmed is None:
                break
            if "continuity.rationale_entries" in dropped and re_dropped_at is None:
                re_dropped_at = budget
            if "continuity.negative_decisions" in dropped and nd_dropped_at is None:
                nd_dropped_at = budget
            if re_dropped_at is not None and nd_dropped_at is not None:
                break
        self.assertIsNotNone(re_dropped_at, "rationale_entries was never dropped")
        self.assertIsNotNone(nd_dropped_at, "negative_decisions was never dropped")
        self.assertGreaterEqual(re_dropped_at, nd_dropped_at)

    def test_trimmed_fields_includes_rationale_entries(self) -> None:
        """When trimmed to minimal budget, rationale_entries must appear in dropped set."""
        capsule = self._capsule_with_entries()
        _, dropped = _trim_capsule(capsule, 10)
        self.assertIn("continuity.rationale_entries", dropped)


# ---------------------------------------------------------------------------
# Unit tests: _build_startup_summary
# ---------------------------------------------------------------------------

class TestStartupSummaryRationaleEntries(unittest.TestCase):
    """Validate rationale_entries in startup summary."""

    def test_active_entries_included(self) -> None:
        entries = [
            _sample_entry(tag="a", status="active"),
            _sample_entry(tag="b", status="superseded"),
            _sample_entry(tag="c", status="retired"),
            _sample_entry(tag="d", status="active"),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        re = summary["orientation"]["rationale_entries"]
        self.assertEqual(len(re), 2)
        tags = {r["tag"] for r in re}
        self.assertEqual(tags, {"a", "d"})

    def test_all_superseded_retired_filtered(self) -> None:
        entries = [
            _sample_entry(tag="a", status="superseded"),
            _sample_entry(tag="b", status="retired"),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        self.assertEqual(summary["orientation"]["rationale_entries"], [])

    def test_empty_when_no_entries(self) -> None:
        payload = _base_capsule_payload()
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        self.assertEqual(summary["orientation"]["rationale_entries"], [])

    def test_null_when_capsule_missing(self) -> None:
        out = {
            "capsule": None,
            "source_state": "missing",
            "recovery_warnings": [],
            "trust_signals": None,
        }
        summary = _build_startup_summary(out)
        self.assertIsNone(summary["orientation"])

    def test_entries_are_dicts_not_models(self) -> None:
        """Startup summary entries must be plain dicts (shallow copy)."""
        entries = [_sample_entry(tag="x", status="active")]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        self.assertIsInstance(summary["orientation"]["rationale_entries"][0], dict)


# ---------------------------------------------------------------------------
# Integration tests: upsert + read round trip
# ---------------------------------------------------------------------------

class TestUpsertReadRoundtrip(unittest.TestCase):
    """Validate rationale_entries survive upsert -> read cycle."""

    def _do_upsert(self, repo_root: Path, payload: dict, snapshot: dict | None = None) -> dict:
        settings = _settings(repo_root)
        gm = _GitManagerStub(repo_root)
        req_data: dict = {
            "subject_kind": payload["subject_kind"],
            "subject_id": payload["subject_id"],
            "capsule": payload,
        }
        if snapshot is not None:
            req_data["session_end_snapshot"] = snapshot
        req = ContinuityUpsertRequest(**req_data)
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_upsert(req=req, auth=_AuthStub())

    def _do_read(self, repo_root: Path, subject_kind: str = "user",
                 subject_id: str = "test-agent", view: str | None = None) -> dict:
        settings = _settings(repo_root)
        gm = _GitManagerStub(repo_root)
        req = ContinuityReadRequest(subject_kind=subject_kind, subject_id=subject_id, view=view)
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_read(req=req, auth=_AuthStub())

    def test_roundtrip_preserves_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(3))
            self._do_upsert(repo, payload)
            out = self._do_read(repo)
            self.assertTrue(out["ok"])
            entries = out["capsule"]["continuity"]["rationale_entries"]
            self.assertEqual(len(entries), 3)
            tags = {e["tag"] for e in entries}
            self.assertEqual(tags, {"auth_choice", "db_engine", "cache_strategy"})

    def test_modify_entries_returns_latest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            self._do_upsert(repo, payload)

            later = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            payload["updated_at"] = later
            payload["verified_at"] = later
            payload["continuity"]["rationale_entries"] = [
                _sample_entry(tag="new_tag", summary="New decision"),
            ]
            self._do_upsert(repo, payload)

            out = self._do_read(repo)
            entries = out["capsule"]["continuity"]["rationale_entries"]
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["tag"], "new_tag")

    def test_clear_to_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            self._do_upsert(repo, payload)

            later = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            payload["updated_at"] = later
            payload["verified_at"] = later
            payload["continuity"]["rationale_entries"] = []
            self._do_upsert(repo, payload)

            out = self._do_read(repo)
            self.assertEqual(out["capsule"]["continuity"]["rationale_entries"], [])

    def test_startup_view_filters_active_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            entries = [
                _sample_entry(tag="active_one", status="active"),
                _sample_entry(tag="old_one", status="superseded"),
                _sample_entry(tag="done_one", status="retired"),
            ]
            payload = _base_capsule_payload(rationale_entries=entries)
            _write_capsule(repo, payload)
            out = self._do_read(repo, view="startup")
            self.assertIn("startup_summary", out)
            re = out["startup_summary"]["orientation"]["rationale_entries"]
            self.assertEqual(len(re), 1)
            self.assertEqual(re[0]["tag"], "active_one")


# ---------------------------------------------------------------------------
# Integration tests: session-end snapshot
# ---------------------------------------------------------------------------

class TestSessionEndSnapshotRationale(unittest.TestCase):
    """Validate session-end snapshot merge for rationale_entries."""

    def _do_upsert(self, repo_root: Path, payload: dict, snapshot: dict | None = None) -> dict:
        settings = _settings(repo_root)
        gm = _GitManagerStub(repo_root)
        req_data: dict = {
            "subject_kind": payload["subject_kind"],
            "subject_id": payload["subject_id"],
            "capsule": payload,
        }
        if snapshot is not None:
            req_data["session_end_snapshot"] = snapshot
        req = ContinuityUpsertRequest(**req_data)
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_upsert(req=req, auth=_AuthStub())

    def _snapshot_payload(self, rationale_entries: list[dict] | None = None) -> dict:
        snap: dict = {
            "open_loops": ["fresh loop"],
            "top_priorities": ["fresh priority"],
            "active_constraints": ["fresh constraint"],
            "stance_summary": "Fresh approach for testing session end.",
        }
        if rationale_entries is not None:
            snap["rationale_entries"] = rationale_entries
        return snap

    def test_p1_override_when_present(self) -> None:
        """Explicit rationale_entries in snapshot override capsule values."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            new_entries = [_sample_entry(tag="snapshot_entry", summary="From snapshot")]
            out = self._do_upsert(repo, payload, self._snapshot_payload(rationale_entries=new_entries))
            self.assertTrue(out["ok"])

            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            entries = written["continuity"]["rationale_entries"]
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["tag"], "snapshot_entry")

    def test_snapshot_override_stamps_child_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(1))
            new_entries = [_sample_entry(tag="snapshot_entry", summary="From snapshot")]
            new_entries[0]["last_confirmed_at"] = "2026-03-22T09:15:00Z"
            self._do_upsert(repo, payload, self._snapshot_payload(rationale_entries=new_entries))

            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            stamped = written["continuity"]["rationale_entries"][0]
            self.assertEqual(stamped["created_at"], written["updated_at"])
            self.assertEqual(stamped["updated_at"], written["updated_at"])
            self.assertEqual(stamped["last_confirmed_at"], "2026-03-22T09:15:00Z")

    def test_p1_preserve_when_none(self) -> None:
        """When snapshot has no rationale_entries field, capsule values are preserved."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            out = self._do_upsert(repo, payload, self._snapshot_payload())
            self.assertTrue(out["ok"])

            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            entries = written["continuity"]["rationale_entries"]
            self.assertEqual(len(entries), 2)

    def test_p1_clear_with_empty_list(self) -> None:
        """Explicit empty list in snapshot clears rationale_entries."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            out = self._do_upsert(repo, payload, self._snapshot_payload(rationale_entries=[]))
            self.assertTrue(out["ok"])

            written = json.loads(
                (repo / "memory" / "continuity" / "user-test-agent.json").read_text("utf-8")
            )
            self.assertEqual(written["continuity"]["rationale_entries"], [])


# ---------------------------------------------------------------------------
# Integration tests: list endpoint summary
# ---------------------------------------------------------------------------

class TestListSummaryRationaleEntryCount(unittest.TestCase):
    """Validate rationale_entry_count on list summary entries."""

    def test_active_with_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(3))
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["capsules"][0]["rationale_entry_count"], 3)

    def test_active_without_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload()
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["capsules"][0]["rationale_entry_count"], 0)

    def test_fallback_includes_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            _write_fallback(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user", include_fallback=True),
                    auth=_AuthStub(),
                )
            fallback_entries = [e for e in out["capsules"] if e["artifact_state"] == "fallback"]
            self.assertTrue(len(fallback_entries) > 0)
            self.assertEqual(fallback_entries[0]["rationale_entry_count"], 2)

    def test_cold_stub_null_count(self) -> None:
        """Cold stubs do not contain the full capsule; count must be null."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            _write_cold_stub(repo)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user", include_cold=True),
                    auth=_AuthStub(),
                )
            cold_entries = [e for e in out["capsules"] if e["artifact_state"] == "cold"]
            self.assertEqual(len(cold_entries), 1)
            self.assertIsNone(cold_entries[0]["rationale_entry_count"])


# ---------------------------------------------------------------------------
# Integration tests: fallback read
# ---------------------------------------------------------------------------

class TestFallbackPreservesRationale(unittest.TestCase):
    """Validate fallback read returns rationale_entries."""

    def test_fallback_read_preserves_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=_sample_entries(2))
            _write_fallback(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            req = ContinuityReadRequest(
                subject_kind="user", subject_id="test-agent", allow_fallback=True,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(req=req, auth=_AuthStub())
            self.assertTrue(out["ok"])
            self.assertEqual(out["source_state"], "fallback")
            entries = out["capsule"]["continuity"]["rationale_entries"]
            self.assertEqual(len(entries), 2)


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------

class TestBackwardCompatibility(unittest.TestCase):
    """Ensure pre-feature capsules load cleanly."""

    def test_legacy_capsule_without_rationale_entries(self) -> None:
        """A capsule JSON without rationale_entries must load with empty list."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload()
            payload["continuity"].pop("rationale_entries", None)
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            req = ContinuityReadRequest(subject_kind="user", subject_id="test-agent")
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(req=req, auth=_AuthStub())
            self.assertTrue(out["ok"])
            entries = out["capsule"]["continuity"].get("rationale_entries", [])
            self.assertEqual(entries, [])


# ---------------------------------------------------------------------------
# Supersession lifecycle
# ---------------------------------------------------------------------------

class TestSupersessionLifecycle(unittest.TestCase):
    """Validate supersession semantics end-to-end."""

    def test_superseded_entry_persists_in_capsule(self) -> None:
        """Superseded entries remain in the capsule on read."""
        entries = [
            _sample_entry(tag="old_approach", status="superseded"),
            _sample_entry(tag="new_approach", supersedes="old_approach"),
        ]
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=entries)
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            req = ContinuityReadRequest(subject_kind="user", subject_id="test-agent")
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(req=req, auth=_AuthStub())
            capsule_entries = out["capsule"]["continuity"]["rationale_entries"]
            self.assertEqual(len(capsule_entries), 2)
            statuses = {e["tag"]: e["status"] for e in capsule_entries}
            self.assertEqual(statuses["old_approach"], "superseded")
            self.assertEqual(statuses["new_approach"], "active")

    def test_superseded_filtered_from_startup_summary(self) -> None:
        entries = [
            _sample_entry(tag="old_approach", status="superseded"),
            _sample_entry(tag="new_approach", supersedes="old_approach"),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        re = summary["orientation"]["rationale_entries"]
        self.assertEqual(len(re), 1)
        self.assertEqual(re[0]["tag"], "new_approach")


# ---------------------------------------------------------------------------
# Retirement lifecycle
# ---------------------------------------------------------------------------

class TestRetirementLifecycle(unittest.TestCase):
    """Validate retirement semantics."""

    def test_retired_entry_persists_in_capsule(self) -> None:
        entries = [_sample_entry(tag="resolved_tension", status="retired", kind="tension")]
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload(rationale_entries=entries)
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            req = ContinuityReadRequest(subject_kind="user", subject_id="test-agent")
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(req=req, auth=_AuthStub())
            self.assertEqual(len(out["capsule"]["continuity"]["rationale_entries"]), 1)
            self.assertEqual(out["capsule"]["continuity"]["rationale_entries"][0]["status"], "retired")

    def test_retired_filtered_from_startup_summary(self) -> None:
        entries = [
            _sample_entry(tag="active_one", status="active"),
            _sample_entry(tag="retired_one", status="retired"),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        re = summary["orientation"]["rationale_entries"]
        self.assertEqual(len(re), 1)
        self.assertEqual(re[0]["tag"], "active_one")


# ---------------------------------------------------------------------------
# Finding 1: compare/revalidate detect rationale-entry changes
# ---------------------------------------------------------------------------

class TestCompareDetectsRationaleChanges(unittest.TestCase):
    """Validate compare detects rationale_entries differences."""

    def _settings(self, repo_root: Path) -> Settings:
        return _settings(repo_root)

    def _signals(self) -> list[dict]:
        now = _now_iso()
        return [
            {"kind": "system_check", "source_ref": "ref", "observed_at": now, "summary": "ok"},
        ]

    def test_compare_detects_added_rationale_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            active = _base_capsule_payload()
            _write_capsule(repo, active)
            candidate = _base_capsule_payload(rationale_entries=_sample_entries(2))
            out = continuity_compare_service(
                repo_root=repo,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user", subject_id="test-agent",
                    candidate_capsule=candidate, signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )
            self.assertFalse(out["identical"])
            self.assertIn("continuity.rationale_entries", out["changed_fields"])

    def test_compare_detects_modified_rationale_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            active = _base_capsule_payload(rationale_entries=_sample_entries(1))
            _write_capsule(repo, active)
            candidate = _base_capsule_payload(rationale_entries=[
                _sample_entry(tag="auth_choice", summary="Changed decision"),
            ])
            out = continuity_compare_service(
                repo_root=repo,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user", subject_id="test-agent",
                    candidate_capsule=candidate, signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )
            self.assertFalse(out["identical"])
            self.assertIn("continuity.rationale_entries", out["changed_fields"])

    def test_compare_identical_rationale_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            entries = _sample_entries(2)
            active = _base_capsule_payload(rationale_entries=entries)
            for entry in active["continuity"]["rationale_entries"]:
                entry["created_at"] = active["updated_at"]
                entry["updated_at"] = active["updated_at"]
            _write_capsule(repo, active)
            candidate = _base_capsule_payload(rationale_entries=entries)
            candidate["updated_at"] = active["updated_at"]
            candidate["verified_at"] = active["verified_at"]
            for entry in candidate["continuity"]["rationale_entries"]:
                entry["created_at"] = active["updated_at"]
                entry["updated_at"] = active["updated_at"]
            out = continuity_compare_service(
                repo_root=repo,
                auth=_AuthStub(),
                req=ContinuityCompareRequest(
                    subject_kind="user", subject_id="test-agent",
                    candidate_capsule=candidate, signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )
            self.assertTrue(out["identical"])


class TestRevalidateDetectsRationaleChanges(unittest.TestCase):
    """Validate revalidate detects rationale_entries differences on correct outcome."""

    def _signals(self) -> list[dict]:
        now = _now_iso()
        return [
            {"kind": "system_check", "source_ref": "ref", "observed_at": now, "summary": "ok"},
        ]

    def test_revalidate_correct_detects_rationale_changes(self) -> None:
        """When candidate has different rationale_entries, revalidate must detect the diff
        and produce outcome='correct' with updated=True (not collapse to 'confirm')."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            active = _base_capsule_payload()
            _write_capsule(repo, active)
            candidate = _base_capsule_payload(rationale_entries=_sample_entries(2))
            gm = _GitManagerStub(repo)
            out = continuity_revalidate_service(
                repo_root=repo,
                gm=gm,
                auth=_AuthStub(),
                req=ContinuityRevalidateRequest(
                    subject_kind="user", subject_id="test-agent",
                    outcome="correct",
                    candidate_capsule=candidate, signals=self._signals(),
                ),
                audit=lambda *_args: None,
            )
            self.assertTrue(out["ok"])
            # If the compare detected the rationale change, outcome stays "correct" and updated=True.
            # If it missed the change, outcome collapses to "confirm" and updated=False.
            self.assertEqual(out["outcome"], "correct")
            self.assertTrue(out["updated"])


# ---------------------------------------------------------------------------
# Finding 2: cold-stub rendering preserves rationale entries
# ---------------------------------------------------------------------------

class TestColdStubRationaleRendering(unittest.TestCase):
    """Validate cold-stub rendering includes rationale_entries section."""

    def test_section_order_includes_rationale_entries(self) -> None:
        self.assertIn("rationale_entries", CONTINUITY_COLD_STUB_SECTION_ORDER)

    def test_render_cold_rationale_entries_active_only(self) -> None:
        items = [
            {"tag": "auth", "kind": "decision", "status": "active", "summary": "Chose JWT"},
            {"tag": "old", "kind": "assumption", "status": "superseded", "summary": "Old thing"},
            {"tag": "done", "kind": "tension", "status": "retired", "summary": "Resolved"},
            {"tag": "cache", "kind": "decision", "status": "active", "summary": "Redis added"},
        ]
        lines = _render_cold_rationale_entries(items)
        self.assertEqual(len(lines), 2)
        self.assertIn("[decision] auth: Chose JWT", lines[0])
        self.assertIn("[decision] cache: Redis added", lines[1])

    def test_render_cold_rationale_entries_max_3(self) -> None:
        items = [
            {"tag": f"t{i}", "kind": "decision", "status": "active", "summary": f"S{i}"}
            for i in range(6)
        ]
        lines = _render_cold_rationale_entries(items)
        self.assertEqual(len(lines), 3)

    def test_render_cold_rationale_entries_empty(self) -> None:
        self.assertEqual(_render_cold_rationale_entries([]), [])
        self.assertEqual(_render_cold_rationale_entries(None), [])

    def test_render_cold_rationale_entries_truncates(self) -> None:
        items = [
            {"tag": "x" * 200, "kind": "decision", "status": "active", "summary": "y" * 300},
        ]
        lines = _render_cold_rationale_entries(items)
        self.assertEqual(len(lines), 1)
        # Tag truncated to 80, summary to 160
        self.assertLessEqual(len(lines[0]), 80 + 20 + 160 + 10)

    def test_cold_stub_text_includes_rationale_section(self) -> None:
        """Full cold stub builder includes rationale_entries section in output."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(microsecond=0)
        entries = [_sample_entry(tag="auth_choice", status="active")]
        payload = _base_capsule_payload(rationale_entries=entries)
        envelope = {
            "capsule": payload,
            "archived_at": _now_iso(),
        }
        stub_text = _build_cold_stub_text(
            envelope=envelope,
            source_archive_path="memory/continuity/archive/user-test-agent-20260328T120000Z.json",
            cold_storage_path="memory/continuity/cold/user-test-agent-20260328T120000Z.json.gz",
            cold_stored_at=_now_iso(),
            now=now,
        )
        self.assertIn("## rationale_entries", stub_text)
        self.assertIn("auth_choice", stub_text)


# ---------------------------------------------------------------------------
# Finding 3: rationale_entry_count semantics (total count)
# ---------------------------------------------------------------------------

class TestListSummaryRationaleEntryCountSemantics(unittest.TestCase):
    """Validate rationale_entry_count counts all entries regardless of status."""

    def test_count_includes_all_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            entries = [
                _sample_entry(tag="a", status="active"),
                _sample_entry(tag="b", status="superseded"),
                _sample_entry(tag="c", status="retired"),
            ]
            payload = _base_capsule_payload(rationale_entries=entries)
            _write_capsule(repo, payload)
            settings = _settings(repo)
            gm = _GitManagerStub(repo)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user"),
                    auth=_AuthStub(),
                )
            # Total count = 3, not just active count (1)
            self.assertEqual(out["capsules"][0]["rationale_entry_count"], 3)


# ---------------------------------------------------------------------------
# Finding 4: startup summary copy isolation
# ---------------------------------------------------------------------------

class TestStartupSummaryCopyIsolation(unittest.TestCase):
    """Verify startup summary entries are fully isolated from source capsule."""

    def test_mutating_summary_alternatives_does_not_affect_source(self) -> None:
        entries = [
            _sample_entry(tag="x", status="active", alternatives=["alt1", "alt2"]),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        # Mutate the summary copy
        summary["orientation"]["rationale_entries"][0]["alternatives_considered"].append("injected")
        # Source capsule must be unaffected
        source_alts = payload["continuity"]["rationale_entries"][0]["alternatives_considered"]
        self.assertEqual(source_alts, ["alt1", "alt2"])

    def test_mutating_summary_depends_on_does_not_affect_source(self) -> None:
        entries = [
            _sample_entry(tag="x", status="active", depends_on=["dep1"]),
        ]
        payload = _base_capsule_payload(rationale_entries=entries)
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        summary["orientation"]["rationale_entries"][0]["depends_on"].append("injected")
        source_deps = payload["continuity"]["rationale_entries"][0]["depends_on"]
        self.assertEqual(source_deps, ["dep1"])


# ---------------------------------------------------------------------------
# Finding 5: self-referential supersedes rejected
# ---------------------------------------------------------------------------

class TestSelfReferentialSupersedesRejected(unittest.TestCase):
    """Validate that an entry cannot supersede itself."""

    def test_self_supersedes_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            entries = [
                _sample_entry(tag="self_ref", status="superseded", supersedes="self_ref"),
            ]
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("must not reference its own tag", ctx.exception.detail)

    def test_non_self_supersedes_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            entries = [
                _sample_entry(tag="old_one", status="superseded"),
                _sample_entry(tag="new_one", supersedes="old_one"),
            ]
            payload = _base_capsule_payload(rationale_entries=entries)
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["continuity"]["rationale_entries"]), 2)


# ---------------------------------------------------------------------------
# Finding 7: session-end snapshot with invalid supersession is rejected
# ---------------------------------------------------------------------------

class TestSnapshotInvalidSupersessionRejected(unittest.TestCase):
    """Validate that post-merge validation rejects invalid supersession in snapshot."""

    def _do_upsert(self, repo_root: Path, payload: dict, snapshot: dict) -> dict:
        settings = _settings(repo_root)
        gm = _GitManagerStub(repo_root)
        req_data: dict = {
            "subject_kind": payload["subject_kind"],
            "subject_id": payload["subject_id"],
            "capsule": payload,
            "session_end_snapshot": snapshot,
        }
        req = ContinuityUpsertRequest(**req_data)
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_upsert(req=req, auth=_AuthStub())

    def test_snapshot_with_dangling_supersedes_rejected(self) -> None:
        """Snapshot rationale_entries with supersedes pointing to nonexistent tag must be rejected."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload()
            snapshot = {
                "open_loops": ["loop"],
                "top_priorities": ["p"],
                "active_constraints": ["c"],
                "stance_summary": "Fresh stance for testing snapshot supersession.",
                "rationale_entries": [
                    _sample_entry(tag="new_one", supersedes="nonexistent"),
                ],
            }
            with self.assertRaises(HTTPException) as ctx:
                self._do_upsert(repo, payload, snapshot)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("supersedes", ctx.exception.detail)

    def test_snapshot_with_self_supersedes_rejected(self) -> None:
        """Snapshot rationale_entries with self-referential supersedes must be rejected."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            payload = _base_capsule_payload()
            snapshot = {
                "open_loops": ["loop"],
                "top_priorities": ["p"],
                "active_constraints": ["c"],
                "stance_summary": "Fresh stance for testing snapshot self ref.",
                "rationale_entries": [
                    _sample_entry(tag="self_ref", status="superseded", supersedes="self_ref"),
                ],
            }
            with self.assertRaises(HTTPException) as ctx:
                self._do_upsert(repo, payload, snapshot)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("must not reference its own tag", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main()
