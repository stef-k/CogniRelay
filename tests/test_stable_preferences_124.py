"""Tests for #124: stable user preferences on continuity capsules."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.continuity.service import (
    _build_startup_summary,
    _estimated_tokens,
    _render_value,
    _trim_capsule,
    _validate_capsule,
)
from app.main import continuity_list, continuity_read, continuity_upsert
from app.models import (
    ContinuityCapsule,
    ContinuityListRequest,
    ContinuityReadRequest,
    ContinuityUpsertRequest,
    StablePreference,
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
    """Auth stub permitting all scopes for stable-preferences tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub recording committed files."""

    def __init__(self, repo_root: Path | None = None) -> None:
        super().__init__(repo_root)
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


def _base_capsule_payload(
    *,
    subject_kind: str = "user",
    subject_id: str = "test-agent",
    stable_preferences: list[dict] | None = None,
) -> dict:
    """Return a valid baseline capsule dict."""
    now = _now_iso()
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
        "continuity": {
            "top_priorities": ["priority one"],
            "active_concerns": ["concern one"],
            "active_constraints": ["constraint one"],
            "open_loops": ["loop one"],
            "stance_summary": "Current stance text for testing purposes.",
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.85, "relationship_model": 0.0},
    }
    if stable_preferences is not None:
        payload["stable_preferences"] = stable_preferences
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


def _sample_prefs(n: int = 2) -> list[dict]:
    """Return n sample preference dicts with unique tags."""
    now = _now_iso()
    return [
        {"tag": f"pref_{i}", "content": f"Preference content {i}", "set_at": now}
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Unit tests: StablePreference model
# ---------------------------------------------------------------------------

class TestStablePreferenceModel(unittest.TestCase):
    """Validate StablePreference Pydantic model constraints."""

    def test_valid_construction(self) -> None:
        p = StablePreference(tag="timezone", content="UTC+2 (Athens)", set_at=_now_iso())
        self.assertEqual(p.tag, "timezone")
        self.assertEqual(p.content, "UTC+2 (Athens)")

    def test_tag_too_long(self) -> None:
        with self.assertRaises(ValidationError):
            StablePreference(tag="x" * 81, content="ok", set_at=_now_iso())

    def test_content_too_long(self) -> None:
        with self.assertRaises(ValidationError):
            StablePreference(tag="ok", content="x" * 241, set_at=_now_iso())

    def test_empty_tag_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            StablePreference(tag="", content="ok", set_at=_now_iso())

    def test_empty_content_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            StablePreference(tag="ok", content="", set_at=_now_iso())

    def test_max_length_tag_accepted(self) -> None:
        p = StablePreference(tag="t" * 80, content="ok", set_at=_now_iso())
        self.assertEqual(len(p.tag), 80)

    def test_max_length_content_accepted(self) -> None:
        p = StablePreference(tag="ok", content="c" * 240, set_at=_now_iso())
        self.assertEqual(len(p.content), 240)


# ---------------------------------------------------------------------------
# Unit tests: ContinuityCapsule stable_preferences field
# ---------------------------------------------------------------------------

class TestCapsuleFieldDefaults(unittest.TestCase):
    """Validate stable_preferences field on ContinuityCapsule."""

    def test_default_empty_list(self) -> None:
        payload = _base_capsule_payload()
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(capsule.stable_preferences, [])

    def test_max_12_accepted(self) -> None:
        payload = _base_capsule_payload(stable_preferences=_sample_prefs(12))
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(len(capsule.stable_preferences), 12)

    def test_13_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            ContinuityCapsule(**_base_capsule_payload(stable_preferences=_sample_prefs(13)))

    def test_model_dump_includes_empty_list(self) -> None:
        """model_dump with exclude_none=True must still include empty stable_preferences."""
        payload = _base_capsule_payload()
        capsule = ContinuityCapsule(**payload)
        dumped = capsule.model_dump(mode="json", exclude_none=True)
        self.assertIn("stable_preferences", dumped)
        self.assertEqual(dumped["stable_preferences"], [])

    def test_backward_compat_missing_key(self) -> None:
        """A capsule dict without stable_preferences key loads with empty list default."""
        payload = _base_capsule_payload()
        payload.pop("stable_preferences", None)
        capsule = ContinuityCapsule(**payload)
        self.assertEqual(capsule.stable_preferences, [])


# ---------------------------------------------------------------------------
# Unit tests: _validate_capsule
# ---------------------------------------------------------------------------

class TestValidateCapsuleStablePreferences(unittest.TestCase):
    """Validate _validate_capsule checks for stable_preferences."""

    def test_user_capsule_with_prefs_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(subject_kind="user", stable_preferences=_sample_prefs(2))
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["stable_preferences"]), 2)

    def test_peer_capsule_with_prefs_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(subject_kind="peer", stable_preferences=_sample_prefs(1))
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(len(result["stable_preferences"]), 1)

    def test_thread_capsule_with_prefs_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(subject_kind="thread", stable_preferences=_sample_prefs(1))
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("stable_preferences", str(ctx.exception.detail))

    def test_task_capsule_with_prefs_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(subject_kind="task", stable_preferences=_sample_prefs(1))
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_thread_capsule_empty_prefs_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(subject_kind="thread", stable_preferences=[])
            capsule = ContinuityCapsule(**payload)
            result, _ = _validate_capsule(Path(td), capsule)
            self.assertEqual(result["stable_preferences"], [])

    def test_duplicate_tags_rejected(self) -> None:
        now = _now_iso()
        prefs = [
            {"tag": "dup", "content": "first", "set_at": now},
            {"tag": "dup", "content": "second", "set_at": now},
        ]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(stable_preferences=prefs)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("Duplicate", str(ctx.exception.detail))

    def test_invalid_set_at_rejected(self) -> None:
        prefs = [{"tag": "tz", "content": "UTC+2", "set_at": "not-a-timestamp"}]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(stable_preferences=prefs)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_non_utc_set_at_rejected(self) -> None:
        """set_at with timezone offset other than Z must be rejected."""
        prefs = [{"tag": "tz", "content": "UTC+2", "set_at": "2026-03-20T10:00:00+02:00"}]
        with tempfile.TemporaryDirectory() as td:
            payload = _base_capsule_payload(stable_preferences=prefs)
            capsule = ContinuityCapsule(**payload)
            with self.assertRaises(HTTPException) as ctx:
                _validate_capsule(Path(td), capsule)
            self.assertEqual(ctx.exception.status_code, 400)


# ---------------------------------------------------------------------------
# Unit tests: _trim_capsule
# ---------------------------------------------------------------------------

class TestTrimCapsuleStablePreferences(unittest.TestCase):
    """Validate stable_preferences trim behaviour."""

    def _capsule_with_prefs(self) -> dict:
        payload = _base_capsule_payload(stable_preferences=_sample_prefs(5))
        capsule = ContinuityCapsule(**payload)
        return capsule.model_dump(mode="json", exclude_none=True)

    def test_generous_budget_keeps_prefs(self) -> None:
        capsule = self._capsule_with_prefs()
        trimmed, dropped = _trim_capsule(capsule, 4000)
        self.assertIsNotNone(trimmed)
        self.assertIn("stable_preferences", trimmed)
        self.assertEqual(len(trimmed["stable_preferences"]), 5)
        self.assertNotIn("stable_preferences", dropped)

    def test_tight_budget_drops_prefs_as_unit(self) -> None:
        """When budget is tight, stable_preferences is dropped entirely (all-or-nothing)."""
        capsule = self._capsule_with_prefs()
        # Find a budget that forces trimming into phase 1 far enough to reach stable_preferences.
        # Start with enough for just core orientation fields.
        budget = _estimated_tokens(_render_value(capsule)) // 3
        trimmed, dropped = _trim_capsule(capsule, budget)
        if trimmed is not None and "stable_preferences" not in trimmed:
            self.assertIn("stable_preferences", dropped)
        elif trimmed is not None and "stable_preferences" in trimmed:
            # Budget was generous enough to keep prefs -- shrink further.
            trimmed2, dropped2 = _trim_capsule(capsule, budget // 2)
            if trimmed2 is not None:
                self.assertNotIn("stable_preferences", trimmed2)
                self.assertIn("stable_preferences", dropped2)

    def test_drop_order_after_working_hypotheses(self) -> None:
        """stable_preferences must be trimmed after continuity.working_hypotheses."""
        capsule = self._capsule_with_prefs()
        capsule["continuity"]["working_hypotheses"] = ["hypo " * 20] * 5
        # Use a budget that forces partial trimming.
        full_tokens = _estimated_tokens(_render_value(capsule))
        # Walk budget down to find where working_hypotheses and stable_preferences drop.
        wh_dropped_at = None
        sp_dropped_at = None
        for budget in range(full_tokens, 0, -10):
            _, dropped = _trim_capsule(capsule, budget)
            if "continuity.working_hypotheses" in dropped and wh_dropped_at is None:
                wh_dropped_at = budget
            if "stable_preferences" in dropped and sp_dropped_at is None:
                sp_dropped_at = budget
            if wh_dropped_at is not None and sp_dropped_at is not None:
                break
        # stable_preferences should be dropped at same budget or lower (i.e. after) working_hypotheses.
        if wh_dropped_at is not None and sp_dropped_at is not None:
            self.assertGreaterEqual(wh_dropped_at, sp_dropped_at)

    def test_trimmed_fields_includes_stable_preferences(self) -> None:
        capsule = self._capsule_with_prefs()
        # Force everything to be trimmed.
        _, dropped = _trim_capsule(capsule, 10)
        self.assertIn("stable_preferences", dropped)


# ---------------------------------------------------------------------------
# Unit tests: _build_startup_summary
# ---------------------------------------------------------------------------

class TestStartupSummaryStablePreferences(unittest.TestCase):
    """Validate stable_preferences in startup summary."""

    def test_present_when_capsule_has_prefs(self) -> None:
        payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        self.assertIn("stable_preferences", summary)
        self.assertEqual(len(summary["stable_preferences"]), 2)
        self.assertIsInstance(summary["stable_preferences"][0], dict)
        self.assertIn("tag", summary["stable_preferences"][0])

    def test_empty_list_when_no_prefs(self) -> None:
        payload = _base_capsule_payload()
        out = {
            "capsule": payload,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {},
        }
        summary = _build_startup_summary(out)
        self.assertEqual(summary["stable_preferences"], [])

    def test_null_when_capsule_missing(self) -> None:
        out = {
            "capsule": None,
            "source_state": "missing",
            "recovery_warnings": [],
            "trust_signals": None,
        }
        summary = _build_startup_summary(out)
        self.assertIsNone(summary["stable_preferences"])


# ---------------------------------------------------------------------------
# Integration tests: upsert / read roundtrip
# ---------------------------------------------------------------------------

class TestUpsertReadRoundtrip(unittest.TestCase):
    """Validate stable_preferences survive upsert → read cycle."""

    def _do_upsert(self, settings: Settings, gm: _GitManagerStub, auth: _AuthStub,
                   payload: dict) -> dict:
        req = ContinuityUpsertRequest(
            subject_kind=payload["subject_kind"],
            subject_id=payload["subject_id"],
            capsule=ContinuityCapsule(**payload),
        )
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_upsert(req=req, auth=auth)

    def _do_read(self, settings: Settings, gm: _GitManagerStub, auth: _AuthStub,
                 subject_kind: str, subject_id: str, view: str | None = None) -> dict:
        req = ContinuityReadRequest(subject_kind=subject_kind, subject_id=subject_id, view=view)
        with patch("app.main._services", return_value=(settings, gm)):
            return continuity_read(req=req, auth=auth)

    def test_roundtrip_preserves_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(3))
            self._do_upsert(settings, gm, auth, payload)
            out = self._do_read(settings, gm, auth, "user", "test-agent")
            self.assertTrue(out["ok"])
            capsule = out["capsule"]
            self.assertEqual(len(capsule["stable_preferences"]), 3)
            tags = {p["tag"] for p in capsule["stable_preferences"]}
            self.assertEqual(tags, {"pref_0", "pref_1", "pref_2"})

    def test_modify_prefs_returns_latest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
            self._do_upsert(settings, gm, auth, payload)
            # Advance updated_at by 1 second to avoid stale-write detection.
            from datetime import timedelta
            later = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            payload["updated_at"] = later
            payload["verified_at"] = later
            payload["stable_preferences"] = [
                {"tag": "new_tag", "content": "new content", "set_at": later},
            ]
            self._do_upsert(settings, gm, auth, payload)
            out = self._do_read(settings, gm, auth, "user", "test-agent")
            self.assertEqual(len(out["capsule"]["stable_preferences"]), 1)
            self.assertEqual(out["capsule"]["stable_preferences"][0]["tag"], "new_tag")

    def test_clear_to_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
            self._do_upsert(settings, gm, auth, payload)
            # Advance updated_at by 1 second to avoid stale-write detection.
            from datetime import timedelta
            later = (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            payload["updated_at"] = later
            payload["verified_at"] = later
            payload["stable_preferences"] = []
            self._do_upsert(settings, gm, auth, payload)
            out = self._do_read(settings, gm, auth, "user", "test-agent")
            self.assertEqual(out["capsule"]["stable_preferences"], [])

    def test_peer_capsule_with_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(subject_kind="peer", stable_preferences=_sample_prefs(1))
            out = self._do_upsert(settings, gm, auth, payload)
            self.assertTrue(out["ok"])

    def test_duplicate_tags_rejected_400(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            now = _now_iso()
            prefs = [
                {"tag": "dup", "content": "a", "set_at": now},
                {"tag": "dup", "content": "b", "set_at": now},
            ]
            payload = _base_capsule_payload(stable_preferences=prefs)
            with self.assertRaises(HTTPException) as ctx:
                self._do_upsert(settings, gm, auth, payload)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_invalid_set_at_rejected_400(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            prefs = [{"tag": "tz", "content": "UTC+2", "set_at": "invalid"}]
            payload = _base_capsule_payload(stable_preferences=prefs)
            with self.assertRaises(HTTPException) as ctx:
                self._do_upsert(settings, gm, auth, payload)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_thread_with_prefs_rejected_400(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(subject_kind="thread", stable_preferences=_sample_prefs(1))
            with self.assertRaises(HTTPException) as ctx:
                self._do_upsert(settings, gm, auth, payload)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_thread_empty_prefs_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            payload = _base_capsule_payload(subject_kind="thread", stable_preferences=[])
            out = self._do_upsert(settings, gm, auth, payload)
            self.assertTrue(out["ok"])


# ---------------------------------------------------------------------------
# Integration tests: startup view
# ---------------------------------------------------------------------------

class TestStartupViewIntegration(unittest.TestCase):
    """Validate stable_preferences in startup view via endpoint."""

    def test_startup_view_includes_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
            _write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="test-agent", view="startup"),
                    auth=_AuthStub(),
                )
            self.assertIn("startup_summary", out)
            summary = out["startup_summary"]
            self.assertEqual(len(summary["stable_preferences"]), 2)

    def test_startup_view_empty_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload()
            _write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="test-agent", view="startup"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["startup_summary"]["stable_preferences"], [])

    def test_startup_view_missing_capsule_null(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(
                        subject_kind="user", subject_id="nonexistent",
                        allow_fallback=True, view="startup",
                    ),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["source_state"], "missing")
            self.assertIsNone(out["startup_summary"]["stable_preferences"])


# ---------------------------------------------------------------------------
# Integration tests: list summaries
# ---------------------------------------------------------------------------

class TestListSummaryStablePreferenceCount(unittest.TestCase):
    """Validate stable_preference_count on list summary entries."""

    def test_active_with_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(3))
            _write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["count"], 1)
            self.assertEqual(out["capsules"][0]["stable_preference_count"], 3)

    def test_active_without_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload()
            _write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["capsules"][0]["stable_preference_count"], 0)

    def test_fallback_includes_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
            _write_fallback(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user", include_fallback=True),
                    auth=_AuthStub(),
                )
            fallback_entries = [e for e in out["capsules"] if e["artifact_state"] == "fallback"]
            self.assertTrue(len(fallback_entries) > 0)
            self.assertEqual(fallback_entries[0]["stable_preference_count"], 2)


# ---------------------------------------------------------------------------
# Integration tests: session-end snapshot preservation
# ---------------------------------------------------------------------------

class TestSessionEndPreservesPrefs(unittest.TestCase):
    """Verify session-end snapshot does not modify stable_preferences."""

    def test_session_end_preserves_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            auth = _AuthStub()
            prefs = _sample_prefs(3)
            payload = _base_capsule_payload(stable_preferences=prefs)
            snapshot = {
                "open_loops": ["new loop"],
                "top_priorities": ["new priority"],
                "active_constraints": ["new constraint"],
                "stance_summary": "Updated stance from session end snapshot test.",
            }
            req = ContinuityUpsertRequest(
                subject_kind="user",
                subject_id="test-agent",
                capsule=ContinuityCapsule(**payload),
                session_end_snapshot=snapshot,
            )
            with patch("app.main._services", return_value=(settings, gm)):
                upsert_out = continuity_upsert(req=req, auth=auth)
            self.assertTrue(upsert_out["ok"])
            # Read back and verify prefs unchanged.
            with patch("app.main._services", return_value=(settings, gm)):
                read_out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="test-agent"),
                    auth=auth,
                )
            capsule = read_out["capsule"]
            self.assertEqual(len(capsule["stable_preferences"]), 3)
            tags = {p["tag"] for p in capsule["stable_preferences"]}
            self.assertEqual(tags, {"pref_0", "pref_1", "pref_2"})
            # Orientation fields should have snapshot values.
            self.assertEqual(capsule["continuity"]["open_loops"], ["new loop"])


# ---------------------------------------------------------------------------
# Integration tests: fallback and archive preservation
# ---------------------------------------------------------------------------

class TestFallbackPreservesPrefs(unittest.TestCase):
    """Verify fallback snapshot preserves stable_preferences."""

    def test_fallback_read_preserves_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(2))
            _write_fallback(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="test-agent", allow_fallback=True),
                    auth=_AuthStub(),
                )
            self.assertEqual(out["source_state"], "fallback")
            self.assertEqual(len(out["capsule"]["stable_preferences"]), 2)


class TestArchivePreservesPrefs(unittest.TestCase):
    """Verify archive envelope preserves stable_preferences."""

    def test_archive_preserves_prefs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            payload = _base_capsule_payload(stable_preferences=_sample_prefs(4))
            archive_dir = repo_root / "memory" / "continuity" / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            envelope = {
                "schema_type": "continuity_archive_envelope",
                "schema_version": "1.0",
                "active_path": "memory/continuity/user-test-agent.json",
                "archived_at": _now_iso(),
                "capsule": payload,
            }
            (archive_dir / "user-test-agent-20260328.json").write_text(
                json.dumps(envelope), encoding="utf-8"
            )
            settings = _settings(repo_root)
            gm = _GitManagerStub(repo_root)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_list(
                    req=ContinuityListRequest(subject_kind="user", include_archived=True),
                    auth=_AuthStub(),
                )
            archived = [e for e in out["capsules"] if e["artifact_state"] == "archived"]
            self.assertTrue(len(archived) > 0)
            self.assertEqual(archived[0]["stable_preference_count"], 4)


if __name__ == "__main__":
    unittest.main()
