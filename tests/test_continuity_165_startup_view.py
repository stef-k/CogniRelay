"""Tests for #165 startup-oriented continuity read view."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.continuity.service import (
    CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED,
    _build_startup_summary,
)
from app.main import continuity_read
from app.models import ContinuityReadRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all scopes used by continuity tests."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub used to satisfy the service bundle patch."""


class TestStartupView(unittest.TestCase):
    """Validate the #165 startup view contract."""

    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(
        self,
        *,
        subject_kind: str = "user",
        subject_id: str = "stef",
        negative_decisions: list | None = None,
        session_trajectory: list | None = None,
        capsule_health: dict | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        payload: dict = {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": now,
            "verified_at": now,
            "verification_kind": "self_review",
            "source": {
                "producer": "test",
                "update_reason": "pre_compaction",
                "inputs": [],
            },
            "continuity": {
                "top_priorities": ["priority one"],
                "active_concerns": ["concern one"],
                "active_constraints": ["constraint one"],
                "open_loops": ["loop one"],
                "stance_summary": "Current stance text",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.85, "relationship_model": 0.0},
        }
        if negative_decisions is not None:
            payload["continuity"]["negative_decisions"] = negative_decisions
        if session_trajectory is not None:
            payload["continuity"]["session_trajectory"] = session_trajectory
        if capsule_health is not None:
            payload["capsule_health"] = capsule_health
        return payload

    def _write_capsule(self, repo_root: Path, payload: dict) -> None:
        kind = payload["subject_kind"]
        sid = payload["subject_id"].strip().lower().replace(" ", "-")
        d = repo_root / "memory" / "continuity"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{kind}-{sid}.json").write_text(json.dumps(payload), encoding="utf-8")

    def _write_fallback(self, repo_root: Path, capsule: dict) -> None:
        kind = capsule["subject_kind"]
        sid = capsule["subject_id"].strip().lower().replace(" ", "-")
        d = repo_root / "memory" / "continuity" / "fallback"
        d.mkdir(parents=True, exist_ok=True)
        envelope = {
            "schema_type": "continuity_fallback_snapshot",
            "schema_version": "1.0",
            "captured_at": capsule["updated_at"],
            "source_path": f"memory/continuity/{kind}-{sid}.json",
            "verification_status": capsule.get("verification_state", {}).get("status", "unverified"),
            "health_status": capsule.get("capsule_health", {}).get("status", "unknown"),
            "capsule": capsule,
        }
        (d / f"{kind}-{sid}.json").write_text(json.dumps(envelope), encoding="utf-8")

    # --- Test 1: backward compatibility ---
    def test_read_without_view_has_no_startup_summary(self) -> None:
        """Read without view parameter must not include startup_summary."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
            self.assertNotIn("startup_summary", out)

    # --- Test 2: happy path ---
    def test_startup_view_active_capsule_has_all_sections(self) -> None:
        """Startup view with active capsule must include all four top-level sections."""
        neg = [{"decision": "Decided not to X", "rationale": "Because Y"}]
        traj = ["trajectory entry"]
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(negative_decisions=neg, session_trajectory=traj)
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True, view="startup"),
                    auth=_AuthStub(),
                )
            self.assertIn("startup_summary", out)
            ss = out["startup_summary"]
            self.assertIsNotNone(ss["recovery"])
            self.assertIsNotNone(ss["orientation"])
            self.assertIsNotNone(ss["context"])
            self.assertIsNotNone(ss["updated_at"])
            # Capsule must still be present and unchanged
            self.assertIsNotNone(out["capsule"])
            self.assertEqual(out["source_state"], "active")

    # --- Test 3: key order contract ---
    def test_startup_summary_key_order(self) -> None:
        """Startup summary must have keys in exact insertion order per spec."""
        neg = [{"decision": "d", "rationale": "r"}]
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(
                negative_decisions=neg,
                session_trajectory=["t"],
                capsule_health={"status": "healthy", "reasons": [], "last_checked_at": "2026-03-24T18:00:00Z"},
            )
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(list(ss.keys()), ["recovery", "orientation", "context", "updated_at"])
            self.assertEqual(
                list(ss["recovery"].keys()),
                ["source_state", "recovery_warnings", "capsule_health_status", "capsule_health_reasons"],
            )
            self.assertEqual(
                list(ss["orientation"].keys()),
                ["top_priorities", "active_constraints", "open_loops", "negative_decisions"],
            )
            self.assertEqual(
                list(ss["context"].keys()),
                ["session_trajectory", "stance_summary", "active_concerns"],
            )

    # --- Test 4: negative_decisions pass-through ---
    def test_negative_decisions_passthrough(self) -> None:
        """negative_decisions in startup_summary must be identical to capsule's."""
        neg = [
            {"decision": "Decided not to X", "rationale": "Because Y"},
            {"decision": "Skip Z", "rationale": "Not relevant"},
        ]
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(negative_decisions=neg)
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(ss["orientation"]["negative_decisions"], neg)
            # Verify it's the same list content as the capsule's
            self.assertEqual(
                ss["orientation"]["negative_decisions"],
                out["capsule"]["continuity"]["negative_decisions"],
            )

    # --- Test 5: fallback capsule degraded surfacing ---
    def test_startup_view_fallback_surfaces_degraded(self) -> None:
        """Startup view with fallback capsule must surface fallback source_state and warnings."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()
            self._write_fallback(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True, view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(ss["recovery"]["source_state"], "fallback")
            self.assertTrue(any("fallback" in w for w in ss["recovery"]["recovery_warnings"]))

    # --- Test 6: missing capsule ---
    def test_startup_view_missing_capsule(self) -> None:
        """Startup view with missing capsule must null orientation/context/updated_at."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="gone", allow_fallback=True, view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(ss["recovery"]["source_state"], "missing")
            self.assertIsNone(ss["orientation"])
            self.assertIsNone(ss["context"])
            self.assertIsNone(ss["updated_at"])

    # --- Test 7: absent capsule_health defaults ---
    def test_absent_capsule_health_defaults(self) -> None:
        """When capsule has no capsule_health, summary must show null status and empty reasons."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()  # No capsule_health
            self.assertNotIn("capsule_health", payload)
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertIsNone(ss["recovery"]["capsule_health_status"])
            self.assertEqual(ss["recovery"]["capsule_health_reasons"], [])

    # --- Test 8: legacy capsule missing negative_decisions/session_trajectory ---
    def test_legacy_capsule_missing_optional_fields(self) -> None:
        """Legacy capsules without negative_decisions/session_trajectory must show [] in summary."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()
            # Ensure these are NOT set (legacy)
            self.assertNotIn("negative_decisions", payload["continuity"])
            self.assertNotIn("session_trajectory", payload["continuity"])
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(ss["orientation"]["negative_decisions"], [])
            self.assertEqual(ss["context"]["session_trajectory"], [])

    # --- Test 9: degraded capsule_health surfacing ---
    def test_degraded_capsule_health_surfaced(self) -> None:
        """Degraded capsule_health must appear in recovery block."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(
                capsule_health={"status": "degraded", "reasons": ["source drift"], "last_checked_at": "2026-03-24T18:00:00Z"},
            )
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            ss = out["startup_summary"]
            self.assertEqual(ss["recovery"]["capsule_health_status"], "degraded")
            self.assertEqual(ss["recovery"]["capsule_health_reasons"], ["source drift"])

    # --- Test 10: _build_startup_summary is pure and deterministic ---
    def test_build_startup_summary_pure_and_deterministic(self) -> None:
        """_build_startup_summary must produce identical output for identical input."""
        capsule = {
            "continuity": {
                "top_priorities": ["p1"],
                "active_constraints": ["c1"],
                "open_loops": ["l1"],
                "negative_decisions": [{"decision": "d", "rationale": "r"}],
                "session_trajectory": ["t1"],
                "stance_summary": "stance",
                "active_concerns": ["a1"],
            },
            "capsule_health": {"status": "healthy", "reasons": ["r1"]},
            "updated_at": "2026-03-24T18:06:26Z",
        }
        out = {
            "ok": True,
            "path": "memory/continuity/user-stef.json",
            "capsule": capsule,
            "archived": False,
            "source_state": "active",
            "recovery_warnings": ["w1"],
        }
        import copy
        out_snapshot = copy.deepcopy(out)

        result1 = _build_startup_summary(out)
        result2 = _build_startup_summary(out)
        self.assertEqual(result1, result2)
        # Verify key order
        self.assertEqual(list(result1.keys()), ["recovery", "orientation", "context", "updated_at"])
        # Verify input dict was not mutated (outer and nested)
        self.assertNotIn("startup_summary", out)
        self.assertEqual(out, out_snapshot)

    # --- Test 11: summary lists are independent copies, not aliases ---
    def test_startup_summary_lists_are_copies(self) -> None:
        """Mutating the returned summary must not affect the source capsule."""
        capsule = {
            "continuity": {
                "top_priorities": ["p1"],
                "active_constraints": ["c1"],
                "open_loops": ["l1"],
                "negative_decisions": [{"decision": "d", "rationale": "r"}],
                "session_trajectory": ["t1"],
                "stance_summary": "stance",
                "active_concerns": ["a1"],
            },
            "capsule_health": {"status": "healthy", "reasons": ["r1"]},
            "updated_at": "2026-03-24T18:06:26Z",
        }
        out = {
            "ok": True,
            "path": "memory/continuity/user-stef.json",
            "capsule": capsule,
            "archived": False,
            "source_state": "active",
            "recovery_warnings": ["w1"],
        }
        result = _build_startup_summary(out)
        # Mutate every list in the summary
        result["recovery"]["recovery_warnings"].append("injected")
        result["recovery"]["capsule_health_reasons"].append("injected")
        result["orientation"]["top_priorities"].append("injected")
        result["orientation"]["active_constraints"].append("injected")
        result["orientation"]["open_loops"].append("injected")
        result["orientation"]["negative_decisions"].append({"decision": "x", "rationale": "y"})
        # dict(d) is a one-level shallow copy — sufficient because
        # NegativeDecision has only scalar (str) fields.
        result["orientation"]["negative_decisions"][0]["decision"] = "MUTATED"
        result["context"]["session_trajectory"].append("injected")
        result["context"]["active_concerns"].append("injected")
        # Source capsule must be unchanged
        self.assertEqual(capsule["continuity"]["top_priorities"], ["p1"])
        self.assertEqual(capsule["continuity"]["negative_decisions"], [{"decision": "d", "rationale": "r"}])
        self.assertEqual(capsule["capsule_health"]["reasons"], ["r1"])
        self.assertEqual(out["recovery_warnings"], ["w1"])

    # --- Test 12: capsule is value-identical with and without view ---
    def test_capsule_value_identical_with_and_without_view(self) -> None:
        """The capsule dict must be equal whether view is set or not."""
        neg = [{"decision": "d", "rationale": "r"}]
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload(
                negative_decisions=neg,
                session_trajectory=["t"],
                capsule_health={"status": "healthy", "reasons": [], "last_checked_at": "2026-03-24T18:00:00Z"},
            )
            self._write_capsule(repo_root, payload)
            with patch("app.main._services", return_value=(settings, gm)):
                out_plain = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
                    auth=_AuthStub(),
                )
                out_startup = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            self.assertEqual(out_plain["capsule"], out_startup["capsule"])

    # --- Test 13: allow_fallback=False with view="startup" still raises on missing ---
    def test_startup_view_without_fallback_still_raises_on_missing(self) -> None:
        """view='startup' must not suppress the 404 when allow_fallback is False."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                with self.assertRaises(HTTPException) as err:
                    continuity_read(
                        req=ContinuityReadRequest(subject_kind="user", subject_id="gone", view="startup"),
                        auth=_AuthStub(),
                    )
                self.assertEqual(err.exception.status_code, 404)

    # --- Test 14: startup_summary_build_failed degradation path ---
    def test_startup_summary_build_failure_degrades_gracefully(self) -> None:
        """When _build_startup_summary raises, response must have null summary and warning."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            payload = self._capsule_payload()
            self._write_capsule(repo_root, payload)
            with (
                patch("app.main._services", return_value=(settings, gm)),
                patch("app.continuity.service._build_startup_summary", side_effect=RuntimeError("boom")),
            ):
                out = continuity_read(
                    req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
                    auth=_AuthStub(),
                )
            self.assertTrue(out["ok"])
            self.assertIsNone(out["startup_summary"])
            self.assertIn(CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED, out["recovery_warnings"])
            # Capsule must still be present and valid
            self.assertIsNotNone(out["capsule"])


if __name__ == "__main__":
    unittest.main()
