"""Tests for Issue #33 Phase 3 lifecycle preservation and closeout behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.main import backup_create, backup_restore_test, continuity_archive, continuity_list, continuity_upsert, discovery_tools
from app.models import BackupCreateRequest, BackupRestoreTestRequest, ContinuityArchiveRequest, ContinuityListRequest, ContinuityUpsertRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git manager stub that records continuity commits for lifecycle tests."""

    def __init__(self) -> None:
        """Initialize the fake commit ledger."""
        self.commit_file_calls: list[tuple[str, str]] = []
        self.commit_paths_calls: list[tuple[list[str], str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record a single-file commit request."""
        self.commit_file_calls.append((str(path), message))
        return True

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record a multi-path commit request."""
        self.commit_paths_calls.append(([str(path) for path in paths], message))
        return True


class TestContinuity33Phase3(unittest.TestCase):
    """Validate Issue #33 lifecycle preservation and closeout guarantees."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build repository-rooted settings for lifecycle tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self, *, subject_kind: str = "user", subject_id: str = "stef") -> dict:
        """Return a valid continuity capsule payload including the Issue #33 fields."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": now,
            "verified_at": now,
            "verification_kind": "system_check",
            "source": {
                "producer": "phase33-test",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": [f"priority for {subject_id}"],
                "active_concerns": [f"concern for {subject_id}"],
                "active_constraints": [f"constraint for {subject_id}"],
                "open_loops": [f"loop for {subject_id}"],
                "stance_summary": f"stance for {subject_id}",
                "drift_signals": [],
                "trailing_notes": [f"trailing note for {subject_id}"],
                "curiosity_queue": [f"curiosity for {subject_id}"],
                "negative_decisions": [
                    {
                        "decision": f"Do not broaden {subject_id} scope.",
                        "rationale": f"Keep {subject_id} inside the bounded continuity slice.",
                    }
                ],
            },
            "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            "verification_state": {
                "status": "system_confirmed",
                "last_revalidated_at": now,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            },
            "capsule_health": {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": now,
            },
        }

    def test_fallback_snapshot_preserves_issue_33_fields(self) -> None:
        """Upsert-generated fallback snapshots should preserve the new fields in the nested capsule."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload()

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule),
                    auth=AllowAllAuthStub(),
                )

            self.assertTrue(out["ok"])
            snapshot = json.loads(
                (repo_root / "memory" / "continuity" / "fallback" / "user-stef.json").read_text(encoding="utf-8")
            )
            continuity = snapshot["capsule"]["continuity"]
            self.assertEqual(continuity["trailing_notes"], ["trailing note for stef"])
            self.assertEqual(continuity["curiosity_queue"], ["curiosity for stef"])
            self.assertEqual(
                continuity["negative_decisions"][0]["decision"],
                "Do not broaden stef scope.",
            )

    def test_archive_envelope_preserves_issue_33_fields(self) -> None:
        """Archive envelopes should preserve the new fields in the nested capsule."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload()
            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-stef.json").write_text(json.dumps(capsule), encoding="utf-8")

            with patch("app.main._services", return_value=(settings, gm)):
                out = continuity_archive(
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="phase33 archival check"),
                    auth=AllowAllAuthStub(),
                )

            self.assertTrue(out["ok"])
            archive = json.loads((repo_root / out["archived_path"]).read_text(encoding="utf-8"))
            continuity = archive["capsule"]["continuity"]
            self.assertEqual(continuity["trailing_notes"], ["trailing note for stef"])
            self.assertEqual(continuity["curiosity_queue"], ["curiosity for stef"])
            self.assertEqual(
                continuity["negative_decisions"][0]["rationale"],
                "Keep stef inside the bounded continuity slice.",
            )

    def test_backup_restore_validation_accepts_issue_33_continuity_artifacts(self) -> None:
        """Restore validation should accept active, fallback, and archive artifacts containing the new fields."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload(subject_id="alpha")

            with patch("app.main._services", return_value=(settings, gm)):
                continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="alpha", capsule=capsule),
                    auth=AllowAllAuthStub(),
                )
                continuity_archive(
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="alpha", reason="phase33 archive"),
                    auth=AllowAllAuthStub(),
                )
                created = backup_create(req=BackupCreateRequest(include_prefixes=["memory"], note="phase33"), auth=AllowAllAuthStub())
                restored = backup_restore_test(
                    req=BackupRestoreTestRequest(
                        backup_path=created["backup_path"],
                        verify_index_rebuild=False,
                        verify_continuity=True,
                    ),
                    auth=AllowAllAuthStub(),
                )

            self.assertTrue(restored["ok"])
            validation = restored["continuity_validation"]
            self.assertTrue(validation["ok"])
            self.assertEqual(validation["invalid_capsules"], [])
            self.assertEqual(validation["invalid_fallbacks"], [])
            self.assertEqual(validation["invalid_archives"], [])

    def test_continuity_list_summaries_do_not_expand_with_issue_33_fields(self) -> None:
        """List summaries should stay narrow and omit the new continuity-body fields."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            capsule = self._capsule_payload()

            with patch("app.main._services", return_value=(settings, gm)):
                continuity_upsert(
                    req=ContinuityUpsertRequest(subject_kind="user", subject_id="stef", capsule=capsule),
                    auth=AllowAllAuthStub(),
                )
                continuity_archive(
                    req=ContinuityArchiveRequest(subject_kind="user", subject_id="stef", reason="phase33 list check"),
                    auth=AllowAllAuthStub(),
                )
                out = continuity_list(
                    req=ContinuityListRequest(limit=10, include_fallback=True, include_archived=True),
                    auth=AllowAllAuthStub(),
                )

            self.assertGreaterEqual(len(out["capsules"]), 2)
            for summary in out["capsules"]:
                self.assertNotIn("trailing_notes", summary)
                self.assertNotIn("curiosity_queue", summary)
                self.assertNotIn("negative_decisions", summary)

    def test_discovery_schemas_expose_issue_33_fields_without_new_tools(self) -> None:
        """Discovery schemas should expose the new continuity fields through existing tool models."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        self.assertIn("continuity.upsert", by_name)
        self.assertIn("continuity.compare", by_name)
        self.assertIn("continuity.revalidate", by_name)

        schema = by_name["continuity.upsert"]["input_schema"]
        continuity_ref = schema["$defs"]["ContinuityCapsule"]["properties"]["continuity"]["$ref"]
        continuity_key = continuity_ref.split("/")[-1]
        continuity_props = schema["$defs"][continuity_key]["properties"]
        self.assertIn("trailing_notes", continuity_props)
        self.assertIn("curiosity_queue", continuity_props)
        self.assertIn("negative_decisions", continuity_props)

        negative_decisions = continuity_props["negative_decisions"]
        item_ref = negative_decisions["items"]["$ref"]
        item_props = schema["$defs"][item_ref.split("/")[-1]]["properties"]
        self.assertEqual(sorted(item_props), ["decision", "rationale"])
