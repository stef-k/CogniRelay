"""Tests for Phase 4 backup/restore validation and retrieval resilience."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.indexer import rebuild_index
from app.main import backup_create, backup_restore_test, context_retrieve
from app.models import BackupCreateRequest, BackupRestoreTestRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that permits all Phase 4 backup and retrieval operations."""


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records single-file commit requests."""

    def __init__(self) -> None:
        """Initialize the captured commit list."""
        self.commit_file_calls: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record the commit request and report success."""
        self.commit_file_calls.append((str(path), message))
        return True


class TestContinuityPhase4Phase4(unittest.TestCase):
    """Validate backup/restore continuity reporting and index fallback behavior."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build settings rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _capsule_payload(self, *, subject_kind: str, subject_id: str, updated_at: str, verified_at: str) -> dict:
        """Return a minimal valid continuity capsule payload."""
        return {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": updated_at,
            "verified_at": verified_at,
            "verification_kind": "system_check",
            "source": {
                "producer": "phase4-test",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["keep service healthy"],
                "active_concerns": ["phase4 coverage"],
                "active_constraints": ["deterministic behavior"],
                "open_loops": ["finish rollout"],
                "stance_summary": f"stance for {subject_id}",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            "verification_state": {
                "status": "system_confirmed",
                "last_revalidated_at": verified_at,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            },
            "capsule_health": {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": verified_at,
            },
        }

    def _write_valid_active(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict) -> Path:
        """Write one valid active continuity capsule."""
        directory = repo_root / "memory" / "continuity"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{subject_kind}-{subject_id}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _write_fallback(self, repo_root: Path, *, subject_kind: str, subject_id: str, payload: dict) -> Path:
        """Write one valid fallback snapshot envelope."""
        directory = repo_root / "memory" / "continuity" / "fallback"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{subject_kind}-{subject_id}.json"
        path.write_text(
            json.dumps(
                {
                    "schema_type": "continuity_fallback_snapshot",
                    "schema_version": "1.0",
                    "captured_at": payload["updated_at"],
                    "source_path": f"memory/continuity/{subject_kind}-{subject_id}.json",
                    "verification_status": payload["verification_state"]["status"],
                    "health_status": payload["capsule_health"]["status"],
                    "capsule": payload,
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_backup_manifest_includes_continuity_counts(self) -> None:
        """Backup manifests should include continuity counts when memory/continuity is included."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            payload = self._capsule_payload(subject_kind="user", subject_id="alpha", updated_at=now, verified_at=now)
            self._write_valid_active(repo_root, subject_kind="user", subject_id="alpha", payload=payload)
            self._write_fallback(repo_root, subject_kind="user", subject_id="alpha", payload=payload)
            (repo_root / "memory" / "continuity" / "refresh_state.json").write_text(
                json.dumps({"schema_version": "1.0", "last_planned_at": now, "entries": []}),
                encoding="utf-8",
            )
            archive_dir = repo_root / "memory" / "continuity" / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            (archive_dir / "user-alpha-20260316T120000Z.json").write_text(
                json.dumps(
                    {
                        "schema_type": "continuity_archive_envelope",
                        "schema_version": "1.0",
                        "archived_at": now,
                        "active_path": "memory/continuity/user-alpha.json",
                        "archived_by": "peer-admin",
                        "reason": "cleanup",
                        "capsule": payload,
                    }
                ),
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            gm = _GitManagerStub()

            with patch("app.main._services", return_value=(settings, gm)):
                created = backup_create(req=BackupCreateRequest(include_prefixes=["memory"], note="phase4"), auth=_AuthStub())

            manifest = json.loads((repo_root / created["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["continuity_counts"],
                {
                    "active_capsules": 1,
                    "fallback_snapshots": 1,
                    "archive_envelopes": 1,
                    "cold_payloads": 0,
                    "cold_stubs": 0,
                },
            )

    def test_backup_restore_reports_continuity_validation_without_crashing(self) -> None:
        """Restore drills should report invalid continuity artifacts and missing fallbacks."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            now = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            valid = self._capsule_payload(subject_kind="user", subject_id="alpha", updated_at=now, verified_at=now)
            self._write_valid_active(repo_root, subject_kind="user", subject_id="alpha", payload=valid)

            continuity_dir = repo_root / "memory" / "continuity"
            continuity_dir.mkdir(parents=True, exist_ok=True)
            (continuity_dir / "user-bad.json").write_text("{bad json", encoding="utf-8")
            (continuity_dir / "refresh_state.json").write_text(
                json.dumps({"schema_version": "1.0", "last_planned_at": now, "entries": []}),
                encoding="utf-8",
            )

            fallback_dir = continuity_dir / "fallback"
            fallback_dir.mkdir(parents=True, exist_ok=True)
            (fallback_dir / "user-bad.json").write_text(json.dumps({"schema_type": "wrong"}), encoding="utf-8")

            archive_dir = continuity_dir / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            (archive_dir / "user-bad-20260316T120000Z.json").write_text(json.dumps({"schema_type": "wrong"}), encoding="utf-8")

            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            with patch("app.main._services", return_value=(settings, gm)):
                created = backup_create(req=BackupCreateRequest(include_prefixes=["memory"], note="phase4"), auth=_AuthStub())
                restored = backup_restore_test(
                    req=BackupRestoreTestRequest(
                        backup_path=created["backup_path"],
                        verify_index_rebuild=True,
                        verify_continuity=True,
                    ),
                    auth=_AuthStub(),
                )

            self.assertFalse(restored["ok"])
            validation = restored["continuity_validation"]
            self.assertFalse(validation["ok"])
            self.assertEqual(validation["active_capsules"], 2)
            self.assertEqual(validation["fallback_capsules"], 1)
            self.assertEqual(validation["archive_envelopes"], 1)
            self.assertIn("memory/continuity/user-bad.json", validation["invalid_capsules"])
            self.assertIn("memory/continuity/fallback/user-bad.json", validation["invalid_fallbacks"])
            self.assertIn("memory/continuity/archive/user-bad-20260316T120000Z.json", validation["invalid_archives"])
            self.assertIn("memory/continuity/user-alpha.json", validation["missing_fallbacks"])
            self.assertNotIn("memory/continuity/refresh_state.json", validation["invalid_capsules"])

    def test_context_retrieve_uses_indexed_path_when_index_is_stale(self) -> None:
        """Stale indexes should keep indexed retrieval and add a stale warning."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "summaries").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")
            (repo_root / "memory" / "summaries" / "phase4.md").write_text("---\ntype: summary\n---\nphase4 summary\n", encoding="utf-8")
            rebuild_index(repo_root)
            files_index = repo_root / "index" / "files_index.json"
            payload = json.loads(files_index.read_text(encoding="utf-8"))
            payload["generated_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
            files_index.write_text(json.dumps(payload), encoding="utf-8")

            with patch("app.main._services", return_value=(settings, gm)), patch("app.context.service._raw_scan_recent_relevant") as raw_scan:
                out = context_retrieve(
                    ContextRetrieveRequest(task="phase4", limit=5),
                    auth=_AuthStub(),
                )

            self.assertFalse(raw_scan.called)
            self.assertEqual(out["bundle"]["continuity_state"]["recovery_warnings"], ["continuity_index_stale"])
            self.assertEqual(out["bundle"]["recent_relevant"][0]["path"], "memory/summaries/phase4.md")

    def test_context_retrieve_uses_raw_scan_when_index_is_missing(self) -> None:
        """Missing indexes should trigger bounded raw-scan fallback with deterministic ordering."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "summaries").mkdir(parents=True, exist_ok=True)
            (repo_root / "messages" / "threads").mkdir(parents=True, exist_ok=True)
            (repo_root / "journal" / "2026").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")
            (repo_root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
            summary = repo_root / "memory" / "summaries" / "phase4.md"
            message = repo_root / "messages" / "threads" / "thread-1.md"
            journal = repo_root / "journal" / "2026" / "2026-03-16.md"
            continuity_capsule = repo_root / "memory" / "continuity" / "user-infra.json"
            summary.write_text("---\ntype: summary\n---\nphase4 summary body\n", encoding="utf-8")
            message.write_text("---\ntype: message_thread\n---\nphase4 message body\n", encoding="utf-8")
            journal.write_text("---\ntype: journal_entry\n---\nphase4 journal body\n", encoding="utf-8")
            continuity_capsule.write_text('{"schema_version":"1.0","subject_kind":"user","subject_id":"infra"}', encoding="utf-8")
            now = datetime.now(timezone.utc)
            for idx, path in enumerate([summary, message, journal, continuity_capsule]):
                ts = now - timedelta(minutes=idx)
                content = path.read_text(encoding="utf-8")
                path.write_text(content, encoding="utf-8")
                path_stat = ts.timestamp()
                os.utime(path, (path_stat, path_stat))

            with patch("app.main._services", return_value=(settings, gm)):
                out = context_retrieve(
                    ContextRetrieveRequest(task="phase4", limit=5),
                    auth=_AuthStub(),
                )

            self.assertEqual(out["bundle"]["continuity_state"]["recovery_warnings"], ["continuity_index_missing"])
            self.assertEqual(
                [row["path"] for row in out["bundle"]["recent_relevant"]],
                [
                    "memory/summaries/phase4.md",
                    "messages/threads/thread-1.md",
                    "journal/2026/2026-03-16.md",
                ],
            )

    def test_context_retrieve_raw_scan_skips_files_deleted_before_read(self) -> None:
        """Raw-scan fallback should skip files that disappear after candidate enumeration."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            (repo_root / "memory" / "summaries").mkdir(parents=True, exist_ok=True)
            summary = repo_root / "memory" / "summaries" / "phase4.md"
            summary.write_text("---\ntype: summary\n---\nphase4 summary body\n", encoding="utf-8")
            candidate = (summary, summary.stat().st_mtime)
            original_read_bytes = Path.read_bytes

            def flaky_read_bytes(path_obj: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
                if path_obj == summary:
                    raise FileNotFoundError("vanished before read")
                return original_read_bytes(path_obj, *args, **kwargs)

            with patch("app.main._services", return_value=(settings, gm)), patch(
                "app.context.service._raw_scan_candidate_paths",
                return_value=[candidate],
            ), patch("pathlib.Path.read_bytes", new=flaky_read_bytes):
                out = context_retrieve(ContextRetrieveRequest(task="phase4", limit=5), auth=_AuthStub())

            self.assertEqual(out["bundle"]["recent_relevant"], [])
            self.assertEqual(out["bundle"]["continuity_state"]["recovery_warnings"], ["continuity_index_missing"])

    def test_context_retrieve_falls_back_to_raw_scan_when_files_index_is_corrupt(self) -> None:
        """Corrupt files_index.json should degrade stale retrieval to raw-scan fallback."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            (repo_root / "memory" / "core").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "summaries").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory" / "core" / "identity.md").write_text("# identity\n", encoding="utf-8")
            (repo_root / "memory" / "summaries" / "phase4.md").write_text("---\ntype: summary\n---\nphase4 summary\n", encoding="utf-8")
            rebuild_index(repo_root)
            (repo_root / "index" / "files_index.json").write_text("{bad json", encoding="utf-8")
            fallback_rows = [
                {
                    "path": "memory/summaries/phase4.md",
                    "type": "summary",
                    "snippet": "phase4 summary",
                    "importance": 1.0,
                    "modified_at": datetime.now(timezone.utc).isoformat(),
                    "score": 1,
                }
            ]

            with patch("app.main._services", return_value=(settings, gm)), patch(
                "app.context.service._raw_scan_recent_relevant",
                return_value=fallback_rows,
            ) as raw_scan:
                out = context_retrieve(ContextRetrieveRequest(task="phase4", limit=5), auth=_AuthStub())

            self.assertTrue(raw_scan.called)
            self.assertEqual(out["bundle"]["recent_relevant"], fallback_rows)
            self.assertEqual(out["bundle"]["continuity_state"]["recovery_warnings"], ["continuity_index_stale"])


if __name__ == "__main__":
    unittest.main()
