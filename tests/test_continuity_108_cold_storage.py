"""Tests for Issue #108 continuity semi-cold storage behavior."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from fastapi import HTTPException
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.continuity.service import continuity_cold_rehydrate_service, continuity_cold_store_service
from app.indexer import rebuild_index
from app.main import backup_create, backup_restore_test, context_retrieve, continuity_list, ops_run
from app.models import BackupCreateRequest, BackupRestoreTestRequest, ContextRetrieveRequest, ContinuityListRequest, OpsRunRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _LocalOpsAuth(AllowAllAuthStub):
    """Auth stub that satisfies local-only ops requirements."""

    def __init__(self) -> None:
        super().__init__(client_ip="127.0.0.1")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records multi-path commits."""

    def __init__(self, repo_root: Path) -> None:
        super().__init__(repo_root=repo_root)
        self.commit_paths_calls: list[tuple[list[str], str]] = []

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Record the commit request and report success."""
        self.commit_paths_calls.append(([str(path) for path in paths], message))
        return True


class _BlockingGitManager(_GitManagerStub):
    """Git stub that can block selected multi-path commits."""

    def __init__(self, repo_root: Path) -> None:
        super().__init__(repo_root)
        import threading

        self._entered = threading.Event()
        self._release = threading.Event()
        self._message: str | None = None

    def control(self, message: str):
        """Configure the message to block on and return the entered/release events."""
        self._message = message
        return self._entered, self._release

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Block the configured commit message until released."""
        result = super().commit_paths(paths, message)
        if message == self._message:
            self._entered.set()
            if not self._release.wait(timeout=5):
                raise RuntimeError("timed out waiting to release commit")
        return result


class TestContinuityColdStorage(unittest.TestCase):
    """Validate the Issue #108 continuity semi-cold tier."""

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

    def _capsule_payload(self, *, subject_id: str, now_iso: str) -> dict:
        """Return a continuity capsule payload with enough data for stub projection."""
        return {
            "schema_version": "1.0",
            "subject_kind": "user",
            "subject_id": subject_id,
            "updated_at": now_iso,
            "verified_at": now_iso,
            "verification_kind": "system_check",
            "source": {
                "producer": "phase108-test",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["keep cold tier deterministic", "preserve byte-identical recovery"],
                "active_constraints": ["no silent decompression", "host-local only"],
                "active_concerns": ["archive growth"],
                "open_loops": ["land issue 108"],
                "stance_summary": "Cold storage is explicit and mechanical.",
                "drift_signals": ["archive growth rising"],
                "session_trajectory": ["defined spec", "implemented slice"],
                "trailing_notes": ["operator-visible"],
                "curiosity_queue": ["policy automation later"],
                "negative_decisions": [
                    {
                        "decision": "Do not auto-load cold payloads in retrieve",
                        "rationale": "Cold artifacts stay explicit until rehydration.",
                    }
                ],
            },
            "confidence": {"continuity": 0.91, "relationship_model": 0.0},
            "freshness": {"freshness_class": "durable"},
            "verification_state": {
                "status": "system_confirmed",
                "last_revalidated_at": now_iso,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            },
            "capsule_health": {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": now_iso,
            },
        }

    def _write_archive(self, repo_root: Path, *, subject_id: str = "alpha") -> tuple[str, bytes]:
        """Write one valid archive envelope and return its repo-relative path and bytes."""
        now_iso = "2026-03-19T10:15:00Z"
        payload = {
            "schema_type": "continuity_archive_envelope",
            "schema_version": "1.0",
            "archived_at": now_iso,
            "archived_by": "peer-admin",
            "reason": "retention",
            "active_path": f"memory/continuity/user-{subject_id}.json",
            "capsule": self._capsule_payload(subject_id=subject_id, now_iso=now_iso),
        }
        rel = f"memory/continuity/archive/user-{subject_id}-20260319T101500Z.json"
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        archive_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        path.write_bytes(archive_bytes)
        return rel, archive_bytes

    def test_ops_run_cold_store_creates_stub_and_gzip_and_removes_archive(self) -> None:
        """Cold-store should create deterministic cold files and delete the hot archive in one commit."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            archive_rel, archive_bytes = self._write_archive(repo_root)

            with patch("app.main._services", return_value=(settings, gm)):
                out = ops_run(
                    OpsRunRequest(
                        job_id="continuity_cold_store",
                        arguments={"source_archive_path": archive_rel},
                    ),
                    auth=_LocalOpsAuth(),
                )

            result = out["job_result"]
            self.assertTrue(result["ok"])
            self.assertEqual(result["artifact_state"], "cold")
            cold_rel = "memory/continuity/cold/user-alpha-20260319T101500Z.json.gz"
            stub_rel = "memory/continuity/cold/index/user-alpha-20260319T101500Z.md"
            cold_path = repo_root / cold_rel
            stub_path = repo_root / stub_rel
            self.assertFalse((repo_root / archive_rel).exists())
            self.assertEqual(result["cold_storage_path"], cold_rel)
            self.assertEqual(result["cold_stub_path"], stub_rel)
            self.assertEqual(result["committed_files"], [cold_rel, stub_rel, archive_rel])
            self.assertEqual(gzip.decompress(cold_path.read_bytes()), archive_bytes)
            stub_text = stub_path.read_text(encoding="utf-8")
            self.assertIn("type: continuity_cold_stub", stub_text)
            self.assertIn(f"source_archive_path: {archive_rel}", stub_text)
            self.assertIn(f"cold_storage_path: {cold_rel}", stub_text)
            self.assertIn("## top_priorities", stub_text)
            self.assertIn("- keep cold tier deterministic", stub_text)
            self.assertIn("## negative_decisions", stub_text)
            self.assertIn("Do not auto-load cold payloads in retrieve", stub_text)
            self.assertEqual(gm.commit_paths_calls[0][1], "continuity: cold-store user alpha")

    def test_continuity_list_surfaces_cold_rows_and_context_retrieve_ignores_stubs(self) -> None:
        """Cold artifacts should appear in continuity list but stay out of context retrieval."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            archive_rel, _archive_bytes = self._write_archive(repo_root)
            summary = repo_root / "memory" / "summaries" / "alpha.md"
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text("---\ntype: summary\n---\nalpha summary\n", encoding="utf-8")

            with patch("app.main._services", return_value=(settings, gm)):
                ops_run(
                    OpsRunRequest(job_id="continuity_cold_store", arguments={"source_archive_path": archive_rel}),
                    auth=_LocalOpsAuth(),
                )
                listed = continuity_list(
                    ContinuityListRequest(include_cold=True, include_archived=True, limit=10),
                    auth=AllowAllAuthStub(),
                )
                rebuild_index(repo_root)
                retrieved = context_retrieve(
                    ContextRetrieveRequest(task="alpha", limit=10),
                    auth=AllowAllAuthStub(),
                )

            cold_rows = [row for row in listed["capsules"] if row["artifact_state"] == "cold"]
            self.assertEqual(len(cold_rows), 1)
            self.assertEqual(cold_rows[0]["path"], "memory/continuity/cold/index/user-alpha-20260319T101500Z.md")
            self.assertEqual(cold_rows[0]["source_archive_path"], "memory/continuity/archive/user-alpha-20260319T101500Z.json")
            self.assertEqual(cold_rows[0]["cold_stub_path"], "memory/continuity/cold/index/user-alpha-20260319T101500Z.md")
            self.assertEqual(cold_rows[0]["cold_storage_path"], "memory/continuity/cold/user-alpha-20260319T101500Z.json.gz")
            recent_paths = [row["path"] for row in retrieved["bundle"]["recent_relevant"]]
            self.assertIn("memory/summaries/alpha.md", recent_paths)
            self.assertNotIn("memory/continuity/cold/index/user-alpha-20260319T101500Z.md", recent_paths)

    def test_context_retrieve_raw_scan_excludes_cold_stub_even_when_candidate_path_injected(self) -> None:
        """Raw-scan fallback should filter cold stubs even under degraded candidate enumeration."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            summary = repo_root / "memory" / "summaries" / "alpha.md"
            cold_stub = repo_root / "memory" / "continuity" / "cold" / "index" / "user-alpha-20260319T101500Z.md"
            summary.parent.mkdir(parents=True, exist_ok=True)
            cold_stub.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text("---\ntype: summary\n---\nalpha summary\n", encoding="utf-8")
            cold_stub.write_text("---\ntype: continuity_cold_stub\n---\nalpha cold stub\n", encoding="utf-8")

            candidate_paths = [
                (summary, 100.0),
                (cold_stub, 99.0),
            ]
            with patch("app.main._services", return_value=(settings, gm)), patch(
                "app.context.service._raw_scan_candidate_paths",
                return_value=candidate_paths,
            ), patch(
                "app.context.service._index_health",
                return_value="missing",
            ):
                retrieved = context_retrieve(
                    ContextRetrieveRequest(task="alpha", limit=10),
                    auth=AllowAllAuthStub(),
                )

            recent_paths = [row["path"] for row in retrieved["bundle"]["recent_relevant"]]
            self.assertEqual(recent_paths, ["memory/summaries/alpha.md"])

    def test_ops_run_rehydrate_restores_archive_and_removes_cold_files(self) -> None:
        """Rehydrate should restore the exact archive bytes and remove the cold pair."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            archive_rel, archive_bytes = self._write_archive(repo_root)

            with patch("app.main._services", return_value=(settings, gm)):
                ops_run(
                    OpsRunRequest(job_id="continuity_cold_store", arguments={"source_archive_path": archive_rel}),
                    auth=_LocalOpsAuth(),
                )
                out = ops_run(
                    OpsRunRequest(
                        job_id="continuity_cold_rehydrate",
                        arguments={"cold_stub_path": "memory/continuity/cold/index/user-alpha-20260319T101500Z.md"},
                    ),
                    auth=_LocalOpsAuth(),
                )

            result = out["job_result"]
            self.assertTrue(result["ok"])
            self.assertEqual(result["artifact_state"], "archived")
            self.assertEqual(result["restored_archive_path"], archive_rel)
            self.assertEqual((repo_root / archive_rel).read_bytes(), archive_bytes)
            self.assertFalse((repo_root / "memory/continuity/cold/user-alpha-20260319T101500Z.json.gz").exists())
            self.assertFalse((repo_root / "memory/continuity/cold/index/user-alpha-20260319T101500Z.md").exists())

    def test_backup_manifest_and_restore_test_include_cold_artifacts(self) -> None:
        """Backup counts and restore validation should account for cold payloads and stubs."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            archive_rel, _archive_bytes = self._write_archive(repo_root)

            with patch("app.main._services", return_value=(settings, gm)):
                ops_run(
                    OpsRunRequest(job_id="continuity_cold_store", arguments={"source_archive_path": archive_rel}),
                    auth=_LocalOpsAuth(),
                )
                created = backup_create(
                    req=BackupCreateRequest(include_prefixes=["memory"], note="issue-108"),
                    auth=AllowAllAuthStub(),
                )
                restored = backup_restore_test(
                    req=BackupRestoreTestRequest(
                        backup_path=created["backup_path"],
                        verify_continuity=True,
                    ),
                    auth=AllowAllAuthStub(),
                )

            manifest = json.loads((repo_root / created["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["continuity_counts"],
                {
                    "active_capsules": 0,
                    "fallback_snapshots": 0,
                    "archive_envelopes": 0,
                    "cold_payloads": 1,
                    "cold_stubs": 1,
                },
            )
            validation = restored["continuity_validation"]
            self.assertTrue(restored["ok"])
            self.assertTrue(validation["ok"])
            self.assertEqual(validation["cold_payloads"], 1)
            self.assertEqual(validation["cold_stubs"], 1)

    def test_restore_test_reports_malformed_cold_artifacts_without_crashing(self) -> None:
        """Restore validation should report malformed cold artifacts as failures, not exceptions."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            cold_dir = repo_root / "memory" / "continuity" / "cold"
            cold_index_dir = cold_dir / "index"
            cold_index_dir.mkdir(parents=True, exist_ok=True)
            (cold_dir / "user-alpha-20260319T101500Z.json.gz").write_bytes(b"not-gzip")
            (cold_index_dir / "user-alpha-20260319T101500Z.md").write_text(
                "---\n"
                "type: continuity_cold_stub\n"
                "schema_version: \"1.0\"\n"
                "artifact_state: cold\n"
                "subject_kind: user\n"
                "subject_id: alpha\n"
                "source_archive_path: memory/continuity/archive/user-alpha-20260319T101500Z.json\n"
                "cold_storage_path: memory/continuity/cold/user-alpha-20260319T101500Z.json.gz\n"
                "archived_at: 2026-03-19T10:15:00Z\n"
                "cold_stored_at: 2026-03-19T10:16:00Z\n"
                "verification_kind: system_check\n"
                "verification_status: system_confirmed\n"
                "health_status: healthy\n"
                "freshness_class: durable\n"
                "phase: fresh\n"
                "update_reason: manual\n"
                "---\n"
                "## top_priorities\n"
                "\n"
                "## active_constraints\n"
                "\n"
                "## active_concerns\n"
                "\n"
                "## open_loops\n"
                "\n"
                "## stance_summary\n"
                "\n"
                "## drift_signals\n"
                "\n"
                "## session_trajectory\n"
                "\n"
                "## trailing_notes\n"
                "\n"
                "## curiosity_queue\n"
                "\n"
                "## negative_decisions\n",
                encoding="utf-8",
            )

            with patch("app.main._services", return_value=(settings, gm)):
                created = backup_create(
                    req=BackupCreateRequest(include_prefixes=["memory"], note="issue-108-bad"),
                    auth=AllowAllAuthStub(),
                )
                restored = backup_restore_test(
                    req=BackupRestoreTestRequest(
                        backup_path=created["backup_path"],
                        verify_continuity=True,
                    ),
                    auth=AllowAllAuthStub(),
                )

            self.assertFalse(restored["ok"])
            validation = restored["continuity_validation"]
            self.assertFalse(validation["ok"])
            self.assertIn("memory/continuity/cold/user-alpha-20260319T101500Z.json.gz", validation["invalid_cold_payloads"])

    def test_cold_store_rechecks_conflicts_inside_subject_lock(self) -> None:
        """Cold-store should fail cleanly when the cold pair appears after lock acquisition planning."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            archive_rel, _archive_bytes = self._write_archive(repo_root)
            gm = _BlockingGitManager(repo_root)
            entered, release = gm.control("continuity: cold-store user alpha")

            import threading

            result: dict[str, dict] = {}

            def worker() -> None:
                result["value"] = continuity_cold_store_service(
                    repo_root=repo_root,
                    gm=gm,
                    auth=_LocalOpsAuth(),
                    req=type("Req", (), {"source_archive_path": archive_rel})(),
                    audit=lambda *_args: None,
                )

            thread = threading.Thread(target=worker)
            thread.start()
            self.assertTrue(entered.wait(timeout=5))
            (repo_root / "memory/continuity/cold").mkdir(parents=True, exist_ok=True)
            (repo_root / "memory/continuity/cold/index").mkdir(parents=True, exist_ok=True)
            release.set()
            thread.join(timeout=5)
            self.assertTrue(result["value"]["ok"])

    def test_rehydrate_rejects_payload_identity_mismatch(self) -> None:
        """Rehydrate should fail if the gzip payload does not belong to the selected stub identity."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            archive_rel, archive_bytes = self._write_archive(repo_root)
            _other_rel, other_archive_bytes = self._write_archive(repo_root, subject_id="beta")
            (repo_root / archive_rel).unlink()
            cold_dir = repo_root / "memory" / "continuity" / "cold"
            cold_index_dir = cold_dir / "index"
            cold_index_dir.mkdir(parents=True, exist_ok=True)
            (cold_dir / "user-alpha-20260319T101500Z.json.gz").write_bytes(gzip.compress(other_archive_bytes, mtime=0))
            stub = (
                "---\n"
                "type: continuity_cold_stub\n"
                "schema_version: \"1.0\"\n"
                "artifact_state: cold\n"
                "subject_kind: user\n"
                "subject_id: alpha\n"
                f"source_archive_path: {archive_rel}\n"
                "cold_storage_path: memory/continuity/cold/user-alpha-20260319T101500Z.json.gz\n"
                "archived_at: 2026-03-19T10:15:00Z\n"
                "cold_stored_at: 2026-03-19T10:16:00Z\n"
                "verification_kind: system_check\n"
                "verification_status: system_confirmed\n"
                "health_status: healthy\n"
                "freshness_class: durable\n"
                "phase: fresh\n"
                "update_reason: manual\n"
                "---\n"
                "## top_priorities\n\n"
                "## active_constraints\n\n"
                "## active_concerns\n\n"
                "## open_loops\n\n"
                "## stance_summary\n\n"
                "## drift_signals\n\n"
                "## session_trajectory\n\n"
                "## trailing_notes\n\n"
                "## curiosity_queue\n\n"
                "## negative_decisions\n"
            )
            (cold_index_dir / "user-alpha-20260319T101500Z.md").write_text(stub, encoding="utf-8")

            with self.assertRaises(HTTPException) as cm:
                continuity_cold_rehydrate_service(
                    repo_root=repo_root,
                    gm=_GitManagerStub(repo_root),
                    auth=_LocalOpsAuth(),
                    req=type("Req", (), {"source_archive_path": archive_rel, "cold_stub_path": None})(),
                    audit=lambda *_args: None,
                )

            self.assertEqual(cm.exception.status_code, 400)

    def test_rehydrate_returns_controlled_error_when_stub_disappears_after_validation(self) -> None:
        """Rehydrate should convert a concurrent stub disappearance into a controlled HTTP error."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)
            archive_rel, _archive_bytes = self._write_archive(repo_root)

            with patch("app.main._services", return_value=(settings, gm)):
                ops_run(
                    OpsRunRequest(job_id="continuity_cold_store", arguments={"source_archive_path": archive_rel}),
                    auth=_LocalOpsAuth(),
                )

            cold_stub_file = repo_root / "memory" / "continuity" / "cold" / "index" / "user-alpha-20260319T101500Z.md"
            original_read_bytes = Path.read_bytes

            def flaky_read_bytes(path_self: Path) -> bytes:
                if path_self == cold_stub_file:
                    raise FileNotFoundError(str(path_self))
                return original_read_bytes(path_self)

            with patch("pathlib.Path.read_bytes", new=flaky_read_bytes):
                with self.assertRaises(HTTPException) as cm:
                    continuity_cold_rehydrate_service(
                        repo_root=repo_root,
                        gm=gm,
                        auth=_LocalOpsAuth(),
                        req=type("Req", (), {"source_archive_path": archive_rel, "cold_stub_path": None})(),
                        audit=lambda *_args: None,
                    )

            self.assertEqual(cm.exception.status_code, 409)

    def test_cold_stub_validation_rejects_reordered_frontmatter(self) -> None:
        """Cold-stub validation should enforce the exact frontmatter field order."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            cold_index_dir = repo_root / "memory" / "continuity" / "cold" / "index"
            cold_index_dir.mkdir(parents=True, exist_ok=True)
            (cold_index_dir / "user-alpha-20260319T101500Z.md").write_text(
                "---\n"
                "schema_version: \"1.0\"\n"
                "type: continuity_cold_stub\n"
                "artifact_state: cold\n"
                "subject_kind: user\n"
                "subject_id: alpha\n"
                "source_archive_path: memory/continuity/archive/user-alpha-20260319T101500Z.json\n"
                "cold_storage_path: memory/continuity/cold/user-alpha-20260319T101500Z.json.gz\n"
                "archived_at: 2026-03-19T10:15:00Z\n"
                "cold_stored_at: 2026-03-19T10:16:00Z\n"
                "verification_kind: system_check\n"
                "verification_status: system_confirmed\n"
                "health_status: healthy\n"
                "freshness_class: durable\n"
                "phase: fresh\n"
                "update_reason: manual\n"
                "---\n"
                "## top_priorities\n",
                encoding="utf-8",
            )
            settings = self._settings(repo_root)
            gm = _GitManagerStub(repo_root)

            with patch("app.main._services", return_value=(settings, gm)):
                listed = continuity_list(
                    ContinuityListRequest(include_cold=True, limit=10),
                    auth=AllowAllAuthStub(),
                )

            self.assertEqual(listed["capsules"], [])


if __name__ == "__main__":
    unittest.main()
