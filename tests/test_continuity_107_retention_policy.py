"""Tests for Issue #107 executable continuity retention policy behavior."""

from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from app.config import Settings
from app.continuity.service import continuity_retention_plan_service
from app.main import continuity_list, continuity_retention_plan, discovery_tools, manifest, ops_catalog, ops_run
from app.models import ContinuityListRequest, ContinuityRetentionPlanRequest, OpsRunRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


class _LocalOpsAuth(AllowAllAuthStub):
    """Auth stub that satisfies local-only ops requirements."""

    def __init__(self) -> None:
        super().__init__(client_ip="127.0.0.1")


class _SelectiveAuth(AllowAllAuthStub):
    """Auth stub that can deny reads for exact archive paths."""

    def __init__(self, *, denied_read_paths: set[str] | None = None, client_ip: str | None = None) -> None:
        super().__init__(client_ip=client_ip)
        self.denied_read_paths = denied_read_paths or set()

    def require_read_path(self, path: str) -> None:
        if path in self.denied_read_paths:
            from fastapi import HTTPException

            raise HTTPException(status_code=403, detail="forbidden")


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records retention plan and cold-store commits."""

    def __init__(self, repo_root: Path) -> None:
        super().__init__(repo_root=repo_root)
        self.commit_file_calls: list[tuple[str, str]] = []
        self.commit_paths_calls: list[tuple[list[str], str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        self.commit_file_calls.append((str(path), message))
        return True

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        self.commit_paths_calls.append(([str(path) for path in paths], message))
        return True


class _NoCommitGitManagerStub(_GitManagerStub):
    """Git stub that reports no new commit for retention-state persistence."""

    def commit_file(self, path: Path, message: str) -> bool:
        self.commit_file_calls.append((str(path), message))
        return False


class TestContinuityRetentionPolicy(unittest.TestCase):
    """Validate the Issue #107 retention planning and apply surfaces."""

    def _settings(self, repo_root: Path, *, archive_days: int = 90) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
            continuity_retention_archive_days=archive_days,
        )

    def _capsule_payload(self, *, subject_kind: str, subject_id: str, now_iso: str) -> dict:
        return {
            "schema_version": "1.0",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": now_iso,
            "verified_at": now_iso,
            "verification_kind": "system_check",
            "source": {
                "producer": "phase107-test",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": [f"priority for {subject_id}"],
                "active_constraints": ["preserve continuity"],
                "active_concerns": ["archive backlog"],
                "open_loops": ["land issue 107"],
                "stance_summary": "Retention stays explicit and preservation-first.",
                "drift_signals": [],
            },
            "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            "freshness": {"freshness_class": "durable"},
            "verification_state": {
                "status": "system_confirmed",
                "last_revalidated_at": now_iso,
                "strongest_signal": "system_check",
                "evidence_refs": [],
            },
            "capsule_health": {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": now_iso,
            },
        }

    def _write_archive(
        self,
        repo_root: Path,
        *,
        subject_kind: str = "user",
        subject_id: str,
        archived_at: datetime,
    ) -> str:
        archived_iso = archived_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        payload = {
            "schema_type": "continuity_archive_envelope",
            "schema_version": "1.0",
            "archived_at": archived_iso,
            "archived_by": "peer-admin",
            "reason": "retention",
            "active_path": f"memory/continuity/{subject_kind}-{subject_id}.json",
            "capsule": self._capsule_payload(subject_kind=subject_kind, subject_id=subject_id, now_iso=archived_iso),
        }
        rel = f"memory/continuity/archive/{subject_kind}-{subject_id}-{archived_at.strftime('%Y%m%dT%H%M%SZ')}.json"
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return rel

    def _write_invalid_archive(self, repo_root: Path, *, name: str) -> str:
        rel = f"memory/continuity/archive/{name}.json"
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8")
        return rel

    def _write_invalid_utf8_archive(self, repo_root: Path, *, name: str) -> str:
        rel = f"memory/continuity/archive/{name}.json"
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\xff\xfe\x00bad-utf8")
        return rel

    def _write_cold_pair(self, repo_root: Path, *, source_archive_path: str) -> tuple[str, str]:
        stem = Path(source_archive_path).stem
        cold_rel = f"memory/continuity/cold/{Path(source_archive_path).name}.gz"
        stub_rel = f"memory/continuity/cold/index/{stem}.md"
        cold_path = repo_root / cold_rel
        stub_path = repo_root / stub_rel
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        stub_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"gzip-placeholder")
        stub_path.write_text(
            "\n".join(
                [
                    "---",
                    "type: continuity_cold_stub",
                    'schema_version: "1.0"',
                    "artifact_state: cold",
                    "subject_kind: user",
                    f"subject_id: {stem.split('-', 1)[1].rsplit('-', 1)[0]}",
                    f"source_archive_path: {source_archive_path}",
                    f"cold_storage_path: {cold_rel}",
                    "archived_at: 2026-01-01T00:00:00Z",
                    "cold_stored_at: 2026-03-19T10:16:00Z",
                    "verification_kind: system_check",
                    "verification_status: system_confirmed",
                    "health_status: healthy",
                    "freshness_class: durable",
                    "phase: stable",
                    "update_reason: manual",
                    "---",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return cold_rel, stub_rel

    def test_list_and_retention_plan_use_configured_archive_days(self) -> None:
        """Configured stale threshold should drive both list classification and planning."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root, archive_days=30)
            gm = _GitManagerStub(repo_root)
            now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
            recent_rel = self._write_archive(repo_root, subject_id="recent", archived_at=now - timedelta(days=10))
            stale_rel = self._write_archive(repo_root, subject_id="stale", archived_at=now - timedelta(days=40))

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                listed = continuity_list(
                    ContinuityListRequest(include_archived=True, limit=10),
                    auth=AllowAllAuthStub(),
                )
                planned = continuity_retention_plan(
                    ContinuityRetentionPlanRequest(limit=25),
                    auth=AllowAllAuthStub(),
                )

            by_path = {row["path"]: row for row in listed["capsules"]}
            self.assertEqual(by_path["memory/continuity/user-recent.json"]["retention_class"], "archive_recent")
            self.assertEqual(by_path["memory/continuity/user-stale.json"]["retention_class"], "archive_stale")
            self.assertEqual(planned["count"], 1)
            self.assertEqual(planned["candidates"][0]["source_archive_path"], stale_rel)
            self.assertNotEqual(planned["candidates"][0]["source_archive_path"], recent_rel)

    def test_retention_plan_persists_exact_window_and_ordered_warnings(self) -> None:
        """Planning should persist the bounded candidate window with deterministic warnings."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root, archive_days=30)
            gm = _GitManagerStub(repo_root)
            now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)

            oldest_rel = self._write_archive(repo_root, subject_id="oldest", archived_at=now - timedelta(days=80))
            newer_rel = self._write_archive(repo_root, subject_id="newer", archived_at=now - timedelta(days=50))
            partial_rel = self._write_archive(repo_root, subject_id="partial", archived_at=now - timedelta(days=70))
            unauthorized_rel = self._write_archive(repo_root, subject_id="unauthorized", archived_at=now - timedelta(days=60))
            invalid_rel = self._write_invalid_archive(repo_root, name="user-invalid-20260101T000000Z")
            already_cold_rel = self._write_archive(repo_root, subject_id="already-cold", archived_at=now - timedelta(days=90))

            cold_payload_rel = f"memory/continuity/cold/{Path(partial_rel).name}.gz"
            (repo_root / cold_payload_rel).parent.mkdir(parents=True, exist_ok=True)
            (repo_root / cold_payload_rel).write_bytes(b"partial-only")
            self._write_cold_pair(repo_root, source_archive_path=already_cold_rel)

            auth = _SelectiveAuth(denied_read_paths={unauthorized_rel})
            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                out = continuity_retention_plan(
                    ContinuityRetentionPlanRequest(limit=1),
                    auth=auth,
                )

            self.assertTrue(out["ok"])
            self.assertEqual(out["count"], 1)
            self.assertEqual(out["total_candidates"], 2)
            self.assertTrue(out["has_more"])
            self.assertEqual(out["path"], "memory/continuity/retention_state.json")
            self.assertEqual(out["latest_commit"], "test-sha")
            self.assertEqual(out["candidates"][0]["source_archive_path"], oldest_rel)
            self.assertEqual(out["candidates"][0]["policy_action"], "cold_store")
            self.assertEqual(out["candidates"][0]["reason_codes"], ["archive_stale"])
            self.assertEqual(
                out["warnings"],
                [
                    f"continuity_retention_partial_cold_conflict:{partial_rel}",
                    f"continuity_retention_skipped_invalid_archive:{invalid_rel}",
                    f"continuity_retention_skipped_unauthorized:{unauthorized_rel}",
                ],
            )
            state_path = repo_root / "memory" / "continuity" / "retention_state.json"
            self.assertTrue(state_path.exists())
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted,
                {
                    "schema_type": "continuity_retention_plan",
                    "schema_version": "1.0",
                    "generated_at": "2026-03-19T12:00:00Z",
                    "path": "memory/continuity/retention_state.json",
                    "filters": {"subject_kind": None, "limit": 1},
                    "count": 1,
                    "total_candidates": 2,
                    "has_more": True,
                    "warnings": [
                        f"continuity_retention_partial_cold_conflict:{partial_rel}",
                        f"continuity_retention_skipped_invalid_archive:{invalid_rel}",
                        f"continuity_retention_skipped_unauthorized:{unauthorized_rel}",
                    ],
                    "candidates": [out["candidates"][0]],
                },
            )
            self.assertEqual(
                gm.commit_file_calls,
                [(str(state_path), "continuity: retention plan")],
            )
            self.assertEqual(newer_rel, "memory/continuity/archive/user-newer-20260128T120000Z.json")

    def test_ops_run_retention_apply_reports_per_path_statuses(self) -> None:
        """Batch apply should dedupe, continue after failures, and report terminal per-path results."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root, archive_days=30)
            gm = _GitManagerStub(repo_root)
            now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)

            cold_store_rel = self._write_archive(repo_root, subject_id="alpha", archived_at=now - timedelta(days=70))
            recent_rel = self._write_archive(repo_root, subject_id="beta", archived_at=now - timedelta(days=5))
            already_cold_rel = self._write_archive(repo_root, subject_id="gamma", archived_at=now - timedelta(days=70))
            conflict_rel = self._write_archive(repo_root, subject_id="delta", archived_at=now - timedelta(days=70))
            invalid_rel = self._write_invalid_archive(repo_root, name="user-invalid-20260102T000000Z")
            unauthorized_rel = self._write_archive(repo_root, subject_id="epsilon", archived_at=now - timedelta(days=70))

            self._write_cold_pair(repo_root, source_archive_path=already_cold_rel)
            conflict_payload_rel = f"memory/continuity/cold/{Path(conflict_rel).name}.gz"
            (repo_root / conflict_payload_rel).parent.mkdir(parents=True, exist_ok=True)
            (repo_root / conflict_payload_rel).write_bytes(b"partial-only")

            missing_rel = "memory/continuity/archive/user-missing-20260103T000000Z.json"
            invalid_path_rel = "memory/continuity/not-an-archive.md"

            req_paths = [
                cold_store_rel,
                cold_store_rel,
                recent_rel,
                missing_rel,
                already_cold_rel,
                conflict_rel,
                invalid_rel,
                unauthorized_rel,
                invalid_path_rel,
            ]

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                out = ops_run(
                    OpsRunRequest(
                        job_id="continuity_retention_apply",
                        arguments={"source_archive_paths": req_paths},
                    ),
                    auth=_SelectiveAuth(denied_read_paths={unauthorized_rel}, client_ip="127.0.0.1"),
                )

            result = out["job_result"]
            self.assertTrue(result["ok"])
            self.assertEqual(result["requested"], len(req_paths))
            self.assertEqual(result["unique_requested"], len(req_paths) - 1)
            self.assertEqual(result["processed"], len(req_paths) - 1)
            self.assertEqual(result["cold_stored"], 1)
            self.assertEqual(result["failed"], 4)
            self.assertEqual(result["warnings"], [f"duplicate_source_archive_path:{cold_store_rel}"])
            self.assertEqual([row["source_archive_path"] for row in result["results"]], req_paths[0:1] + req_paths[2:])
            self.assertEqual(
                [row["status"] for row in result["results"]],
                [
                    "cold_stored",
                    "skipped_not_stale",
                    "skipped_missing",
                    "skipped_already_cold",
                    "failed_conflict",
                    "failed_invalid_archive",
                    "failed_authorization",
                    "failed_invalid_archive",
                ],
            )
            self.assertFalse((repo_root / cold_store_rel).exists())
            self.assertTrue((repo_root / "memory/continuity/cold/user-alpha-20260108T120000Z.json.gz").exists())
            self.assertTrue((repo_root / "memory/continuity/cold/index/user-alpha-20260108T120000Z.md").exists())

    def test_invalid_utf8_archives_degrade_in_plan_and_apply(self) -> None:
        """Invalid UTF-8 archive envelopes should be treated as malformed, not internal failures."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root, archive_days=30)
            gm = _GitManagerStub(repo_root)
            now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
            utf8_bad_rel = self._write_invalid_utf8_archive(repo_root, name="user-bad-utf8-20260101T000000Z")

            with patch("app.main._services", return_value=(settings, gm)), patch("app.main.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = now
                mocked_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                planned = continuity_retention_plan(
                    ContinuityRetentionPlanRequest(limit=25),
                    auth=AllowAllAuthStub(),
                )
                applied = ops_run(
                    OpsRunRequest(
                        job_id="continuity_retention_apply",
                        arguments={"source_archive_paths": [utf8_bad_rel]},
                    ),
                    auth=_LocalOpsAuth(),
                )

            self.assertEqual(
                planned["warnings"],
                [f"continuity_retention_skipped_invalid_archive:{utf8_bad_rel}"],
            )
            self.assertEqual(applied["job_result"]["results"][0]["status"], "failed_invalid_archive")

    def test_retention_plan_rollback_preserves_newer_bytes_observed_inside_lock(self) -> None:
        """Rollback should restore the bytes observed under lock, not stale pre-lock bytes."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            now = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
            self._write_archive(repo_root, subject_id="alpha", archived_at=now - timedelta(days=70))
            retention_path = repo_root / "memory" / "continuity" / "retention_state.json"
            retention_path.parent.mkdir(parents=True, exist_ok=True)
            retention_path.write_text('{"sentinel":"old"}', encoding="utf-8")
            newer_bytes = b'{"sentinel":"newer"}'

            @contextmanager
            def _mutating_lock(_repo_root: Path):
                retention_path.write_bytes(newer_bytes)
                yield

            with (
                patch("app.continuity.service.repository_mutation_lock", _mutating_lock),
                self.assertRaises(HTTPException) as ctx,
            ):
                continuity_retention_plan_service(
                    repo_root=repo_root,
                    gm=_NoCommitGitManagerStub(repo_root),
                    auth=AllowAllAuthStub(),
                    req=ContinuityRetentionPlanRequest(limit=25),
                    now=now,
                    retention_archive_days=30,
                    audit=lambda *_args: None,
                )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertEqual(retention_path.read_bytes(), newer_bytes)

    def test_discovery_manifest_and_ops_catalog_expose_retention_surfaces(self) -> None:
        """Public discovery surfaces should advertise the new planning and apply entrypoints."""
        tools = discovery_tools()
        by_name = {tool["name"]: tool for tool in tools["tools"]}
        self.assertIn("continuity.retention_plan", by_name)
        self.assertEqual(by_name["continuity.retention_plan"]["path"], "/v1/continuity/retention/plan")
        self.assertFalse(by_name["continuity.retention_plan"]["idempotent"])

        m = manifest()
        self.assertIn("POST /v1/continuity/retention/plan", m["endpoints"])

        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub(Path(td)))):
                catalog = ops_catalog(auth=_LocalOpsAuth())

        self.assertTrue(any(job["job_id"] == "continuity_retention_apply" for job in catalog["jobs"]))


if __name__ == "__main__":
    unittest.main()
