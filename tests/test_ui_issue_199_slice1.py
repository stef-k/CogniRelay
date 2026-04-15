"""Tests for the first bounded operator UI slice of issue #199."""

from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
import gzip
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from app.continuity.cold import _build_cold_stub_text
from app.continuity.paths import continuity_cold_storage_rel_path, continuity_cold_stub_rel_path


def _reload_main_module():
    """Reload config and main so env-controlled UI mounting is recalculated."""
    import app.config as config_module
    import app.main as main_module
    import app.ui.router as ui_router_module

    importlib.reload(config_module)
    importlib.reload(ui_router_module)
    return importlib.reload(main_module)


def _request_with_transport_host(host: str | None, *, headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    """Build a Request carrying an explicit transport client host."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/ui/",
        "raw_path": b"/ui/",
        "query_string": b"",
        "headers": headers or [],
        "client": None if host is None else (host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
        "http_version": "1.1",
    }
    return Request(scope)


def _capsule_payload(*, subject_kind: str, subject_id: str) -> dict:
    """Build a deterministic continuity capsule payload for UI tests."""
    now = datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "handoff-hook",
            "update_reason": "pre_compaction",
            "inputs": ["memory/core/identity.md"],
        },
        "continuity": {
            "top_priorities": [f"priority for {subject_id}"],
            "active_concerns": [f"concern for {subject_id}"],
            "active_constraints": [f"constraint for {subject_id}"],
            "open_loops": [f"loop for {subject_id}"],
            "stance_summary": f"stance for {subject_id}",
            "session_trajectory": [f"trajectory for {subject_id}"],
            "negative_decisions": [
                {
                    "decision": "skip unsafe mutation",
                    "rationale": "read-only slice",
                    "created_at": now,
                    "updated_at": now,
                }
            ],
            "rationale_entries": [
                {
                    "tag": "ui-slice",
                    "kind": "decision",
                    "status": "active",
                    "summary": "ship a thin server-rendered UI",
                    "reasoning": "keep the first slice reviewable",
                    "created_at": now,
                    "updated_at": now,
                }
            ],
            "drift_signals": [],
        },
        "stable_preferences": [
            {
                "tag": "operator-style",
                "content": "prefer readable tables",
                "created_at": now,
                "updated_at": now,
            }
        ],
        "confidence": {"continuity": 0.82, "relationship_model": 0.0},
        "freshness": {"freshness_class": "situational"},
    }


def _write_capsule(repo_root: Path, *, subject_kind: str, subject_id: str) -> None:
    """Write one active continuity capsule to the repository fixture."""
    continuity_dir = repo_root / "memory" / "continuity"
    continuity_dir.mkdir(parents=True, exist_ok=True)
    normalized = subject_id.strip().lower().replace(" ", "-")
    payload = _capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
    (continuity_dir / f"{subject_kind}-{normalized}.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_fallback(repo_root: Path, *, subject_kind: str, subject_id: str) -> None:
    """Write a fallback-only continuity snapshot for degraded UI reads."""
    fallback_dir = repo_root / "memory" / "continuity" / "fallback"
    fallback_dir.mkdir(parents=True, exist_ok=True)
    normalized = subject_id.strip().lower().replace(" ", "-")
    capsule = _capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
    (fallback_dir / f"{subject_kind}-{normalized}.json").write_text(
        json.dumps(
            {
                "schema_type": "continuity_fallback_snapshot",
                "schema_version": "1.0",
                "captured_at": capsule["updated_at"],
                "source_path": f"memory/continuity/{subject_kind}-{normalized}.json",
                "verification_status": "system_confirmed",
                "health_status": "healthy",
                "capsule": capsule,
            }
        ),
        encoding="utf-8",
    )


def _write_archive(repo_root: Path, *, subject_kind: str, subject_id: str) -> str:
    """Write an archived continuity envelope and return its repo-relative path."""
    capsule = _capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
    archived_at = capsule["updated_at"]
    archive_rel = f"memory/continuity/archive/{subject_kind}-{subject_id}-20260415T093000Z.json"
    archive_path = repo_root / archive_rel
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(
        json.dumps(
            {
                "schema_type": "continuity_archive_envelope",
                "schema_version": "1.0",
                "archived_at": archived_at,
                "active_path": f"memory/continuity/{subject_kind}-{subject_id}.json",
                "capsule": capsule,
            }
        ),
        encoding="utf-8",
    )
    return archive_rel


def _write_cold(repo_root: Path, *, subject_kind: str, subject_id: str) -> str:
    """Write a cold continuity stub and payload pair for UI lifecycle tests."""
    capsule = _capsule_payload(subject_kind=subject_kind, subject_id=subject_id)
    archived_at = capsule["updated_at"]
    source_archive_path = f"memory/continuity/archive/{subject_kind}-{subject_id}-20260415T093000Z.json"
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    envelope = {
        "schema_type": "continuity_archive_envelope",
        "schema_version": "1.0",
        "archived_at": archived_at,
        "active_path": f"memory/continuity/{subject_kind}-{subject_id}.json",
        "capsule": capsule,
    }
    payload_path = repo_root / cold_storage_path
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_bytes(gzip.compress(json.dumps(envelope).encode("utf-8")))
    stub_path = repo_root / cold_stub_path
    stub_path.parent.mkdir(parents=True, exist_ok=True)
    stub_path.write_text(
        _build_cold_stub_text(
            envelope=envelope,
            source_archive_path=source_archive_path,
            cold_storage_path=cold_storage_path,
            cold_stored_at=archived_at,
            now=datetime.now(timezone.utc),
        ),
        encoding="utf-8",
    )
    return cold_stub_path


class TestOperatorUiSlice1(unittest.TestCase):
    """Validate the first bounded operator UI slice."""

    def _client(self, repo_root: Path, **env_overrides: str) -> TestClient:
        """Load a TestClient with UI flags recalculated from env."""
        env = {
            "COGNIRELAY_REPO_ROOT": str(repo_root),
            "COGNIRELAY_AUTO_INIT_GIT": "true",
            "COGNIRELAY_AUDIT_LOG_ENABLED": "false",
            **env_overrides,
        }
        patcher = patch.dict(os.environ, env, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        main_module = _reload_main_module()
        return TestClient(main_module.app)

    def test_ui_disabled_does_not_expose_ui_routes(self) -> None:
        """When UI is disabled, the /ui surface should not be mounted."""
        with tempfile.TemporaryDirectory() as td:
            client = self._client(Path(td), COGNIRELAY_UI_ENABLED="false")
            response = client.get("/ui/")

        self.assertEqual(response.status_code, 404)

    def test_ui_localhost_restriction_blocks_non_local_requests(self) -> None:
        """The local-only UI posture should reject non-loopback transport peers."""
        import app.ui.router as ui_router

        request = _request_with_transport_host("10.20.30.40")
        with self.assertRaises(HTTPException) as err:
            ui_router._enforce_ui_access(request, SimpleNamespace(ui_require_localhost=True))

        self.assertEqual(err.exception.status_code, 403)
        self.assertIn("local-only", str(err.exception.detail))

    def test_ui_localhost_restriction_ignores_forwarded_localhost_spoof(self) -> None:
        """Forwarded localhost headers must not bypass a non-local transport source."""
        import app.ui.router as ui_router

        request = _request_with_transport_host(
            "10.20.30.40",
            headers=[
                (b"x-forwarded-for", b"127.0.0.1"),
                (b"x-real-ip", b"127.0.0.1"),
            ],
        )
        with self.assertRaises(HTTPException) as err:
            ui_router._enforce_ui_access(request, SimpleNamespace(ui_require_localhost=True))

        self.assertEqual(err.exception.status_code, 403)

    def test_ui_localhost_restriction_allows_loopback_transport(self) -> None:
        """Loopback transport peers should satisfy the strict local-only gate."""
        import app.ui.router as ui_router

        request = _request_with_transport_host("127.0.0.1")
        self.assertEqual(ui_router._enforce_ui_access(request, SimpleNamespace(ui_require_localhost=True)), "127.0.0.1")

    def test_ui_overview_and_list_pages_render_key_data(self) -> None:
        """Overview and list pages should render health and continuity summaries."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="stef")
            _write_capsule(repo_root, subject_kind="thread", subject_id="guestbook-1")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            overview = client.get("/ui/")
            listing = client.get("/ui/continuity?subject_kind=user")

        self.assertEqual(overview.status_code, 200)
        self.assertIn("Operator Overview", overview.text)
        self.assertIn("Continuity Counts", overview.text)
        self.assertIn("Active capsules", overview.text)
        self.assertEqual(listing.status_code, 200)
        self.assertIn("Continuity Capsules", listing.text)
        self.assertIn("stef", listing.text)
        self.assertIn("/ui/continuity/user/stef", listing.text)

    def test_ui_overview_does_not_initialize_git_when_auto_init_enabled(self) -> None:
        """UI GETs must not create .git even when service-wide auto-init is enabled."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            git_dir = repo_root / ".git"
            self.assertFalse(git_dir.exists())
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
                COGNIRELAY_AUTO_INIT_GIT="true",
            )

            overview = client.get("/ui/")

        self.assertEqual(overview.status_code, 200)
        self.assertFalse(git_dir.exists())
        self.assertIn("Git initialized", overview.text)
        self.assertIn("false", overview.text)
        self.assertIn("Latest commit", overview.text)
        self.assertIn("none", overview.text)

    def test_ui_detail_page_handles_fallback_and_missing_capsules(self) -> None:
        """Detail pages should degrade gracefully for fallback-only and missing continuity."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_fallback(repo_root, subject_kind="user", subject_id="fallback-only")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            fallback = client.get("/ui/continuity/user/fallback-only")
            missing = client.get("/ui/continuity/user/missing-user")

        self.assertEqual(fallback.status_code, 200)
        self.assertIn("Source state: fallback", fallback.text)
        self.assertIn("skip unsafe mutation", fallback.text)
        self.assertIn("prefer readable tables", fallback.text)
        self.assertEqual(missing.status_code, 200)
        self.assertIn("Source state: missing", missing.text)
        self.assertIn("No stable preferences recorded.", missing.text)

    def test_ui_continuity_list_surfaces_lifecycle_states_and_filters_them(self) -> None:
        """The continuity list should show active/fallback/archived/cold states with a simple lifecycle filter."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="active-user")
            _write_fallback(repo_root, subject_kind="user", subject_id="fallback-user")
            _write_archive(repo_root, subject_kind="user", subject_id="archived-user")
            _write_cold(repo_root, subject_kind="user", subject_id="cold-user")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            listing = client.get("/ui/continuity")
            archived_only = client.get("/ui/continuity?artifact_state=archived")

        self.assertEqual(listing.status_code, 200)
        self.assertIn("active", listing.text)
        self.assertIn("fallback", listing.text)
        self.assertIn("archived", listing.text)
        self.assertIn("cold", listing.text)
        self.assertIn("memory/continuity/archive/user-archived-user-20260415T093000Z.json", listing.text)
        self.assertIn("memory/continuity/cold/index/user-cold-user-20260415T093000Z.md", listing.text)
        self.assertEqual(archived_only.status_code, 200)
        self.assertIn("archived-user", archived_only.text)
        self.assertNotIn("active-user", archived_only.text)
        self.assertNotIn("fallback-user", archived_only.text)
        self.assertNotIn("cold-user", archived_only.text)

    def test_ui_detail_page_shows_related_lifecycle_artifacts(self) -> None:
        """The detail page should show related fallback/archive/cold lifecycle visibility for one subject."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="stef")
            _write_fallback(repo_root, subject_kind="user", subject_id="stef")
            _write_archive(repo_root, subject_kind="user", subject_id="stef")
            _write_cold(repo_root, subject_kind="user", subject_id="stef")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            detail = client.get("/ui/continuity/user/stef")

        self.assertEqual(detail.status_code, 200)
        self.assertIn("Related Lifecycle Artifacts", detail.text)
        self.assertIn("Fallback snapshot present", detail.text)
        self.assertIn("Archived artifacts present", detail.text)
        self.assertIn("Cold artifacts present", detail.text)
        self.assertIn("Browse archived", detail.text)
        self.assertIn("Browse cold", detail.text)
        self.assertIn("memory/continuity/archive/user-stef-20260415T093000Z.json", detail.text)
        self.assertIn("memory/continuity/cold/index/user-stef-20260415T093000Z.md", detail.text)

    def test_ui_detail_page_keeps_degraded_reads_while_showing_archived_and_cold_visibility(self) -> None:
        """Missing active/fallback continuity should still render related archived/cold lifecycle visibility."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_archive(repo_root, subject_kind="user", subject_id="history-only")
            _write_cold(repo_root, subject_kind="user", subject_id="history-only")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            detail = client.get("/ui/continuity/user/history-only")

        self.assertEqual(detail.status_code, 200)
        self.assertIn("Source state: missing", detail.text)
        self.assertIn("Archived artifacts present", detail.text)
        self.assertIn("Cold artifacts present", detail.text)
        self.assertIn(">true<", detail.text)
        self.assertIn("memory/continuity/archive/user-history-only-20260415T093000Z.json", detail.text)


if __name__ == "__main__":
    unittest.main()
