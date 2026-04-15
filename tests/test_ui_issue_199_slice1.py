"""Tests for the first bounded operator UI slice of issue #199."""

from __future__ import annotations

import importlib
import asyncio
import json
import os
import re
import tempfile
import unittest
import gzip
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
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


def _request_with_transport_host(
    host: str | None,
    *,
    path: str = "/ui/",
    query_string: bytes = b"",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    """Build a Request carrying an explicit transport client host."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query_string,
        "headers": headers or [],
        "client": None if host is None else (host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
        "http_version": "1.1",
    }
    return Request(scope)


async def _read_first_sse_event_chunk(response: Any) -> dict[str, str]:
    """Read the first SSE event frame from a StreamingResponse iterator."""
    event: dict[str, str] = {}
    async for raw_chunk in response.body_iterator:
        chunk = raw_chunk.decode("utf-8") if isinstance(raw_chunk, bytes) else raw_chunk
        for line in chunk.splitlines():
            if not line:
                if "data" in event:
                    await response.body_iterator.aclose()
                    return event
                continue
            if ": " not in line:
                continue
            key, value = line.split(": ", 1)
            if key == "retry":
                continue
            event[key] = value
    await response.body_iterator.aclose()
    raise AssertionError("No SSE event payload received")


def _ui_live_script() -> str:
    """Return the current UI live-update script text."""
    return (Path(__file__).resolve().parents[1] / "app" / "ui" / "static" / "ui_live.js").read_text(encoding="utf-8")


def _ui_live_backoff_policy() -> dict[str, int]:
    """Parse the reconnect policy constants declared in ui_live.js."""
    script = _ui_live_script()
    policy: dict[str, int] = {}
    for name in ("LIVE_BASE_DELAY_MS", "LIVE_MAX_DELAY_MS", "LIVE_OFFLINE_THRESHOLD"):
        match = re.search(rf"var {name} = (\d+);", script)
        if not match:
            raise AssertionError(f"Could not find {name} in ui_live.js")
        policy[name] = int(match.group(1))
    return policy


def _simulated_backoff_delay(attempt: int) -> int:
    """Simulate the reconnect delay policy declared in ui_live.js."""
    policy = _ui_live_backoff_policy()
    exponent = max(attempt - 1, 0)
    return min(policy["LIVE_MAX_DELAY_MS"], policy["LIVE_BASE_DELAY_MS"] * (2 ** exponent))


def _simulated_reconnect_state(attempt: int) -> str:
    """Simulate the operator-visible reconnect state declared in ui_live.js."""
    threshold = _ui_live_backoff_policy()["LIVE_OFFLINE_THRESHOLD"]
    return "offline" if attempt >= threshold else "reconnecting"


def _capsule_payload(*, subject_kind: str, subject_id: str, capsule_health_status: str | None = None) -> dict:
    """Build a deterministic continuity capsule payload for UI tests."""
    now = datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    payload = {
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
    if capsule_health_status is not None:
        payload["capsule_health"] = {
            "status": capsule_health_status,
            "last_checked_at": now,
            "reasons": [] if capsule_health_status == "healthy" else ["test health override"],
        }
    return payload


def _write_capsule(
    repo_root: Path,
    *,
    subject_kind: str,
    subject_id: str,
    capsule_health_status: str | None = None,
) -> None:
    """Write one active continuity capsule to the repository fixture."""
    continuity_dir = repo_root / "memory" / "continuity"
    continuity_dir.mkdir(parents=True, exist_ok=True)
    normalized = subject_id.strip().lower().replace(" ", "-")
    payload = _capsule_payload(
        subject_kind=subject_kind,
        subject_id=subject_id,
        capsule_health_status=capsule_health_status,
    )
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


def _write_archive(
    repo_root: Path,
    *,
    subject_kind: str,
    subject_id: str,
    capsule_health_status: str | None = None,
) -> str:
    """Write an archived continuity envelope and return its repo-relative path."""
    capsule = _capsule_payload(
        subject_kind=subject_kind,
        subject_id=subject_id,
        capsule_health_status=capsule_health_status,
    )
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

    def test_ui_continuity_filter_finds_archived_rows_beyond_mixed_display_limit(self) -> None:
        """Lifecycle filtering must stay correct even when the mixed view exceeds the display cap."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            for idx in range(205):
                _write_capsule(repo_root, subject_kind="user", subject_id=f"active-{idx:03d}")
            _write_archive(repo_root, subject_kind="user", subject_id="late-archive")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            listing = client.get("/ui/continuity")
            archived_only = client.get("/ui/continuity?artifact_state=archived")

        self.assertEqual(listing.status_code, 200)
        self.assertIn("archived 1", listing.text)
        self.assertIn("Display truncated: true", listing.text)
        self.assertEqual(archived_only.status_code, 200)
        self.assertIn("late-archive", archived_only.text)
        self.assertIn("Showing 1 result(s) from 1 matched row(s).", archived_only.text)

    def test_ui_continuity_query_matches_bounded_fields_case_insensitively(self) -> None:
        """Search should use deterministic token matching across fixed row fields."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="Alpha Agent")
            _write_archive(repo_root, subject_kind="task", subject_id="Beta-Memory")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            response = client.get("/ui/continuity?q=ALPHA+healthy")
            archive_response = client.get("/ui/continuity?q=archive+beta-memory")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Alpha Agent", response.text)
        self.assertNotIn("Beta-Memory", response.text)
        self.assertIn('query "alpha healthy"', response.text)
        self.assertIn("case-insensitive substring matching", response.text)
        self.assertEqual(archive_response.status_code, 200)
        self.assertIn("Beta-Memory", archive_response.text)
        self.assertNotIn("Alpha Agent", archive_response.text)

    def test_ui_continuity_health_filter_and_missing_optional_fields_degrade_cleanly(self) -> None:
        """Health filtering should work while rows missing optional search fields remain searchable."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="Degraded User",
                capsule_health_status="degraded",
            )
            _write_cold(repo_root, subject_kind="user", subject_id="Cold Only")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            degraded_only = client.get("/ui/continuity?health_status=degraded")
            cold_search = client.get("/ui/continuity?q=cold+only")

        self.assertEqual(degraded_only.status_code, 200)
        self.assertIn("Degraded User", degraded_only.text)
        self.assertNotIn("Cold Only", degraded_only.text)
        self.assertIn("all lifecycle states; degraded", degraded_only.text)
        self.assertEqual(cold_search.status_code, 200)
        self.assertIn("Cold Only", cold_search.text)
        self.assertIn("memory/continuity/cold/index/user-Cold Only-20260415T093000Z.md", cold_search.text)

    def test_ui_continuity_combined_filters_are_conjunctive_and_preserve_chip_scope(self) -> None:
        """Combined q/kind/health/lifecycle filters should stay conjunctive while chips keep pre-lifecycle scope."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(
                repo_root,
                subject_kind="user",
                subject_id="Scoped Match",
                capsule_health_status="degraded",
            )
            _write_archive(
                repo_root,
                subject_kind="user",
                subject_id="Scoped Match",
                capsule_health_status="degraded",
            )
            _write_capsule(
                repo_root,
                subject_kind="peer",
                subject_id="Scoped Match",
                capsule_health_status="degraded",
            )
            _write_archive(repo_root, subject_kind="user", subject_id="Healthy Match")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            response = client.get(
                "/ui/continuity?q=%20SCOPED%20MATCH%20&subject_kind=user&artifact_state=archived&health_status=degraded"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn('value="scoped match"', response.text)
        self.assertIn('query "scoped match"; user; archived; degraded', response.text)
        self.assertIn("Showing 1 result(s) from 1 matched row(s).", response.text)
        self.assertIn("Scoped Match", response.text)
        self.assertNotIn("Healthy Match", response.text)
        self.assertNotIn("/ui/continuity/peer/Scoped%20Match", response.text)
        self.assertIn("active 1", response.text)
        self.assertIn("archived 1", response.text)
        self.assertIn("Lifecycle chips reflect the current subject/query/health scope before lifecycle-state narrowing", response.text)

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
        self.assertIn("Open user archived list", detail.text)
        self.assertIn("Open user cold list", detail.text)
        self.assertIn("memory/continuity/archive/user-stef-20260415T093000Z.json", detail.text)
        self.assertIn("memory/continuity/cold/index/user-stef-20260415T093000Z.md", detail.text)

    def test_ui_events_stream_bounded_snapshot_for_current_scope(self) -> None:
        """The SSE endpoint should stream one deterministic bounded snapshot payload."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="active-user")
            _write_archive(repo_root, subject_kind="user", subject_id="archived-user")
            self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )
            import app.ui.router as ui_router

            router = ui_router.build_ui_router(app_version="test-version")
            endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/events")
            request = _request_with_transport_host(
                "127.0.0.1",
                path="/ui/events",
                query_string=b"artifact_state=archived",
            )
            response = asyncio.run(
                endpoint(
                    request,
                    q=None,
                    subject_kind=None,
                    artifact_state="archived",
                    health_status=None,
                    detail_subject_kind=None,
                    detail_subject_id=None,
                )
            )
            event = asyncio.run(_read_first_sse_event_chunk(response))

        self.assertEqual(response.media_type, "text/event-stream")
        self.assertEqual(event["event"], "ui-snapshot")
        payload = json.loads(event["data"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertEqual(payload["continuity"]["scope"]["artifact_state"], "archived")
        self.assertEqual(payload["continuity"]["matched_count"], 1)
        self.assertEqual(payload["continuity"]["artifact_counts"]["archived"], 1)
        self.assertEqual(payload["continuity"]["recent_change"]["subject_id"], "archived-user")
        self.assertNotIn("status_label", payload["overview"])

    def test_ui_events_stream_degrades_instead_of_failing_on_snapshot_error(self) -> None:
        """Snapshot failures should stay in-band as degraded SSE payloads instead of breaking the stream."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="active-user")
            self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )
            import app.ui.router as ui_router

            router = ui_router.build_ui_router(app_version="test-version")
            endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/events")
            request = _request_with_transport_host("127.0.0.1", path="/ui/events")

            with patch("app.ui.router._ui_live_continuity_summary", side_effect=RuntimeError("boom")):
                response = asyncio.run(
                    endpoint(
                        request,
                        q=None,
                        subject_kind=None,
                        artifact_state=None,
                        health_status=None,
                        detail_subject_kind=None,
                        detail_subject_id=None,
                    )
                )
                event = asyncio.run(_read_first_sse_event_chunk(response))

        self.assertEqual(response.media_type, "text/event-stream")
        payload = json.loads(event["data"])
        self.assertFalse(payload["ok"])
        self.assertIn("ui_continuity_snapshot_failed:RuntimeError", payload["warnings"])
        self.assertFalse(payload["continuity"]["available"])
        self.assertEqual(payload["continuity"]["latest_recorded_at"], "unavailable")

    def test_ui_events_stream_includes_bounded_detail_summary_when_requested(self) -> None:
        """The SSE endpoint should expose a small detail summary for one subject when requested."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="stef")
            _write_archive(repo_root, subject_kind="user", subject_id="stef")
            self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )
            import app.ui.router as ui_router

            router = ui_router.build_ui_router(app_version="test-version")
            endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/events")
            request = _request_with_transport_host(
                "127.0.0.1",
                path="/ui/events",
                query_string=b"detail_subject_kind=user&detail_subject_id=stef",
            )
            response = asyncio.run(
                endpoint(
                    request,
                    q=None,
                    subject_kind=None,
                    artifact_state=None,
                    health_status=None,
                    detail_subject_kind="user",
                    detail_subject_id="stef",
                )
            )
            event = asyncio.run(_read_first_sse_event_chunk(response))

        payload = json.loads(event["data"])
        self.assertIsNotNone(payload["detail"])
        self.assertTrue(payload["detail"]["available"])
        self.assertEqual(payload["detail"]["subject_kind"], "user")
        self.assertEqual(payload["detail"]["subject_id"], "stef")
        self.assertEqual(payload["detail"]["artifact_counts"]["active"], 1)
        self.assertEqual(payload["detail"]["artifact_counts"]["archived"], 1)
        self.assertEqual(payload["detail"]["source_state"], "active")
        self.assertEqual(payload["detail"]["recovery_warning_count"], 0)

    def test_ui_pages_include_progressive_live_update_hooks_without_js_requirement(self) -> None:
        """Overview, continuity, and detail pages should expose optional live hooks while remaining server-rendered."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="stef")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            overview = client.get("/ui/")
            continuity = client.get("/ui/continuity")
            detail = client.get("/ui/continuity/user/stef")

        self.assertEqual(overview.status_code, 200)
        self.assertIn('/ui/static/ui_live.js', overview.text)
        self.assertIn('data-live-page="overview"', overview.text)
        self.assertIn('data-live-stream="/ui/events"', overview.text)
        self.assertIn("JavaScript disabled; live updates are unavailable.", overview.text)
        self.assertEqual(continuity.status_code, 200)
        self.assertIn('data-live-page="continuity"', continuity.text)
        self.assertIn("Data refreshed:", continuity.text)
        self.assertIn("Showing <span data-live-displayed-count>", continuity.text)
        self.assertEqual(detail.status_code, 200)
        self.assertIn('data-live-page="detail"', detail.text)
        self.assertIn("Current source state:", detail.text)
        self.assertIn("Capsule updated at:", detail.text)
        self.assertIn("Recovery warnings:", detail.text)

    def test_ui_layout_vendors_mucss_and_exposes_dark_theme_selector(self) -> None:
        """The operator UI should serve the vendored µCSS slate theme, dark-default selector, and shared back-to-top control."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_capsule(repo_root, subject_kind="user", subject_id="stef")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            overview = client.get("/ui/")
            theme_css = client.get("/ui/static/mu.slate.css")

        self.assertEqual(overview.status_code, 200)
        self.assertIn('/ui/static/mu.slate.css', overview.text)
        self.assertIn('data-theme="dark"', overview.text)
        self.assertIn('data-theme-select', overview.text)
        self.assertIn('data-back-to-top', overview.text)
        self.assertIn('<option value="dark" selected="selected">Dark</option>', overview.text)
        self.assertEqual(theme_css.status_code, 200)
        self.assertIn("µCSS", theme_css.text)

    def test_ui_live_script_backoff_grows_and_caps_at_declared_max_delay(self) -> None:
        """Reconnect delay should grow exponentially and then stop at the declared cap."""
        policy = _ui_live_backoff_policy()

        self.assertEqual(policy["LIVE_BASE_DELAY_MS"], 1000)
        self.assertEqual(policy["LIVE_MAX_DELAY_MS"], 16000)
        self.assertEqual(
            [_simulated_backoff_delay(attempt) for attempt in range(1, 8)],
            [1000, 2000, 4000, 8000, 16000, 16000, 16000],
        )

    def test_ui_live_script_reconnect_state_transitions_match_threshold(self) -> None:
        """Reconnect attempts should stay reconnecting until the offline threshold is reached."""
        policy = _ui_live_backoff_policy()

        self.assertEqual(policy["LIVE_OFFLINE_THRESHOLD"], 4)
        self.assertEqual(
            [_simulated_reconnect_state(attempt) for attempt in range(1, 6)],
            ["reconnecting", "reconnecting", "reconnecting", "offline", "offline"],
        )

    def test_ui_live_script_declares_visible_connection_states_and_degraded_paths(self) -> None:
        """The script should still explicitly surface connected, reconnecting, degraded, and offline states."""
        script = _ui_live_script()

        self.assertIn("function reconnectState(attempt)", script)
        self.assertIn('setState(root, "connected"', script)
        self.assertIn('setState(root, "reconnecting"', script)
        self.assertIn('setState(root, "degraded"', script)
        self.assertIn('setState(root, "offline"', script)
        self.assertIn("Live updates degraded; malformed snapshot ignored.", script)
        self.assertIn("Live updates connected with degraded snapshot data.", script)

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

    def test_ui_detail_page_live_region_includes_bounded_recovery_warning_count(self) -> None:
        """Detail live coverage may expand only within the header/meta region."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_archive(repo_root, subject_kind="user", subject_id="history-only")
            client = self._client(
                repo_root,
                COGNIRELAY_UI_ENABLED="true",
                COGNIRELAY_UI_REQUIRE_LOCALHOST="false",
            )

            detail = client.get("/ui/continuity/user/history-only")

        self.assertEqual(detail.status_code, 200)
        self.assertIn('data-live-detail-warning-count', detail.text)
        self.assertIn("Recovery warnings: <span data-live-detail-warning-count>2</span>", detail.text)


if __name__ == "__main__":
    unittest.main()
