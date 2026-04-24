"""Tests for #249 read-only task-centric UI views."""

from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from starlette.requests import Request


def _reload_ui_router():
    """Reload config and UI router so env-controlled settings are current."""
    import app.config as config_module
    import app.ui.router as ui_router_module

    importlib.reload(config_module)
    return importlib.reload(ui_router_module)


def _request(path: str, *, query_string: bytes = b"") -> Request:
    """Build a localhost UI request."""
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": query_string,
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "http_version": "1.1",
        }
    )


def _ui_html_response(
    repo_root: Path,
    *,
    route_path: str,
    request_path: str,
    query_string: bytes = b"",
    endpoint_kwargs: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """Render one UI route directly for deterministic assertions."""
    with patch.dict(
        os.environ,
        {
            "COGNIRELAY_REPO_ROOT": str(repo_root),
            "COGNIRELAY_AUTO_INIT_GIT": "true",
            "COGNIRELAY_AUDIT_LOG_ENABLED": "false",
        },
        clear=False,
    ):
        ui_router = _reload_ui_router()
        router = ui_router.build_ui_router(app_version="test-version")
        endpoint = next(route.endpoint for route in router.routes if route.path == route_path)
        response = endpoint(_request(request_path, query_string=query_string), **(endpoint_kwargs or {}))
        return SimpleNamespace(status_code=response.status_code, text=response.body.decode("utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a deterministic JSON artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _task_payload(task_id: str | None, **overrides: Any) -> dict[str, Any]:
    """Build a task artifact payload."""
    payload: dict[str, Any] = {
        "task_id": task_id,
        "title": f"Title {task_id}",
        "description": f"Description {task_id}",
        "status": "open",
        "owner_peer": "stef",
        "collaborators": ["alice"],
        "thread_id": "thread-249",
        "blocked_by": [],
        "created_at": "2026-04-20T00:00:00Z",
        "updated_at": "2026-04-21T00:00:00Z",
    }
    if task_id is None:
        payload.pop("task_id")
    payload.update(overrides)
    return payload


def _capsule_payload(task_id: str, related_documents: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Build a minimal task continuity capsule."""
    now = datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    continuity: dict[str, Any] = {
        "top_priorities": ["ship task UI"],
        "active_concerns": [],
        "active_constraints": [],
        "open_loops": [],
        "stance_summary": "read-only UI",
        "session_trajectory": [],
        "negative_decisions": [],
        "rationale_entries": [],
        "drift_signals": [],
    }
    if related_documents is not None:
        continuity["related_documents"] = related_documents
    return {
        "schema_version": "1.0",
        "subject_kind": "task",
        "subject_id": task_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {"producer": "test", "update_reason": "manual", "inputs": ["tests/test_ui_issue_249_task_views.py"]},
        "continuity": continuity,
        "confidence": {"continuity": 0.8, "relationship_model": 0.0},
        "freshness": {"freshness_class": "situational"},
    }


def _fallback_payload(capsule: dict[str, Any]) -> dict[str, Any]:
    """Wrap a capsule in the continuity fallback envelope used by read fallback."""
    return {
        "schema_type": "continuity_fallback_snapshot",
        "schema_version": "1.0",
        "captured_at": capsule["updated_at"],
        "source_path": f"memory/continuity/task-{capsule['subject_id']}.json",
        "verification_status": "verified",
        "health_status": "healthy",
        "capsule": capsule,
    }


class TaskViewsIssue249Tests(unittest.TestCase):
    """Exercise the #249 read-only task UI contract."""

    def test_task_list_empty_state_with_missing_roots_and_nav(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            response = _ui_html_response(Path(tmp), route_path="/ui/tasks", request_path="/ui/tasks")

        self.assertEqual(response.status_code, 200)
        self.assertIn("No tasks matched the current filter.", response.text)
        self.assertIn("task_root_missing:tasks/open", response.text)
        self.assertIn("task_root_missing:tasks/done", response.text)
        self.assertIn('href="/ui/tasks"', response.text)
        self.assertIn('href="/ui/continuity"', response.text)
        self.assertIn('href="/ui/graph"', response.text)

    def test_task_list_open_done_filter_order_duplicates_and_degraded_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(repo / "tasks/open/task-a.json", _task_payload("shared", title="Open canonical", status="open", updated_at="2026-04-23T00:00:00Z"))
            _write_json(repo / "tasks/done/shared.json", _task_payload("shared", title="Done duplicate", status="done", updated_at="2026-04-24T00:00:00Z"))
            _write_json(repo / "tasks/open/blocked.json", _task_payload("blocked", status="blocked", updated_at="2026-04-22T00:00:00Z"))
            _write_json(repo / "tasks/open/in-progress.json", _task_payload("progress", status="in_progress", updated_at="2026-04-21T00:00:00Z"))
            _write_json(repo / "tasks/done/done.json", _task_payload("done", status="done", updated_at="2026-04-20T00:00:00Z"))
            (repo / "tasks/open/bad.json").write_text("{bad", encoding="utf-8")
            (repo / "tasks/open/not-object.json").write_text("[]", encoding="utf-8")
            unreadable = repo / "tasks/open/unreadable.json"
            unreadable.write_text(json.dumps(_task_payload("unreadable")), encoding="utf-8")
            unreadable.chmod(0)
            (repo / "tasks/open/note.txt").write_text("ignored", encoding="utf-8")
            (repo / "tasks/open/nested").mkdir()
            _write_json(repo / "tasks/open/nested/ignored.json", _task_payload("nested"))
            (repo / "tasks/open/link.json").symlink_to(repo / "tasks/open/task-a.json")

            response = _ui_html_response(Path(tmp), route_path="/ui/tasks", request_path="/ui/tasks")
            filtered = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks",
                request_path="/ui/tasks",
                query_string=b"status=blocked&q=alice",
                endpoint_kwargs={"status": "blocked", "q": "alice"},
            )

        self.assertIn("duplicate_task_artifacts:shared", response.text)
        self.assertIn("task_artifact_skipped:tasks/open/bad.json", response.text)
        self.assertIn("task_artifact_skipped:tasks/open/not-object.json", response.text)
        self.assertIn("task_artifact_skipped:tasks/open/unreadable.json", response.text)
        self.assertIn("Open canonical", response.text)
        self.assertNotIn("Done duplicate", response.text)
        self.assertIn("in_progress", response.text)
        self.assertIn("blocked", response.text)
        self.assertIn("done", response.text)
        self.assertLess(response.text.index("Open canonical"), response.text.index("Title blocked"))
        self.assertIn("Blocked By", response.text)
        self.assertIn("Related Documents", response.text)
        self.assertIn("Matched count</dt><dd>1</dd>", filtered.text)
        self.assertIn("blocked", filtered.text)
        self.assertNotIn("Open canonical", filtered.text)

    def test_search_ignores_non_string_scalars_and_non_string_collaborators(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(
                repo / "tasks/open/task-a.json",
                _task_payload(
                    "task-a",
                    title=123,
                    description={"nested": "needle"},
                    owner_peer=False,
                    collaborators=["human", 123, "", {"bad": "needle"}],
                ),
            )
            misses = _ui_html_response(Path(tmp), route_path="/ui/tasks", request_path="/ui/tasks", query_string=b"q=needle", endpoint_kwargs={"q": "needle"})
            hits = _ui_html_response(Path(tmp), route_path="/ui/tasks", request_path="/ui/tasks", query_string=b"q=human", endpoint_kwargs={"q": "human"})

        self.assertIn("Matched count</dt><dd>0</dd>", misses.text)
        self.assertIn("Matched count</dt><dd>1</dd>", hits.text)
        self.assertIn('<span class="muted">Untitled task</span>', hits.text)

    def test_task_detail_sections_relationship_links_related_docs_and_escaping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(
                repo / "tasks/open/task-a.json",
                _task_payload(
                    "task-a",
                    title="<script>alert(1)</script>",
                    description="<b>description</b>",
                    blocked_by=["dep-1", "slash/dep", "", 3],
                    related_documents=[
                        {"path": "docs/a.md", "kind": "spec", "label": "Artifact", "relevance": "high"},
                        {"path": "", "kind": "bad"},
                        {"path": "docs/a.md", "kind": "duplicate", "label": "Duplicate"},
                    ],
                    metadata={
                        "related_documents": [
                            {"path": "docs/a.md", "kind": "metadata", "label": "Same path different source"},
                            {"label": "missing path"},
                        ],
                        "other": "<script>metadata</script>",
                    },
                ),
            )
            _write_json(
                repo / "memory/continuity/task-task-a.json",
                _capsule_payload(
                    "task-a",
                    related_documents=[
                        {"path": "docs/c.md", "kind": "continuity", "label": "Continuity", "relevance": "medium"},
                        {"path": "", "kind": "bad"},
                    ],
                ),
            )
            response = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks/{task_id}",
                request_path="/ui/tasks/task-a",
                endpoint_kwargs={"task_id": "task-a"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertLess(response.text.index("<h2>Task</h2>"), response.text.index("<h2>Warnings</h2>"))
        self.assertLess(response.text.index("<h2>Relationships</h2>"), response.text.index("<h2>Related Documents</h2>"))
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", response.text)
        self.assertNotIn("<script>alert(1)</script>", response.text)
        self.assertIn("/ui/continuity/thread/thread-249", response.text)
        self.assertIn("/ui/graph?subject_kind=thread&amp;subject_id=thread-249", response.text)
        self.assertIn("/ui/continuity/task/task-a", response.text)
        self.assertIn("/ui/graph?subject_kind=task&amp;subject_id=task-a", response.text)
        self.assertIn("/ui/tasks/dep-1", response.text)
        self.assertIn("/ui/tasks?task_id=slash%2Fdep", response.text)
        self.assertIn("/ui/graph?subject_kind=task&amp;subject_id=slash%2Fdep", response.text)
        self.assertEqual(response.text.count("docs/a.md"), 2)
        self.assertIn("docs/c.md", response.text)
        self.assertIn("task_artifact", response.text)
        self.assertIn("task_metadata", response.text)
        self.assertIn("task_continuity", response.text)
        self.assertEqual(response.text.count("related_document_skipped:task_artifact"), 1)
        self.assertEqual(response.text.count("related_document_skipped:task_metadata"), 1)
        self.assertEqual(response.text.count("related_document_skipped:task_continuity"), 1)
        self.assertIn("No metadata recorded.", response.text)
        self.assertNotIn("metadata&lt;/script&gt;", response.text)
        self.assertNotIn('method="post"', response.text.lower())
        self.assertNotIn("delete", response.text.lower())
        self.assertNotIn("schedule", response.text.lower())

    def test_query_detail_slash_id_not_found_and_slash_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(
                repo / "tasks/open/slashy.json",
                _task_payload("a/b", thread_id="thread/with/slash", blocked_by=[]),
            )
            list_response = _ui_html_response(Path(tmp), route_path="/ui/tasks", request_path="/ui/tasks")
            missing = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks",
                request_path="/ui/tasks",
                query_string=b"task_id=missing%2Fid",
                endpoint_kwargs={"task_id": "missing/id"},
            )
            detail = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks",
                request_path="/ui/tasks",
                query_string=b"task_id=a%2Fb",
                endpoint_kwargs={"task_id": "a/b"},
            )

        self.assertIn("/ui/tasks?task_id=a%2Fb", list_response.text)
        self.assertIn("task_not_found:missing/id", missing.text)
        self.assertIn("Task ID</dt><dd>a/b</dd>", detail.text)
        self.assertIn("Continuity link unavailable for slash-containing ID.", detail.text)
        self.assertIn("/ui/graph?subject_kind=thread&amp;subject_id=thread%2Fwith%2Fslash", detail.text)
        self.assertIn("/ui/graph?subject_kind=task&amp;subject_id=a%2Fb", detail.text)
        self.assertNotIn("/ui/continuity/task/a%2Fb", detail.text)

    def test_invalid_empty_query_task_id_and_inferred_identity_collision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(repo / "tasks/open/inferred.json", _task_payload(None, title="Inferred"))
            _write_json(repo / "tasks/done/other.json", _task_payload("inferred", title="Explicit duplicate", status="done"))
            invalid = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks",
                request_path="/ui/tasks",
                query_string=b"task_id=%20%20",
                endpoint_kwargs={"task_id": "  "},
            )
            detail = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks/{task_id}",
                request_path="/ui/tasks/inferred",
                endpoint_kwargs={"task_id": "inferred"},
            )

        self.assertIn("task_not_found", invalid.text)
        self.assertNotIn("task_not_found:", invalid.text)
        self.assertIn("task_id_inferred:tasks/open/inferred.json", detail.text)
        self.assertIn("duplicate_task_artifacts:inferred", detail.text)
        self.assertIn("Inferred", detail.text)
        self.assertNotIn("Explicit duplicate", detail.text)

    def test_task_continuity_missing_recovery_and_unexpected_failure_degrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            _write_json(repo / "tasks/open/missing-continuity.json", _task_payload("missing-continuity"))
            missing = _ui_html_response(Path(tmp), route_path="/ui/tasks/{task_id}", request_path="/ui/tasks/missing-continuity", endpoint_kwargs={"task_id": "missing-continuity"})
            _write_json(repo / "tasks/open/fallback-task.json", _task_payload("fallback-task"))
            _write_json(
                repo / "memory/continuity/fallback/task-fallback-task.json",
                _fallback_payload(_capsule_payload("fallback-task", related_documents=[{"path": "docs/fallback.md"}])),
            )
            recovery = _ui_html_response(Path(tmp), route_path="/ui/tasks/{task_id}", request_path="/ui/tasks/fallback-task", endpoint_kwargs={"task_id": "fallback-task"})
            _write_json(repo / "tasks/open/fail-task.json", _task_payload("fail-task"))
            self.assertIn("docs/fallback.md", recovery.text)
            self.assertIn("task_continuity:continuity_active_missing", recovery.text)
            self.assertIn("task_continuity:continuity_fallback_used", recovery.text)
            with patch.dict(
                os.environ,
                {
                    "COGNIRELAY_REPO_ROOT": str(repo),
                    "COGNIRELAY_AUTO_INIT_GIT": "true",
                    "COGNIRELAY_AUDIT_LOG_ENABLED": "false",
                },
                clear=False,
            ):
                ui_router = _reload_ui_router()
                router = ui_router.build_ui_router(app_version="test-version")
                endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/tasks/{task_id}")
                with patch.object(ui_router, "continuity_read_service", side_effect=RuntimeError("boom")):
                    response = endpoint(_request("/ui/tasks/fail-task"), task_id="fail-task")
                    failure = SimpleNamespace(status_code=response.status_code, text=response.body.decode("utf-8"))

        self.assertNotIn("task_continuity:", missing.text)
        self.assertNotIn("task_continuity_unavailable:missing-continuity", missing.text)
        self.assertIn("task_continuity_unavailable:fail-task", failure.text)

    def test_not_found_warning_relevance_excludes_unrelated_artifact_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / "tasks/open").mkdir(parents=True)
            (repo / "tasks/open/bad.json").write_text("{bad", encoding="utf-8")
            response = _ui_html_response(
                Path(tmp),
                route_path="/ui/tasks",
                request_path="/ui/tasks",
                query_string=b"task_id=a%2Fb",
                endpoint_kwargs={"task_id": "a/b"},
            )

        self.assertIn("task_not_found:a/b", response.text)
        self.assertIn("task_root_missing:tasks/done", response.text)
        self.assertNotIn("task_artifact_skipped:tasks/open/bad.json", response.text)


if __name__ == "__main__":
    unittest.main()
