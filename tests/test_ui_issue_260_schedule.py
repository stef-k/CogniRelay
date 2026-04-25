"""Tests for #260 read-only schedule/reminder UI inspection."""

from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

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


def _ui_schedule_response(
    repo_root: Path,
    *,
    query_string: bytes = b"",
    service: Any | None = None,
) -> tuple[SimpleNamespace, Any]:
    """Render /ui/schedule directly and return the response plus service mock."""
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
        endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/schedule")
        mock_service = service if service is not None else Mock(return_value=_schedule_result())
        with patch.object(ui_router, "schedule_list_service", mock_service):
            response = endpoint(_request("/ui/schedule", query_string=query_string))
        return SimpleNamespace(status_code=response.status_code, text=response.body.decode("utf-8")), mock_service


def _schedule_item(**overrides: Any) -> dict[str, Any]:
    """Build a representative schedule item."""
    item: dict[str, Any] = {
        "schedule_id": "sched_pending",
        "kind": "task_nudge",
        "status": "pending",
        "derived_state": "scheduled",
        "title": "Check build",
        "due_at": "2026-05-01T12:00:00Z",
        "task_id": "task-260",
        "thread_id": "thread-260",
        "subject_kind": "thread",
        "subject_id": "thread-260",
        "updated_at": "2026-04-25T10:00:00Z",
    }
    item.update(overrides)
    return item


def _schedule_result(*, items: list[dict[str, Any]] | None = None, warnings: list[str] | None = None, total: int | None = None) -> dict[str, Any]:
    """Build a schedule.list service response."""
    rows = [_schedule_item()] if items is None else items
    return {
        "ok": True,
        "count": len(rows),
        "total": len(rows) if total is None else total,
        "limit": 200,
        "offset": 0,
        "items": rows,
        "warnings": [] if warnings is None else warnings,
    }


class ScheduleUiIssue260Tests(unittest.TestCase):
    """Exercise the read-only schedule inspection UI contract."""

    def test_route_renders_nav_between_tasks_and_retrieval_and_calls_service_once(self) -> None:
        captured: dict[str, Any] = {}

        def service(**kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return _schedule_result()

        with tempfile.TemporaryDirectory() as tmp:
            response, service_mock = _ui_schedule_response(Path(tmp), service=Mock(side_effect=service))

        self.assertEqual(response.status_code, 200)
        self.assertIn("<title>Schedule</title>", response.text)
        self.assertIn('href="/ui/schedule"', response.text)
        self.assertLess(response.text.index('href="/ui/tasks"'), response.text.index('href="/ui/schedule"'))
        self.assertLess(response.text.index('href="/ui/schedule"'), response.text.index('href="/ui/context"'))
        self.assertIn('<a class="nav-link active" href="/ui/schedule">Schedule</a>', response.text)
        service_mock.assert_called_once()
        self.assertEqual(captured["repo_root"], Path(tmp))
        self.assertEqual(captured["auth"].scopes, {"read:files"})
        self.assertEqual(captured["query"], {"limit": 200, "offset": 0, "include_retired": False})

    def test_required_columns_rows_states_counts_and_truncation_render(self) -> None:
        items = [
            _schedule_item(schedule_id="sched_scheduled", derived_state="scheduled", title="Future"),
            _schedule_item(schedule_id="sched_due", derived_state="due", title="Due now"),
            _schedule_item(schedule_id="sched_done", status="done", derived_state="terminal", title="Terminal"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_schedule_response(Path(tmp), service=Mock(return_value=_schedule_result(items=items, total=250)))

        for column in ("schedule_id", "kind", "status", "derived_state", "title", "due_at", "task_id", "thread_id", "subject_kind", "subject_id", "updated_at"):
            self.assertIn(f"<th>{column}</th>", response.text)
        self.assertIn("sched_scheduled", response.text)
        self.assertIn("sched_due", response.text)
        self.assertIn("sched_done", response.text)
        self.assertIn("scheduled", response.text)
        self.assertIn("due", response.text)
        self.assertIn("terminal", response.text)
        self.assertIn("<dt>count</dt><dd>3</dd>", response.text)
        self.assertIn("<dt>service_count</dt><dd>3</dd>", response.text)
        self.assertIn("<dt>service_total</dt><dd>250</dd>", response.text)
        self.assertIn("<dt>limit</dt><dd>200</dd>", response.text)
        self.assertIn("<dt>truncated</dt><dd>true</dd>", response.text)
        self.assertIn("Only the first 200 service-ordered rows are available in this UI slice.", response.text)

    def test_status_and_include_retired_filter_validation_and_service_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            valid, valid_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=pending&include_retired=true",
                service=Mock(return_value=_schedule_result()),
            )
            retired, retired_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=retired",
                service=Mock(return_value=_schedule_result(items=[_schedule_item(status="retired", derived_state="terminal")])),
            )
            invalid, invalid_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=bogus&include_retired=yes",
                service=Mock(return_value=_schedule_result(items=[])),
            )
            padded, padded_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=%20pending%20",
                service=Mock(return_value=_schedule_result(items=[])),
            )
            all_filter, all_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=&include_retired=false",
                service=Mock(return_value=_schedule_result()),
            )

        self.assertNotIn("invalid_schedule_ui_filter", valid.text)
        self.assertEqual(valid_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": True, "status": "pending"})
        self.assertEqual(retired_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False, "status": "retired"})
        self.assertIn("retired", retired.text)
        self.assertEqual(invalid_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertIn("invalid_schedule_ui_filter:status", invalid.text)
        self.assertIn("invalid_schedule_ui_filter:include_retired", invalid.text)
        self.assertEqual(padded_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertIn("invalid_schedule_ui_filter:status", padded.text)
        self.assertEqual(all_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertNotIn("invalid_schedule_ui_filter", all_filter.text)

    def test_derived_state_and_q_filter_are_ui_side_and_raw_q_is_redisplayed(self) -> None:
        items = [
            _schedule_item(schedule_id="sched_alpha", derived_state="scheduled", title="Alpha Build"),
            _schedule_item(schedule_id="sched_beta", derived_state="due", title="Beta Build", task_id="task-beta"),
            _schedule_item(schedule_id="sched_gamma", derived_state="terminal", title="Gamma"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            response, service = _ui_schedule_response(
                Path(tmp),
                query_string=b"derived_state=due&q=++BETA%09task-beta++&q=ignored",
                service=Mock(return_value=_schedule_result(items=items)),
            )
            invalid, invalid_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"derived_state=late",
                service=Mock(return_value=_schedule_result(items=items)),
            )
            padded, padded_service = _ui_schedule_response(
                Path(tmp),
                query_string=b"derived_state=%20due%20",
                service=Mock(return_value=_schedule_result(items=items)),
            )

        self.assertEqual(service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertIn('value="  BETA\ttask-beta  "', response.text)
        self.assertIn("sched_beta", response.text)
        self.assertNotIn("sched_alpha", response.text)
        self.assertNotIn("sched_gamma", response.text)
        self.assertIn("<dt>count</dt><dd>1</dd>", response.text)
        self.assertEqual(invalid_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertIn("invalid_schedule_ui_filter:derived_state", invalid.text)
        self.assertEqual(padded_service.call_args.kwargs["query"], {"limit": 200, "offset": 0, "include_retired": False})
        self.assertIn("invalid_schedule_ui_filter:derived_state", padded.text)

    def test_link_order_and_url_encoding_for_task_thread_subject_and_retrieval(self) -> None:
        items = [
            _schedule_item(
                schedule_id="sched_links",
                task_id="task/260",
                thread_id="thread/260",
                subject_kind="task",
                subject_id="subject/260",
            ),
            _schedule_item(
                schedule_id="sched_subject_retrieval",
                task_id=None,
                thread_id="thread 260",
                subject_kind="thread",
                subject_id="thread 260",
            ),
            _schedule_item(
                schedule_id="sched_peer",
                task_id="",
                thread_id="",
                subject_kind="peer",
                subject_id="peer-260",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_schedule_response(Path(tmp), service=Mock(return_value=_schedule_result(items=items)))

        self.assertIn('<a href="/ui/tasks?task_id=task%2F260">task/260</a> · <a href="/ui/context?task=task%2F260">Retrieval</a>', response.text)
        self.assertIn('thread/260 · <a href="/ui/graph?subject_kind=thread&amp;subject_id=thread%2F260">Graph</a>', response.text)
        self.assertIn('subject/260 · <a href="/ui/graph?subject_kind=task&amp;subject_id=subject%2F260">Graph</a>', response.text)
        self.assertIn('thread 260 · <a href="/ui/continuity/thread/thread%20260">Continuity</a> · <a href="/ui/graph?subject_kind=thread&amp;subject_id=thread+260">Graph</a>', response.text)
        self.assertIn(
            'thread 260 · <a href="/ui/continuity/thread/thread%20260">Continuity</a> · '
            '<a href="/ui/graph?subject_kind=thread&amp;subject_id=thread+260">Graph</a> · '
            '<a href="/ui/context?subject_kind=thread&amp;subject_id=thread+260">Retrieval</a>',
            response.text,
        )
        self.assertIn('peer-260 · <a href="/ui/continuity/peer/peer-260">Continuity</a> · <a href="/ui/context?subject_kind=peer&amp;subject_id=peer-260">Retrieval</a>', response.text)
        self.assertNotIn('/ui/continuity/task/subject%2F260', response.text)

    def test_empty_degraded_malformed_and_exception_states_render_http_200(self) -> None:
        degraded_codes = ["schedule_db_missing", "schedule_db_locked", "schedule_db_corrupt", "schedule_schema_too_new", "schedule_bootstrap_failed"]
        for code in degraded_codes:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as tmp:
                response, _service = _ui_schedule_response(
                    Path(tmp),
                    service=Mock(return_value=_schedule_result(items=[], warnings=[code])),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn(code, response.text)
                self.assertIn("No schedule items matched the current filters.", response.text)

        malformed_items = [_schedule_item(schedule_id=["bad"], title={"nested": "bad"}, task_id=None)]
        with tempfile.TemporaryDirectory() as tmp:
            malformed, _service = _ui_schedule_response(
                Path(tmp),
                service=Mock(return_value=_schedule_result(items=malformed_items, warnings=["schedule_row_invalid:sched_bad", "schedule_rows_skipped"])),
            )
            failed, _service = _ui_schedule_response(
                Path(tmp),
                service=Mock(side_effect=RuntimeError("raw secret")),
            )
            failed_invalid_filter, _service = _ui_schedule_response(
                Path(tmp),
                query_string=b"status=bogus",
                service=Mock(side_effect=RuntimeError("raw secret")),
            )

        self.assertEqual(malformed.status_code, 200)
        self.assertIn("schedule_rows_skipped", malformed.text)
        self.assertIn("schedule_row_invalid:sched_bad", malformed.text)
        self.assertIn('<span class="muted">n/a</span>', malformed.text)
        self.assertEqual(failed.status_code, 200)
        self.assertIn("schedule_ui_service_exception:RuntimeError", failed.text)
        self.assertNotIn("raw secret", failed.text)
        self.assertIn("No schedule items matched the current filters.", failed.text)
        self.assertIn("schedule_ui_service_exception:RuntimeError", failed_invalid_filter.text)
        self.assertIn("invalid_schedule_ui_filter:status", failed_invalid_filter.text)
        self.assertLess(
            failed_invalid_filter.text.index("schedule_ui_service_exception:RuntimeError"),
            failed_invalid_filter.text.index("invalid_schedule_ui_filter:status"),
        )

    def test_read_only_no_mutation_controls_services_or_schedule_live_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_schedule_response(Path(tmp), service=Mock(return_value=_schedule_result()))

        lowered = response.text.lower()
        self.assertNotIn('method="post"', lowered)
        self.assertNotIn('data-live-page="schedule"', lowered)
        for forbidden in ("create", "update", "edit", "acknowledge", "retire", "delete", "remove", "complete", "mutate"):
            self.assertNotIn(f">{forbidden}<", lowered)
            self.assertNotIn(f'value="{forbidden}"', lowered)

        import app.ui.router as ui_router

        self.assertFalse(hasattr(ui_router, "schedule_create_service"))
        self.assertFalse(hasattr(ui_router, "schedule_update_service"))
        self.assertFalse(hasattr(ui_router, "schedule_acknowledge_service"))
        self.assertFalse(hasattr(ui_router, "schedule_retire_service"))
        self.assertNotIn('data-live-page="schedule"', response.text)
        script = (Path(__file__).resolve().parents[1] / "app" / "ui" / "static" / "ui_live.js").read_text(encoding="utf-8")
        self.assertNotIn('"schedule"', script)
        self.assertNotIn("payload.schedule", script)
        self.assertNotIn("applySchedule", script)


if __name__ == "__main__":
    unittest.main()
