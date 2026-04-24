"""Tests for #251 read-only context retrieval inspector UI."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch
from urllib.parse import urlencode

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


def _ui_context_response(
    repo_root: Path,
    *,
    query: dict[str, str] | None = None,
    service: Any | None = None,
) -> tuple[SimpleNamespace, Any]:
    """Render /ui/context directly and return the response plus service mock."""
    query_string = urlencode(query or {}).encode("utf-8")
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
        endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/context")
        mock_service = service if service is not None else Mock(return_value=_retrieval_result())
        with patch.object(ui_router, "context_retrieve_service", mock_service):
            response = endpoint(_request("/ui/context", query_string=query_string))
        return SimpleNamespace(status_code=response.status_code, text=response.body.decode("utf-8")), mock_service


async def _first_sse_payload(repo_root: Path) -> dict[str, Any]:
    """Read the first emitted /ui/events snapshot payload."""
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
        endpoint = next(route.endpoint for route in router.routes if route.path == "/ui/events")
        response = await endpoint(
            _request("/ui/events"),
            q=None,
            subject_kind=None,
            artifact_state=None,
            health_status=None,
            detail_subject_kind=None,
            detail_subject_id=None,
            graph_subject_kind=None,
            graph_subject_id=None,
        )
        event: dict[str, str] = {}
        async for raw_chunk in response.body_iterator:
            chunk = raw_chunk.decode("utf-8") if isinstance(raw_chunk, bytes) else raw_chunk
            for line in chunk.splitlines():
                if not line:
                    if "data" in event:
                        await response.body_iterator.aclose()
                        return json.loads(event["data"])
                    continue
                if ": " not in line:
                    continue
                key, value = line.split(": ", 1)
                if key != "retry":
                    event[key] = value
        raise AssertionError("No SSE payload emitted")


def _retrieval_result(**overrides: Any) -> dict[str, Any]:
    """Build a representative context.retrieve service result."""
    bundle: dict[str, Any] = {
        "generated_at": "2026-04-24T12:00:00Z",
        "recent_relevant": [
            {
                "path": "docs/context.md",
                "type": "markdown",
                "score": 0.75,
                "modified_at": "2026-04-23T00:00:00Z",
                "importance": True,
                "warning": "index_stale",
                "snippet": "Relevant context",
                "subject_kind": "thread",
                "subject_id": "thread 251",
                "task_id": "task-251",
            }
        ],
        "open_questions": ["What changed?"],
        "notes": ["Inspect only."],
        "token_budget_hint": "within_budget",
        "continuity_state": {
            "present": True,
            "fallback_used": False,
            "requested_selector_count": 1,
            "omitted_selector_count": 0,
            "warnings": [],
            "recovery_warnings": [],
            "budget": {"token_budget_hint": "within_budget", "trimmed": False},
            "trust_signals": {"aggregate": "healthy"},
            "salience_metadata": {"source": "test"},
            "capsules": [
                {
                    "subject_kind": "thread",
                    "subject_id": "thread 251",
                    "source_state": "active",
                    "path": "memory/continuity/thread-thread-251.json",
                    "health_status": "healthy",
                    "trust_signals": {"verified": True},
                    "degraded": False,
                    "recovery_warnings": [],
                    "warnings": [],
                }
            ],
        },
    }
    bundle.update(overrides)
    return {"ok": True, "bundle": bundle}


class ContextInspectorIssue251Tests(unittest.TestCase):
    """Exercise the read-only context retrieval inspector contract."""

    def test_empty_default_state_nav_and_no_service_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = Mock(return_value=_retrieval_result())
            response, service = _ui_context_response(Path(tmp), service=service)

        self.assertEqual(response.status_code, 200)
        self.assertIn('action="/ui/context"', response.text)
        self.assertIn("No retrieval request has been run.", response.text)
        self.assertIn('href="/ui/tasks"', response.text)
        self.assertIn('href="/ui/context"', response.text)
        self.assertIn('href="/ui/graph"', response.text)
        self.assertLess(response.text.index('href="/ui/tasks"'), response.text.index('href="/ui/context"'))
        self.assertLess(response.text.index('href="/ui/context"'), response.text.index('href="/ui/graph"'))
        service.assert_not_called()

    def test_successful_retrieval_invokes_service_with_exact_defaults_and_renders_sections(self) -> None:
        captured: dict[str, Any] = {}

        def service(**kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return _retrieval_result()

        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(
                Path(tmp),
                query={"task": "  task free form  ", "subject_kind": "thread", "subject_id": "thread-251"},
                service=Mock(side_effect=service),
            )

        self.assertEqual(response.status_code, 200)
        req = captured["req"]
        self.assertEqual(req.task, "  task free form  ")
        self.assertEqual(req.subject_kind, "thread")
        self.assertEqual(req.subject_id, "thread-251")
        self.assertEqual(req.continuity_mode, "auto")
        self.assertEqual(req.continuity_verification_policy, "allow_degraded")
        self.assertEqual(req.continuity_resilience_policy, "allow_fallback")
        self.assertEqual(req.continuity_selectors, [])
        self.assertEqual(req.continuity_max_capsules, 1)
        self.assertEqual(req.max_tokens_estimate, 12000)
        self.assertEqual(req.include_types, [])
        self.assertEqual(req.time_window_days, 30)
        self.assertEqual(req.limit, 10)
        self.assertEqual(captured["auth"].scopes, {"read:files", "search"})
        self.assertEqual(captured["auth"].read_namespaces, {"*"})
        self.assertEqual(captured["auth"].write_namespaces, set())
        section_names = [
            "Selector",
            "Request Parameters",
            "Retrieval Status",
            "Warnings",
            "Recovery Warnings",
            "Token Budget",
            "Continuity State",
            "Recent Relevant",
            "Open Questions",
            "Notes",
        ]
        positions = [response.text.index(f"<h2>{name}</h2>") for name in section_names]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("2026-04-24T12:00:00Z", response.text)
        self.assertIn("within_budget", response.text)
        self.assertIn("thread 251", response.text)
        self.assertIn("Relevant context", response.text)
        self.assertIn("What changed?", response.text)
        self.assertIn("Inspect only.", response.text)

    def test_warning_and_degraded_retrieval_render_http_200(self) -> None:
        result = _retrieval_result()
        state = result["bundle"]["continuity_state"]
        state["warnings"] = ["continuity_missing_capsule"]
        state["recovery_warnings"] = ["continuity_index_stale"]
        state["fallback_used"] = True
        state["budget"] = {"token_budget_hint": "trimmed", "trimmed": True}
        state["capsules"][0]["source_state"] = "fallback"
        state["capsules"][0]["degraded"] = True
        state["capsules"][0]["degraded_reason"] = "fallback_used"
        state["capsules"][0]["trust_signals"] = {"stale": True}
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(
                Path(tmp),
                query={"task": "inspect", "subject_kind": "thread", "subject_id": "thread-251"},
                service=Mock(return_value=result),
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("continuity_missing_capsule", response.text)
        self.assertIn("continuity_index_stale", response.text)
        self.assertIn("fallback", response.text)
        self.assertIn("fallback_used", response.text)
        self.assertIn("trimmed", response.text)

    def test_service_exception_degrades_without_raw_exception_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(
                Path(tmp),
                query={"task": "inspect", "subject_kind": "thread", "subject_id": "thread-251"},
                service=Mock(side_effect=RuntimeError("raw secret exception")),
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("ui_context_retrieve_failed", response.text)
        self.assertIn("Bundle available</dt><dd>false</dd>", response.text)
        self.assertIn("inspect", response.text)
        self.assertNotIn("raw secret exception", response.text)

    def test_invalid_and_incomplete_inputs_warning_order_and_no_invocation(self) -> None:
        cases = [
            ({"task": "inspect", "subject_kind": "invalid"}, ["ui_context_invalid_subject_kind"], ["ui_context_subject_id_required"]),
            ({"task": "", "subject_kind": "", "subject_id": ""}, ["ui_context_task_required"], []),
            ({"task": "inspect", "subject_kind": "thread"}, ["ui_context_subject_id_required"], []),
            (
                {"task": "inspect", "subject_kind": "invalid", "subject_id": ""},
                ["ui_context_invalid_subject_kind"],
                ["ui_context_subject_id_required"],
            ),
            ({"task": "inspect", "subject_id": "thread-251"}, ["ui_context_subject_kind_required"], []),
            ({"task": "   ", "subject_kind": "thread", "subject_id": "thread-251"}, ["ui_context_task_required"], []),
        ]
        for query, expected, absent in cases:
            with self.subTest(query=query), tempfile.TemporaryDirectory() as tmp:
                service = Mock(return_value=_retrieval_result())
                response, service = _ui_context_response(Path(tmp), query=query, service=service)
                self.assertEqual(response.status_code, 200)
                for warning in expected:
                    self.assertIn(warning, response.text)
                for warning in absent:
                    self.assertNotIn(warning, response.text)
                service.assert_not_called()

        with tempfile.TemporaryDirectory() as tmp:
            response, service = _ui_context_response(
                Path(tmp),
                query={"task": "inspect", "subject_kind": "   ", "subject_id": "thread-251"},
                service=Mock(return_value=_retrieval_result()),
            )
        self.assertLess(response.text.index("ui_context_subject_kind_required"), response.text.index("Warnings</h2>") + 500)
        service.assert_not_called()

    def test_raw_form_values_are_redisplayed_and_escaped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            response, service = _ui_context_response(
                Path(tmp),
                query={
                    "task": '  <script>alert("task")</script>  ',
                    "subject_kind": '<b onclick="x">',
                    "subject_id": '<img src=x onerror=alert(1)>',
                },
                service=Mock(return_value=_retrieval_result()),
            )

        service.assert_not_called()
        self.assertIn('value="  &lt;script&gt;alert(&quot;task&quot;)&lt;/script&gt;  "', response.text)
        self.assertIn('value="&lt;b onclick=&quot;x&quot;&gt;"', response.text)
        self.assertIn('value="&lt;img src=x onerror=alert(1)&gt;"', response.text)
        self.assertNotIn('<script>alert("task")</script>', response.text)
        self.assertNotIn("<img src=x onerror=alert(1)>", response.text)

    def test_recent_relevant_scalars_malformed_items_order_and_links(self) -> None:
        result = _retrieval_result(
            recent_relevant=[
                {
                    "path": "first.md",
                    "type": "note",
                    "score": 1,
                    "modified_at": "2026-04-20",
                    "importance": False,
                    "warning": "warn",
                    "snippet": "first",
                    "subject_kind": "thread",
                    "subject_id": "thread/special",
                    "task_id": "task/special",
                },
                ["malformed"],
                {
                    "path": "second.md",
                    "type": "note",
                    "score": {"bad": "shape"},
                    "importance": ["bad"],
                    "snippet": "second",
                    "subject_kind": "user",
                    "subject_id": "safe user",
                    "task_id": "safe-task",
                },
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(
                Path(tmp),
                query={"task": "inspect"},
                service=Mock(return_value=result),
            )

        self.assertLess(response.text.index("first.md"), response.text.index("second.md"))
        self.assertIn("<td>1</td>", response.text)
        self.assertIn("<td>False</td>", response.text)
        self.assertIn("<td>warn</td>", response.text)
        self.assertIn("<td>n/a</td>", response.text)
        self.assertIn("/ui/graph?subject_kind=thread&amp;subject_id=thread%2Fspecial", response.text)
        self.assertNotIn("/ui/continuity/thread/thread%2Fspecial", response.text)
        self.assertIn("/ui/tasks?task_id=task%2Fspecial", response.text)
        self.assertIn("/ui/continuity/user/safe%20user", response.text)
        self.assertIn("/ui/tasks/safe-task", response.text)

    def test_html_escaping_for_service_returned_fields(self) -> None:
        result = _retrieval_result(
            generated_at="<script>generated</script>",
            recent_relevant=[
                {
                    "path": "<script>path</script>",
                    "type": "<b>type</b>",
                    "score": "<i>score</i>",
                    "modified_at": "<u>modified</u>",
                    "importance": "<em>importance</em>",
                    "warning": "<strong>warning</strong>",
                    "snippet": "<script>snippet</script>",
                }
            ],
            open_questions=["<script>question</script>"],
            notes=["<script>note</script>"],
            token_budget_hint="<script>budget</script>",
        )
        state = result["bundle"]["continuity_state"]
        state["warnings"] = ["<script>warning</script>"]
        state["recovery_warnings"] = ["<script>recovery</script>"]
        state["trust_signals"] = {"html": "<script>trust</script>"}
        state["capsules"][0]["path"] = "<script>capsule</script>"
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(
                Path(tmp),
                query={"task": "<script>task</script>"},
                service=Mock(return_value=result),
            )

        self.assertNotIn("<script>path</script>", response.text)
        self.assertNotIn("<script>question</script>", response.text)
        self.assertNotIn("<script>warning</script>", response.text)
        self.assertIn("&lt;script&gt;path&lt;/script&gt;", response.text)
        self.assertIn("&lt;script&gt;question&lt;/script&gt;", response.text)
        self.assertIn("&lt;script&gt;warning&lt;/script&gt;", response.text)

    def test_read_only_no_mutation_controls_and_sse_deferred(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            response, _service = _ui_context_response(Path(tmp), query={"task": "inspect"})
            sse_payload = asyncio.run(_first_sse_payload(Path(tmp)))

        self.assertNotIn('method="post"', response.text.lower())
        self.assertNotIn("delete", response.text.lower())
        self.assertNotIn("reminder", response.text.lower())
        self.assertNotIn("schedule", response.text.lower())
        self.assertNotIn("onboarding", response.text.lower())
        self.assertNotIn('data-live-page="context"', response.text)
        self.assertNotIn("retrieval", sse_payload)
        self.assertNotIn("context", sse_payload)


if __name__ == "__main__":
    unittest.main()
