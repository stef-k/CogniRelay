"""Tests for #213 slice 1 deterministic mixed retrieval."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.context.service import _assemble_mixed_retrieval_bundle, context_retrieve_service
from app.models import ContextRetrieveRequest


class _AuthStub:
    """Auth stub that allows all reads used in mixed retrieval tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class TestMixedRetrievalSlice1(unittest.TestCase):
    """Validate the bounded deterministic mixed retrieval contract."""

    def test_thread_selector_uses_exact_phase_order_and_same_class_deduplication(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-abc")
        auth = _AuthStub()
        calls: list[tuple[str, object]] = []
        capsule = {
            "subject_kind": "thread",
            "subject_id": "thread-abc",
            "continuity": {
                "related_documents": [
                    {"path": "docs/specs/thread-abc.md", "kind": "spec", "label": "Spec"},
                    {"path": "docs/notes/thread-abc.md", "kind": "note", "label": "Notes"},
                    {"path": "docs/specs/thread-abc.md", "kind": "spec", "label": "Spec duplicate"},
                ]
            },
        }

        def _fake_continuity_read_service(**kwargs: object) -> dict[str, object]:
            calls.append(("continuity", kwargs["req"]))
            return {"ok": True, "capsule": capsule}

        def _fake_read_file_service(**kwargs: object) -> dict[str, object]:
            path = kwargs["path"]
            calls.append(("read", path))
            return {"ok": True, "path": path, "content": f"content for {path}"}

        def _fake_search_service(**kwargs: object) -> dict[str, object]:
            search_req = kwargs["req"]
            calls.append(("search", search_req))
            return {
                "ok": True,
                "results": [
                    {"path": "journal/2026/2026-04-20.md", "score": 3.0},
                    {"path": "docs/specs/thread-abc.md", "score": 2.0},
                    {"path": "journal/2026/2026-04-20.md", "score": 1.0},
                ],
            }

        with (
            patch("app.context.service.continuity_read_service", side_effect=_fake_continuity_read_service),
            patch("app.context.service.read_file_service", side_effect=_fake_read_file_service),
            patch("app.context.service.search_service", side_effect=_fake_search_service),
        ):
            result = _assemble_mixed_retrieval_bundle(
                repo_root=Path("."),
                auth=auth,
                req=req,
                now=datetime.now(timezone.utc),
            )

        self.assertEqual(result["continuity"], [capsule])
        self.assertEqual(
            result["supporting_documents"],
            [
                {
                    "ok": True,
                    "path": "docs/specs/thread-abc.md",
                    "content": "content for docs/specs/thread-abc.md",
                },
                {
                    "ok": True,
                    "path": "docs/notes/thread-abc.md",
                    "content": "content for docs/notes/thread-abc.md",
                },
            ],
        )
        self.assertEqual(
            result["search_hits"],
            [
                {"path": "journal/2026/2026-04-20.md", "score": 3.0},
                {"path": "docs/specs/thread-abc.md", "score": 2.0},
            ],
        )
        self.assertEqual([name for name, _ in calls], ["continuity", "read", "read", "read", "search"])
        continuity_req = calls[0][1]
        self.assertEqual(continuity_req.subject_kind, "thread")
        self.assertEqual(continuity_req.subject_id, "thread-abc")
        self.assertFalse(continuity_req.allow_fallback)
        search_req = calls[-1][1]
        self.assertEqual(search_req.query, "thread-abc")
        self.assertEqual(search_req.sort_by, "relevance")
        self.assertEqual(search_req.include_types, [])

    def test_missing_capsule_skips_related_documents_and_still_executes_search(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="task", subject_id="task-42")
        calls: list[str] = []

        def _fake_continuity_read_service(**_kwargs: object) -> dict[str, object]:
            calls.append("continuity")
            raise HTTPException(status_code=404, detail="File not found")

        def _fake_search_service(**kwargs: object) -> dict[str, object]:
            calls.append("search")
            search_req = kwargs["req"]
            return {"ok": True, "results": [{"path": "docs/tasks/task-42.md", "score": 1.0, "query": search_req.query}]}

        with (
            patch("app.context.service.continuity_read_service", side_effect=_fake_continuity_read_service),
            patch("app.context.service.read_file_service") as read_file_service_mock,
            patch("app.context.service.search_service", side_effect=_fake_search_service),
        ):
            result = _assemble_mixed_retrieval_bundle(
                repo_root=Path("."),
                auth=_AuthStub(),
                req=req,
                now=datetime.now(timezone.utc),
            )

        self.assertEqual(result["continuity"], [])
        self.assertEqual(result["supporting_documents"], [])
        self.assertEqual(result["search_hits"], [{"path": "docs/tasks/task-42.md", "score": 1.0, "query": "task-42"}])
        self.assertEqual(calls, ["continuity", "search"])
        read_file_service_mock.assert_not_called()

    def test_phase_failures_degrade_without_synthetic_placeholders(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-abc")
        capsule = {
            "subject_kind": "thread",
            "subject_id": "thread-abc",
            "continuity": {
                "related_documents": [
                    {"path": "docs/specs/thread-abc.md", "kind": "spec", "label": "Spec"},
                    {"path": "docs/notes/thread-abc.md", "kind": "note", "label": "Notes"},
                ]
            },
        }

        def _fake_read_file_service(**kwargs: object) -> dict[str, object]:
            path = kwargs["path"]
            if path == "docs/specs/thread-abc.md":
                return {"ok": False, "path": path, "warning": "not readable now"}
            raise HTTPException(status_code=404, detail="File not found")

        with (
            patch("app.context.service.continuity_read_service", return_value={"ok": True, "capsule": capsule}),
            patch("app.context.service.read_file_service", side_effect=_fake_read_file_service),
            patch("app.context.service.search_service", side_effect=RuntimeError("search backend offline")),
        ):
            result = _assemble_mixed_retrieval_bundle(
                repo_root=Path("."),
                auth=_AuthStub(),
                req=req,
                now=datetime.now(timezone.utc),
            )

        self.assertEqual(result["continuity"], [capsule])
        self.assertEqual(
            result["supporting_documents"],
            [{"ok": False, "path": "docs/specs/thread-abc.md", "warning": "not readable now"}],
        )
        self.assertEqual(result["search_hits"], [])

    def test_phase_2_continues_after_early_read_exception(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-abc")
        capsule = {
            "subject_kind": "thread",
            "subject_id": "thread-abc",
            "continuity": {
                "related_documents": [
                    {"path": "docs/specs/thread-abc.md", "kind": "spec", "label": "Spec"},
                    {"path": "docs/notes/thread-abc.md", "kind": "note", "label": "Notes"},
                ]
            },
        }

        def _fake_read_file_service(**kwargs: object) -> dict[str, object]:
            path = kwargs["path"]
            if path == "docs/specs/thread-abc.md":
                raise RuntimeError("transient read failure")
            return {"ok": True, "path": path, "content": f"content for {path}"}

        with (
            patch("app.context.service.continuity_read_service", return_value={"ok": True, "capsule": capsule}),
            patch("app.context.service.read_file_service", side_effect=_fake_read_file_service),
            patch("app.context.service.search_service", return_value={"ok": True, "results": []}),
        ):
            result = _assemble_mixed_retrieval_bundle(
                repo_root=Path("."),
                auth=_AuthStub(),
                req=req,
                now=datetime.now(timezone.utc),
            )

        self.assertEqual(
            result["supporting_documents"],
            [
                {
                    "ok": True,
                    "path": "docs/notes/thread-abc.md",
                    "content": "content for docs/notes/thread-abc.md",
                }
            ],
        )

    def test_phase_3_uses_degraded_normal_search_results_without_gating_on_ok(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-abc")

        with (
            patch("app.context.service.continuity_read_service", side_effect=HTTPException(status_code=404, detail="File not found")),
            patch(
                "app.context.service.search_service",
                return_value={
                    "ok": False,
                    "warning": "index temporarily stale",
                    "results": [
                        {"path": "docs/specs/thread-abc.md", "score": 2.0, "warning": "stale"},
                        {"path": "docs/notes/thread-abc.md", "score": 1.0},
                    ],
                },
            ),
        ):
            result = _assemble_mixed_retrieval_bundle(
                repo_root=Path("."),
                auth=_AuthStub(),
                req=req,
                now=datetime.now(timezone.utc),
            )

        self.assertEqual(
            result["search_hits"],
            [
                {"path": "docs/specs/thread-abc.md", "score": 2.0, "warning": "stale"},
                {"path": "docs/notes/thread-abc.md", "score": 1.0},
            ],
        )

    def test_context_retrieve_keeps_mixed_retrieval_internal_only(self) -> None:
        req = ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-abc")
        continuity_state = {
            "present": False,
            "requested_selectors": [],
            "omitted_selectors": [],
            "capsules": [],
            "selection_order": [],
            "budget": {"token_budget_hint": "normal"},
            "warnings": [],
            "fallback_used": False,
            "recovery_warnings": [],
            "trust_signals": None,
            "salience_metadata": None,
        }

        with (
            patch("app.context.service._load_core_memory", return_value=[]),
            patch("app.context.service.build_continuity_state", return_value=continuity_state),
            patch("app.context.service._index_health", return_value="healthy"),
            patch("app.context.service.search_index", return_value=[]),
            patch("app.context.service._assemble_mixed_retrieval_bundle") as mixed_retrieval_mock,
        ):
            now = datetime.now(timezone.utc)
            auth = _AuthStub()
            result = context_retrieve_service(
                repo_root=Path("."),
                auth=auth,
                req=req,
                now=now,
                audit=lambda *_args, **_kwargs: None,
            )

        mixed_retrieval_mock.assert_not_called()
        self.assertEqual(
            set(result["bundle"].keys()),
            {
                "task",
                "generated_at",
                "core_memory",
                "recent_relevant",
                "open_questions",
                "token_budget_hint",
                "time_window_days",
                "notes",
                "continuity_state",
            },
        )
        self.assertNotIn("mixed_retrieval", result["bundle"])
        self.assertNotIn("continuity", result["bundle"])
        self.assertNotIn("supporting_documents", result["bundle"])
        self.assertNotIn("search_hits", result["bundle"])


if __name__ == "__main__":
    unittest.main()
