"""Tests for #256 graph context runtime embedding."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.context.graph import CONTEXT_GRAPH_CAPS, compact_agent_graph, derive_agent_graph_context
from app.context.service import context_retrieve_service
from app.continuity.service import continuity_read_service
from app.help.service import help_limit_payload, help_limits_index_payload, help_tool_payload
from app.main import discovery_tools
from app.models import ContextRetrieveRequest, ContinuityReadRequest


class _AuthStub:
    peer_id = "peer-test"

    def __init__(self, *, denied_paths: set[str] | None = None, denied_scopes: set[str] | None = None) -> None:
        self.denied_paths = denied_paths or set()
        self.denied_scopes = denied_scopes or set()

    def require(self, scope: str) -> None:
        if scope in self.denied_scopes:
            raise HTTPException(status_code=403, detail=f"Missing scope: {scope}")

    def require_read_path(self, path: str) -> None:
        if path in self.denied_paths:
            raise HTTPException(status_code=403, detail=f"Read path namespace not allowed: {path}")


def _now() -> datetime:
    return datetime(2026, 4, 25, 9, 0, 0, tzinfo=timezone.utc)


def _capsule(*, subject_kind: str, subject_id: str, related_documents: list[dict[str, str]] | None = None) -> dict[str, object]:
    return {
        "schema_version": "1.1",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": "2026-04-25T09:00:00Z",
        "verified_at": "2026-04-25T09:00:00Z",
        "verification_kind": "self_review",
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": {
            "top_priorities": [],
            "active_concerns": [],
            "active_constraints": [],
            "open_loops": [],
            "stance_summary": "Ready.",
            "drift_signals": [],
            "related_documents": related_documents or [],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.0},
    }


def _write_json(repo_root: Path, rel: str, payload: object) -> None:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _create_graph_roots(repo_root: Path) -> None:
    for rel in (
        "tasks/open",
        "tasks/done",
        "memory/continuity",
        "memory/continuity/fallback",
        "memory/continuity/archive",
        "memory/continuity/cold/index",
    ):
        (repo_root / rel).mkdir(parents=True, exist_ok=True)


class TestGraphRuntime256(unittest.TestCase):
    def test_context_retrieve_includes_bounded_graph_context_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1", "blocked_by": ["task-0"]})
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))

            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContextRetrieveRequest(task="continue", subject_kind="thread", subject_id="thread-1"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        graph = result["bundle"]["graph_context"]
        self.assertEqual(graph["anchor"], {"id": "thread:thread-1", "kind": "thread", "subject_id": "thread-1"})
        self.assertEqual(graph["nodes"], [{"id": "task:task-1", "kind": "task", "subject_id": "task-1"}])
        self.assertEqual(graph["warnings"], [])
        self.assertEqual(graph["truncation"]["nodes"]["limit"], 24)

    def test_context_retrieve_continuity_mode_off_suppresses_graph(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContextRetrieveRequest(task="continue", subject_kind="thread", subject_id="thread-1", continuity_mode="off"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        graph = result["bundle"]["graph_context"]
        self.assertIsNone(graph["anchor"])
        self.assertEqual(graph["nodes"], [])
        self.assertEqual(graph["warnings"][0]["code"], "graph_suppressed_by_continuity_mode")

    def test_context_retrieve_uses_first_eligible_selector(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-2.json", {"task_id": "task-2", "thread_id": "thread-2"})
            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContextRetrieveRequest(
                    task="continue",
                    subject_kind="user",
                    subject_id="user-1",
                    continuity_selectors=[{"subject_kind": "task", "subject_id": "task-2"}],
                ),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        self.assertEqual(result["bundle"]["graph_context"]["anchor"]["id"], "task:task-2")

    def test_startup_continuity_read_includes_graph_summary_after_base_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))

            result = continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="thread", subject_id="thread-1", view="startup"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        self.assertIn("startup_summary", result)
        self.assertEqual(result["graph_summary"]["anchor"], {"id": "thread:thread-1", "kind": "thread", "subject_id": "thread-1"})
        self.assertEqual(result["graph_summary"]["truncation"]["nodes"]["limit"], 12)

    def test_non_startup_continuity_read_remains_graph_free(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))
            result = continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="thread", subject_id="thread-1"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        self.assertNotIn("graph_summary", result)

    def test_base_continuity_read_error_has_no_graph_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            with self.assertRaises(HTTPException) as caught:
                continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="thread", subject_id="missing", view="startup"),
                    now=_now(),
                    audit=lambda *_args, **_kwargs: None,
                )

        self.assertEqual(caught.exception.status_code, 404)

    def test_graph_failure_degrades_context_retrieve_and_startup_read(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))
            with patch("app.context.graph.derive_internal_graph_slice1", side_effect=RuntimeError("boom")):
                context_result = context_retrieve_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContextRetrieveRequest(task="continue", subject_kind="thread", subject_id="thread-1"),
                    now=_now(),
                    audit=lambda *_args, **_kwargs: None,
                )
                startup_result = continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="thread", subject_id="thread-1", view="startup"),
                    now=_now(),
                    audit=lambda *_args, **_kwargs: None,
                )

        self.assertTrue(context_result["ok"])
        self.assertEqual(context_result["bundle"]["graph_context"]["warnings"][0]["code"], "graph_derivation_failed")
        self.assertTrue(startup_result["ok"])
        self.assertEqual(startup_result["graph_summary"]["warnings"][0]["code"], "graph_derivation_failed")

    def test_graph_truncation_and_projection_metadata_are_deterministic(self) -> None:
        helper_result = {
            "anchor": {"id": "task:task-1", "family": "task"},
            "nodes": [{"id": f"task:dep-{idx:02d}", "family": "task"} for idx in range(30)],
            "edges": [{"family": "depends_on", "source_id": "task:task-1", "target_id": f"task:dep-{idx:02d}"} for idx in range(30)],
            "warnings": [],
        }

        graph = compact_agent_graph(helper_result, selected_kind="task", selected_subject_id="task-1", caps=CONTEXT_GRAPH_CAPS)

        self.assertEqual(graph["truncation"]["nodes"], {"limit": 24, "available": 30, "returned": 24, "truncated": True})
        self.assertEqual(graph["truncation"]["edges"], {"limit": 32, "available": 24, "returned": 24, "truncated": False})
        self.assertEqual(graph["truncation"]["blockers"], {"limit": 8, "available": 24, "returned": 8, "truncated": True})
        self.assertEqual([warning["code"] for warning in graph["warnings"]], ["graph_truncated", "graph_truncated"])

    def test_auth_path_denial_is_graph_local_and_skips_denied_task_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1"})
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))
            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(denied_paths={"tasks/open/task-1.json"}),
                req=ContextRetrieveRequest(task="continue", subject_kind="thread", subject_id="thread-1"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        graph = result["bundle"]["graph_context"]
        self.assertNotIn({"id": "task:task-1", "kind": "task", "subject_id": "task-1"}, graph["nodes"])
        self.assertEqual(graph["warnings"][0]["code"], "graph_source_denied")
        self.assertEqual(graph["warnings"][0]["details"]["path"], "tasks/open/task-1.json")

    def test_read_files_scope_denial_skips_only_task_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1"})
            _write_json(
                repo_root,
                "memory/continuity/task-task-1.json",
                _capsule(
                    subject_kind="task",
                    subject_id="task-1",
                    related_documents=[{"path": "docs/allowed.md", "kind": "spec", "label": "Allowed"}],
                ),
            )

            with patch("app.context.graph._load_task_candidate") as load_task_candidate:
                graph = derive_agent_graph_context(
                    repo_root=repo_root,
                    auth=_AuthStub(denied_scopes={"read:files"}),
                    subject_kind="task",
                    subject_id="task-1",
                    caps=CONTEXT_GRAPH_CAPS,
                )

        load_task_candidate.assert_not_called()
        self.assertEqual(graph["anchor"], {"id": "task:task-1", "kind": "task", "subject_id": "task-1"})
        self.assertEqual(graph["nodes"], [{"id": "document:docs/allowed.md", "kind": "document", "subject_id": "docs/allowed.md"}])
        self.assertEqual(
            graph["edges"],
            [{"relationship": "references_document", "source_id": "task:task-1", "target_id": "document:docs/allowed.md"}],
        )
        self.assertEqual(
            graph["related_documents"],
            [{"path": "docs/allowed.md", "node_id": "document:docs/allowed.md", "source_id": "task:task-1"}],
        )
        source_denials = [warning for warning in graph["warnings"] if warning["code"] == "graph_source_denied"]
        self.assertEqual(len(source_denials), 1)
        self.assertEqual(
            source_denials[0]["details"],
            {"source_class": "task_artifact", "path": None, "anchor_id": "task:task-1"},
        )

    def test_read_files_scope_denial_empty_when_selected_task_has_no_continuity_representation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1"})

            with patch("app.context.graph._load_task_candidate") as load_task_candidate:
                graph = derive_agent_graph_context(
                    repo_root=repo_root,
                    auth=_AuthStub(denied_scopes={"read:files"}),
                    subject_kind="task",
                    subject_id="task-1",
                    caps=CONTEXT_GRAPH_CAPS,
                )

        load_task_candidate.assert_not_called()
        self.assertIsNone(graph["anchor"])
        self.assertEqual(graph["nodes"], [])
        self.assertEqual(graph["edges"], [])
        self.assertEqual(graph["related_documents"], [])
        self.assertEqual([warning["code"] for warning in graph["warnings"]], ["graph_source_denied"])
        self.assertEqual(
            graph["warnings"][0]["details"],
            {"source_class": "task_artifact", "path": None, "anchor_id": "task:task-1"},
        )

    def test_related_document_denial_omits_node_edge_projection_without_path_leak(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(
                repo_root,
                "memory/continuity/thread-thread-1.json",
                _capsule(
                    subject_kind="thread",
                    subject_id="thread-1",
                    related_documents=[
                        {"path": "docs/public.md", "kind": "spec", "label": "Public"},
                        {"path": "docs/secret.md", "kind": "spec", "label": "Secret"},
                    ],
                ),
            )
            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(denied_paths={"docs/secret.md"}),
                req=ContextRetrieveRequest(task="continue", subject_kind="thread", subject_id="thread-1"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )

        graph = result["bundle"]["graph_context"]
        self.assertEqual(
            graph["related_documents"],
            [{"path": "docs/public.md", "node_id": "document:docs/public.md", "source_id": "thread:thread-1"}],
        )
        self.assertIn({"id": "document:docs/public.md", "kind": "document", "subject_id": "docs/public.md"}, graph["nodes"])
        self.assertNotIn("document:docs/secret.md", json.dumps(graph["nodes"]))
        self.assertNotIn("document:docs/secret.md", json.dumps(graph["edges"]))
        self.assertNotIn("docs/secret.md", json.dumps(graph["related_documents"]))
        self.assertEqual(
            [warning for warning in graph["warnings"] if warning["code"] == "graph_source_denied"],
            [
                {
                    "code": "graph_source_denied",
                    "message": "A graph source was omitted because access was denied.",
                    "details": {
                        "source_class": "related_document",
                        "path": "docs/secret.md",
                        "anchor_id": "thread:thread-1",
                    },
                }
            ],
        )

    def test_selected_anchor_denial_returns_empty_graph_without_malformed_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))
            graph = derive_agent_graph_context(
                repo_root=repo_root,
                auth=_AuthStub(denied_paths={"memory/continuity/thread-thread-1.json"}),
                subject_kind="thread",
                subject_id="thread-1",
                caps=CONTEXT_GRAPH_CAPS,
            )

        self.assertIsNone(graph["anchor"])
        self.assertEqual(graph["nodes"], [])
        self.assertEqual(graph["edges"], [])
        self.assertEqual(graph["related_documents"], [])
        self.assertEqual([warning["code"] for warning in graph["warnings"]], ["graph_source_denied"])
        self.assertEqual(graph["warnings"][0]["details"]["source_class"], "continuity_capsule")
        self.assertNotIn("graph_result_malformed", json.dumps(graph["warnings"]))

    def test_derivation_failures_preserve_public_reason(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "tasks/open").mkdir(parents=True)
            graph = derive_agent_graph_context(
                repo_root=repo_root,
                auth=_AuthStub(),
                subject_kind="thread",
                subject_id="thread-1",
                caps=CONTEXT_GRAPH_CAPS,
            )
        self.assertEqual(graph["warnings"][0]["details"]["reason"], "task_root_missing")

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            (repo_root / "memory/continuity/archive").rmdir()
            graph = derive_agent_graph_context(
                repo_root=repo_root,
                auth=_AuthStub(),
                subject_kind="thread",
                subject_id="thread-1",
                caps=CONTEXT_GRAPH_CAPS,
            )
        self.assertEqual(graph["warnings"][0]["details"]["reason"], "continuity_root_missing")

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            with patch("app.context.graph._enumerate_task_candidates", side_effect=RuntimeError("boom")):
                graph = derive_agent_graph_context(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    subject_kind="thread",
                    subject_id="thread-1",
                    caps=CONTEXT_GRAPH_CAPS,
                )
        self.assertEqual(graph["warnings"][0]["details"]["reason"], "helper_exception")

    def test_archive_fallback_and_cold_candidates_are_denied_before_file_open(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            denied = {
                "memory/continuity/archive/thread-thread-1.json",
                "memory/continuity/fallback/thread-thread-1.json",
                "memory/continuity/cold/index/thread-thread-1.json",
            }
            for rel in denied:
                _write_json(repo_root, rel, {"not": "a valid envelope", "subject_id": "thread-1"})
            graph = derive_agent_graph_context(
                repo_root=repo_root,
                auth=_AuthStub(denied_paths=denied),
                subject_kind="thread",
                subject_id="thread-1",
                caps=CONTEXT_GRAPH_CAPS,
            )

        self.assertIsNone(graph["anchor"])
        self.assertEqual(
            sorted((warning["details"]["source_class"], warning["details"]["path"]) for warning in graph["warnings"]),
            [
                ("archive_artifact", "memory/continuity/archive/thread-thread-1.json"),
                ("cold_index_artifact", "memory/continuity/cold/index/thread-thread-1.json"),
                ("fallback_artifact", "memory/continuity/fallback/thread-thread-1.json"),
            ],
        )

    def test_startup_user_peer_unsupported_and_missing_anchor_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/user-user-1.json", _capsule(subject_kind="user", subject_id="user-1"))
            unsupported = continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="user", subject_id="user-1", view="startup"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )
        self.assertIsNone(unsupported["graph_summary"]["anchor"])
        self.assertEqual(unsupported["graph_summary"]["warnings"][0]["code"], "graph_anchor_not_supported")

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", _capsule(subject_kind="thread", subject_id="thread-1"))
            with patch(
                "app.context.graph.derive_internal_graph_slice1",
                return_value={"anchor": None, "nodes": [], "edges": [], "warnings": ["anchor_not_found"]},
            ):
                missing = continuity_read_service(
                    repo_root=repo_root,
                    auth=_AuthStub(),
                    req=ContinuityReadRequest(subject_kind="thread", subject_id="thread-1", view="startup"),
                    now=_now(),
                    audit=lambda *_args, **_kwargs: None,
                )
        self.assertEqual(missing["graph_summary"]["warnings"][0]["code"], "graph_anchor_not_found")

    def test_exact_warning_message_details_schemas(self) -> None:
        graph = compact_agent_graph(
            {
                "anchor": {"bad": "shape"},
                "nodes": [{"bad": "shape"}],
                "edges": [{"bad": "shape"}],
                "warnings": ["graph_derivation_failed"],
                "derivation_failure_reason": "unknown",
            },
            selected_kind="task",
            selected_subject_id="task-1",
            caps=CONTEXT_GRAPH_CAPS,
        )
        self.assertEqual(
            graph["warnings"],
            [
                {
                    "code": "graph_derivation_failed",
                    "message": "Graph context could not be derived.",
                    "details": {"reason": "unknown"},
                }
            ],
        )

    def test_malformed_helper_anchor_is_counted_without_anchor_not_found(self) -> None:
        graph = compact_agent_graph(
            {"anchor": {"bad": "shape"}, "nodes": [{"id": "task:task-2"}], "edges": [], "warnings": []},
            selected_kind="task",
            selected_subject_id="task-1",
            caps=CONTEXT_GRAPH_CAPS,
        )

        self.assertEqual(graph["anchor"], {"id": "task:task-1", "kind": "task", "subject_id": "task-1"})
        self.assertEqual(graph["warnings"][0]["code"], "graph_result_malformed")
        self.assertEqual(graph["warnings"][0]["details"]["malformed_anchors"], 1)

    def test_graph_data_is_not_written_into_capsules(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_graph_roots(repo_root)
            rel = "memory/continuity/thread-thread-1.json"
            _write_json(repo_root, rel, _capsule(subject_kind="thread", subject_id="thread-1"))
            before = (repo_root / rel).read_text(encoding="utf-8")
            continuity_read_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContinuityReadRequest(subject_kind="thread", subject_id="thread-1", view="startup"),
                now=_now(),
                audit=lambda *_args, **_kwargs: None,
            )
            after = (repo_root / rel).read_text(encoding="utf-8")

        self.assertEqual(after, before)

    def test_discovery_and_help_describe_graph_runtime_sections_without_request_flag(self) -> None:
        tools = {tool["name"]: tool for tool in discovery_tools()["tools"]}
        self.assertIn("bundle.graph_context", tools["context.retrieve"]["description"])
        self.assertIn("graph_summary", tools["continuity.read"]["description"])
        self.assertNotIn("include_graph", json.dumps(tools["context.retrieve"]["input_schema"]))
        self.assertNotIn("include_graph", json.dumps(tools["continuity.read"]["input_schema"]))

        context_help = json.dumps(help_tool_payload("context.retrieve"))
        read_help = json.dumps(help_tool_payload("continuity.read"))
        self.assertIn("bundle.graph_context", context_help)
        self.assertIn("graph_suppressed_by_continuity_mode", context_help)
        self.assertIn("graph_summary.warnings", read_help)
        self.assertIn("non-startup reads are intentionally graph-free", read_help)

    def test_limits_help_lists_graph_response_caps_and_capsule_cap_is_unchanged(self) -> None:
        limits = help_limits_index_payload()
        response_group = next(group for group in limits["groups"] if group["id"] == "response_orientation_caps")
        self.assertIn("context.retrieve.graph_context.nodes", response_group["field_paths"])
        self.assertIn("continuity.read.startup.graph_summary.edges", response_group["field_paths"])
        self.assertEqual(help_limit_payload("context.retrieve.graph_context.nodes")["limit"]["max_items"], 24)
        self.assertEqual(help_limit_payload("continuity.read.startup.graph_summary.blockers")["limit"]["max_items"], 4)
        self.assertEqual(help_limit_payload("continuity.capsule_serialized_utf8")["limit"]["max_length"], 20 * 1024)

    def test_required_docs_cover_graph_contract_and_255_boundary(self) -> None:
        docs = {
            "api": Path("docs/api-surface.md").read_text(encoding="utf-8"),
            "mcp": Path("docs/mcp.md").read_text(encoding="utf-8"),
            "payload": Path("docs/payload-reference.md").read_text(encoding="utf-8"),
            "onboarding": Path("docs/agent-onboarding.md").read_text(encoding="utf-8"),
        }
        for text in docs.values():
            self.assertIn("bundle.graph_context", text)
            self.assertIn("graph_summary", text)
        payload = docs["payload"]
        for code in (
            "graph_anchor_not_provided",
            "graph_anchor_not_supported",
            "graph_anchor_not_found",
            "graph_derivation_failed",
            "graph_truncated",
            "graph_result_malformed",
            "graph_source_denied",
            "graph_suppressed_by_continuity_mode",
        ):
            self.assertIn(code, payload)
        self.assertIn("20 KB", payload)
        self.assertIn("#255", docs["api"])
