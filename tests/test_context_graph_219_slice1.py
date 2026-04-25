"""Tests for #219 slice 1 internal graph derivation."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.context.graph import derive_internal_graph_slice1
from app.context.service import context_retrieve_service
from app.models import ContextRetrieveRequest


class _AuthStub:
    """Auth stub that permits reads used by graph and retrieval tests."""

    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    """Git manager stub for retrieval tests."""

    def latest_commit(self) -> str:
        return "test-sha"


def _base_capsule(*, subject_kind: str, subject_id: str) -> dict[str, object]:
    """Return a minimal continuity capsule fixture."""
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": "1.1",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {"producer": "test", "update_reason": "manual", "inputs": []},
        "continuity": {
            "top_priorities": [],
            "active_concerns": [],
            "active_constraints": [],
            "open_loops": [],
            "stance_summary": "",
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.9, "relationship_model": 0.8},
    }


def _fallback_payload(capsule: dict[str, object]) -> dict[str, object]:
    """Wrap a capsule in the fallback envelope shape."""
    return {
        "schema_type": "continuity_fallback_snapshot",
        "schema_version": "1.1",
        "captured_at": "2026-04-23T00:00:00Z",
        "active_path": f"memory/continuity/{capsule['subject_kind']}-{capsule['subject_id']}.json",
        "capsule": capsule,
    }


def _archive_payload(capsule: dict[str, object]) -> dict[str, object]:
    """Wrap a capsule in the archive envelope shape."""
    return {
        "schema_type": "continuity_archive_envelope",
        "schema_version": "1.1",
        "archived_at": "2026-04-23T00:00:00Z",
        "reason": "fixture",
        "capsule": capsule,
    }


def _cold_stub_text(*, subject_kind: str, subject_id: str, archive_rel: str) -> str:
    """Return a minimal valid cold-stub fixture."""
    return "\n".join(
        [
            "---",
            "type: continuity_cold_stub",
            'schema_version: "1.1"',
            "artifact_state: cold",
            f"subject_kind: {subject_kind}",
            f"subject_id: {subject_id}",
            f"source_archive_path: {archive_rel}",
            f"cold_storage_path: memory/continuity/cold/{Path(archive_rel).name}.gz",
            "archived_at: 2026-04-23T00:00:00Z",
            "cold_stored_at: 2026-04-23T00:00:00Z",
            "verification_kind: self_review",
            "verification_status: self_attested",
            "health_status: healthy",
            "freshness_class: durable",
            "phase: fresh",
            "update_reason: fixture",
            "---",
            "## top_priorities",
            "",
            "## active_constraints",
            "",
            "## active_concerns",
            "",
            "## open_loops",
            "",
            "## stance_summary",
            "",
            "## drift_signals",
            "",
            "## session_trajectory",
            "",
            "## trailing_notes",
            "",
            "## curiosity_queue",
            "",
            "## negative_decisions",
            "",
            "## rationale_entries",
            "",
            "",
        ]
    )


def _write_json(repo_root: Path, rel: str, payload: object) -> None:
    """Write one JSON fixture."""
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_text(repo_root: Path, rel: str, text: str) -> None:
    """Write one text fixture."""
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _create_required_graph_roots(repo_root: Path) -> None:
    """Create every required slice-1 discovery root."""
    for rel in (
        "tasks/open",
        "tasks/done",
        "memory/continuity",
        "memory/continuity/fallback",
        "memory/continuity/archive",
        "memory/continuity/cold/index",
    ):
        (repo_root / rel).mkdir(parents=True, exist_ok=True)


class TestContextGraph219Slice1(unittest.TestCase):
    """Validate the exact internal-only graph helper contract for #219 slice 1."""

    def test_invalid_subject_kind_returns_exact_empty_shape_for_full_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            invalid_cases = [
                {},
                {"subject_kind": None},
                {"subject_kind": 123},
                {"subject_kind": ""},
                {"subject_kind": "   "},
                {"subject_kind": "Thread"},
                {"subject_kind": "TASK"},
                {"subject_kind": " task "},
            ]

            for kwargs in invalid_cases:
                with self.subTest(kwargs=kwargs):
                    result = derive_internal_graph_slice1(
                        repo_root=repo_root,
                        subject_id="task-1",
                        **kwargs,
                    )
                    self.assertEqual(
                        result,
                        {
                            "anchor": None,
                            "nodes": [],
                            "edges": [],
                            "warnings": ["invalid_subject_kind"],
                        },
                    )

    def test_invalid_subject_id_returns_anchor_not_found_for_full_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            invalid_cases = [
                {},
                {"subject_id": None},
                {"subject_id": 123},
                {"subject_id": ""},
                {"subject_id": "   "},
            ]

            for kwargs in invalid_cases:
                with self.subTest(kwargs=kwargs):
                    result = derive_internal_graph_slice1(
                        repo_root=repo_root,
                        subject_kind="thread",
                        **kwargs,
                    )
                    self.assertEqual(
                        result,
                        {
                            "anchor": None,
                            "nodes": [],
                            "edges": [],
                            "warnings": ["anchor_not_found"],
                        },
                    )

    def test_invalid_subject_kind_wins_validation_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            invalid_pairs = [
                {},
                {"subject_kind": None},
                {"subject_kind": 123, "subject_id": None},
                {"subject_kind": "", "subject_id": ""},
                {"subject_kind": "   ", "subject_id": "   "},
                {"subject_kind": " task ", "subject_id": "   "},
            ]

            for kwargs in invalid_pairs:
                with self.subTest(kwargs=kwargs):
                    result = derive_internal_graph_slice1(
                        repo_root=repo_root,
                        **kwargs,
                    )
                    self.assertEqual(
                        result,
                        {
                            "anchor": None,
                            "nodes": [],
                            "edges": [],
                            "warnings": ["invalid_subject_kind"],
                        },
                    )

    def test_task_anchor_unions_corroborating_artifacts_and_orders_nodes_and_edges(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            task_open = {
                "task_id": "task-1",
                "thread_id": "thread-z",
                "blocked_by": ["task-b", "task-a", "task-a", "   ", None],
            }
            task_done = {
                "task_id": "task-1",
                "thread_id": "thread-a",
                "blocked_by": ["task-c"],
            }
            _write_json(repo_root, "tasks/open/task-1.json", task_open)
            _write_json(repo_root, "tasks/done/task-1.json", task_done)

            active_capsule = _base_capsule(subject_kind="task", subject_id="task-1")
            active_capsule["continuity"]["related_documents"] = [
                {"path": "docs/zeta.md", "kind": "spec", "label": "Zeta"},
                {"path": "docs/alpha.md", "kind": "spec", "label": "Alpha"},
                {"path": "docs/alpha.md", "kind": "spec", "label": "Alpha duplicate"},
            ]
            active_capsule["thread_descriptor"] = {"label": "task-1", "lifecycle": "superseded", "superseded_by": "task-next"}
            fallback_capsule = _base_capsule(subject_kind="task", subject_id="task-1")
            fallback_capsule["continuity"]["related_documents"] = [
                {"path": "docs/beta.md", "kind": "note", "label": "Beta"},
            ]
            fallback_capsule["thread_descriptor"] = {"label": "task-1", "lifecycle": "superseded", "superseded_by": "task-next-2"}
            _write_json(repo_root, "memory/continuity/task-task-1.json", active_capsule)
            _write_json(repo_root, "memory/continuity/fallback/task-task-1.json", _fallback_payload(fallback_capsule))
            _write_json(repo_root, "memory/continuity/archive/task-task-1-20260423T000000Z.json", _archive_payload(fallback_capsule))
            _write_text(
                repo_root,
                "memory/continuity/cold/index/task-task-1-20260423T000000Z.md",
                _cold_stub_text(
                    subject_kind="task",
                    subject_id="task-1",
                    archive_rel="memory/continuity/archive/task-task-1-20260423T000000Z.json",
                ),
            )

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="task",
                subject_id="task-1",
            )

        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["anchor"], {"id": "task:task-1", "family": "task"})
        self.assertEqual(
            result["nodes"],
            [
                {"id": "document:docs/alpha.md", "family": "document"},
                {"id": "document:docs/beta.md", "family": "document"},
                {"id": "document:docs/zeta.md", "family": "document"},
                {"id": "task:task-a", "family": "task"},
                {"id": "task:task-b", "family": "task"},
                {"id": "task:task-c", "family": "task"},
                {"id": "task:task-next", "family": "task"},
                {"id": "task:task-next-2", "family": "task"},
                {"id": "thread:thread-a", "family": "thread"},
                {"id": "thread:thread-z", "family": "thread"},
            ],
        )
        self.assertEqual(
            result["edges"],
            [
                {"family": "depends_on", "source_id": "task:task-1", "target_id": "task:task-a"},
                {"family": "depends_on", "source_id": "task:task-1", "target_id": "task:task-b"},
                {"family": "depends_on", "source_id": "task:task-1", "target_id": "task:task-c"},
                {"family": "linked_to_thread", "source_id": "task:task-1", "target_id": "thread:thread-a"},
                {"family": "linked_to_thread", "source_id": "task:task-1", "target_id": "thread:thread-z"},
                {"family": "references_document", "source_id": "task:task-1", "target_id": "document:docs/alpha.md"},
                {"family": "references_document", "source_id": "task:task-1", "target_id": "document:docs/beta.md"},
                {"family": "references_document", "source_id": "task:task-1", "target_id": "document:docs/zeta.md"},
                {"family": "supersedes", "source_id": "task:task-next", "target_id": "task:task-1"},
                {"family": "supersedes", "source_id": "task:task-next-2", "target_id": "task:task-1"},
            ],
        )
        self.assertNotIn(result["anchor"], result["nodes"])
        self.assertEqual(set(result["anchor"].keys()), {"id", "family"})
        self.assertTrue(all(set(node.keys()) == {"id", "family"} for node in result["nodes"]))
        self.assertTrue(all(set(edge.keys()) == {"family", "source_id", "target_id"} for edge in result["edges"]))

    def test_thread_anchor_uses_exact_task_globs_and_one_hop_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            capsule = _base_capsule(subject_kind="thread", subject_id="thread-1")
            capsule["continuity"]["related_documents"] = [
                {"path": "docs/thread.md", "kind": "spec", "label": "Thread spec"},
            ]
            capsule["thread_descriptor"] = {"label": "thread-1", "lifecycle": "superseded", "superseded_by": "thread-2"}
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", capsule)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1", "blocked_by": ["task-hidden"]})
            _write_json(repo_root, "tasks/done/task-2.json", {"task_id": "task-2", "thread_id": "thread-1"})
            _write_json(repo_root, "tasks/open/nested/task-3.json", {"task_id": "task-3", "thread_id": "thread-1"})
            _write_json(repo_root, "tasks/open/task-4.txt", {"task_id": "task-4", "thread_id": "thread-1"})
            target = repo_root / "tasks" / "open" / "task-1.json"
            symlink_path = repo_root / "tasks" / "done" / "task-link.json"
            symlink_path.parent.mkdir(parents=True, exist_ok=True)
            symlink_path.symlink_to(target)

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["anchor"], {"id": "thread:thread-1", "family": "thread"})
        self.assertEqual(
            result["nodes"],
            [
                {"id": "document:docs/thread.md", "family": "document"},
                {"id": "task:task-1", "family": "task"},
                {"id": "task:task-2", "family": "task"},
                {"id": "thread:thread-2", "family": "thread"},
            ],
        )
        self.assertEqual(
            result["edges"],
            [
                {"family": "linked_to_thread", "source_id": "task:task-1", "target_id": "thread:thread-1"},
                {"family": "linked_to_thread", "source_id": "task:task-2", "target_id": "thread:thread-1"},
                {"family": "references_document", "source_id": "thread:thread-1", "target_id": "document:docs/thread.md"},
                {"family": "supersedes", "source_id": "thread:thread-2", "target_id": "thread:thread-1"},
            ],
        )

    def test_continuity_symlinked_file_is_ignored_before_capsule_load(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            capsule = _base_capsule(subject_kind="thread", subject_id="thread-1")
            regular_rel = "memory/continuity/thread-thread-1.json"
            symlink_rel = "memory/continuity/thread-link.json"
            _write_json(repo_root, regular_rel, capsule)
            (repo_root / symlink_rel).symlink_to(repo_root / regular_rel)
            regular_payload = json.loads((repo_root / regular_rel).read_text(encoding="utf-8"))
            load_calls: list[str] = []

            def _load_capsule(repo_root_arg: Path, rel: str) -> tuple[dict[str, object], list[str]]:
                self.assertEqual(repo_root_arg, repo_root)
                load_calls.append(rel)
                self.assertNotEqual(rel, symlink_rel)
                return regular_payload, []

            with patch("app.context.graph._load_capsule_with_warnings", side_effect=_load_capsule):
                result = derive_internal_graph_slice1(
                    repo_root=repo_root,
                    subject_kind="thread",
                    subject_id="thread-1",
                )

        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["anchor"], {"id": "thread:thread-1", "family": "thread"})
        self.assertEqual(result["nodes"], [])
        self.assertEqual(result["edges"], [])
        self.assertEqual(load_calls, [regular_rel])

    def test_missing_anchor_returns_exact_empty_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            _write_json(repo_root, "tasks/open/task-1.json", {"task_id": "task-1", "thread_id": "thread-1"})

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-missing",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["anchor_not_found"],
            },
        )

    def test_invalid_candidates_are_silently_skipped_after_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            _write_text(repo_root, "tasks/open/task-1.json", "{not-json")
            _write_text(repo_root, "memory/continuity/weird.txt", "{not-json")

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="task",
                subject_id="task-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["anchor_not_found"],
            },
        )

    def test_continuity_candidate_enumeration_is_recursive_and_not_extension_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            capsule = _base_capsule(subject_kind="thread", subject_id="thread-1")
            _write_text(repo_root, "memory/continuity/deep/anchor.data", json.dumps(capsule))

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["anchor"], {"id": "thread:thread-1", "family": "thread"})
        self.assertEqual(result["nodes"], [])
        self.assertEqual(result["edges"], [])

    def test_related_document_sanitation_exception_is_silent_skip_for_that_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            capsule = _base_capsule(subject_kind="thread", subject_id="thread-1")
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", capsule)

            with patch("app.context.graph._related_document_paths", side_effect=RuntimeError("boom")):
                result = derive_internal_graph_slice1(
                    repo_root=repo_root,
                    subject_kind="thread",
                    subject_id="thread-1",
                )

        self.assertEqual(result["warnings"], [])
        self.assertEqual(result["anchor"], {"id": "thread:thread-1", "family": "thread"})
        self.assertEqual(result["nodes"], [])
        self.assertEqual(result["edges"], [])

    def test_required_source_discovery_failure_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _create_required_graph_roots(Path(td))
            with patch("app.context.graph._enumerate_task_candidates", side_effect=OSError("nope")):
                result = derive_internal_graph_slice1(
                    repo_root=Path(td),
                    subject_kind="thread",
                    subject_id="thread-1",
                )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_cold_stub_only_does_not_produce_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            _write_text(
                repo_root,
                "memory/continuity/cold/index/thread-thread-1-20260423T000000Z.md",
                _cold_stub_text(
                    subject_kind="thread",
                    subject_id="thread-1",
                    archive_rel="memory/continuity/archive/thread-thread-1-20260423T000000Z.json",
                ),
            )

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["anchor_not_found"],
            },
        )

    def test_missing_tasks_open_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "tasks" / "open").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_missing_tasks_done_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "tasks" / "done").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_missing_memory_continuity_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "memory" / "continuity" / "fallback").rmdir()
            (repo_root / "memory" / "continuity" / "archive").rmdir()
            (repo_root / "memory" / "continuity" / "cold" / "index").rmdir()
            (repo_root / "memory" / "continuity" / "cold").rmdir()
            (repo_root / "memory" / "continuity").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_missing_memory_continuity_fallback_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "memory" / "continuity" / "fallback").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_missing_memory_continuity_archive_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "memory" / "continuity" / "archive").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_missing_memory_continuity_cold_index_root_returns_graph_derivation_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _create_required_graph_roots(repo_root)
            (repo_root / "memory" / "continuity" / "cold" / "index").rmdir()

            result = derive_internal_graph_slice1(
                repo_root=repo_root,
                subject_kind="thread",
                subject_id="thread-1",
            )

        self.assertEqual(
            result,
            {
                "anchor": None,
                "nodes": [],
                "edges": [],
                "warnings": ["graph_derivation_failed"],
            },
        )

    def test_context_retrieve_exposes_public_graph_context_without_raw_helper_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            now = datetime.now(timezone.utc)
            capsule = _base_capsule(subject_kind="thread", subject_id="thread-1")
            capsule["continuity"]["related_documents"] = [
                {"path": "docs/thread.md", "kind": "spec", "label": "Thread spec"},
            ]
            _write_json(repo_root, "memory/continuity/thread-thread-1.json", capsule)
            _write_text(repo_root, "docs/thread.md", "thread doc")

            result = context_retrieve_service(
                repo_root=repo_root,
                auth=_AuthStub(),
                req=ContextRetrieveRequest(task="unused", subject_kind="thread", subject_id="thread-1"),
                now=now,
                audit=lambda *_args, **_kwargs: None,
            )

        self.assertIn("bundle", result)
        self.assertIn("graph_context", result["bundle"])
        self.assertNotIn("anchor", result)
        self.assertNotIn("family", json.dumps(result["bundle"]["graph_context"]))
        self.assertIn("continuity_state", result["bundle"])
        self.assertIn("recent_relevant", result["bundle"])
