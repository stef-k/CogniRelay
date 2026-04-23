"""Internal-only derived graph helper for #219 slice 1."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.continuity.cold import _load_cold_stub
from app.continuity.persistence import (
    _load_archive_envelope_with_warnings,
    _load_capsule_with_warnings,
    _load_fallback_envelope_payload_with_warnings,
)

_TASK_ROOTS = ("tasks/open", "tasks/done")
_CONTINUITY_ROOTS = (
    "memory/continuity",
    "memory/continuity/fallback",
    "memory/continuity/archive",
    "memory/continuity/cold/index",
)
_VALID_SUBJECT_KINDS = {"thread", "task"}


def _empty_graph_result(warning: str) -> dict[str, Any]:
    """Return the exact empty-result shape for one warning."""
    return {
        "anchor": None,
        "nodes": [],
        "edges": [],
        "warnings": [warning],
    }


def _is_whitespace_only(value: str) -> bool:
    """Return whether the string is non-empty and all-whitespace."""
    return bool(value) and value.isspace()


def _is_non_empty_non_whitespace_string(value: Any) -> bool:
    """Return whether the value participates in slice-1 source-string rules."""
    return isinstance(value, str) and value != "" and not _is_whitespace_only(value)


def _node(family: str, subject_id: str) -> dict[str, str]:
    """Build one node object."""
    return {"id": f"{family}:{subject_id}", "family": family}


def _add_node(nodes: dict[str, str], *, family: str, subject_id: str, anchor_id: str) -> None:
    """Add a non-anchor node by canonical ID."""
    node_id = f"{family}:{subject_id}"
    if node_id == anchor_id:
        return
    nodes[node_id] = family


def _add_edge(edges: set[tuple[str, str, str]], *, family: str, source_id: str, target_id: str) -> None:
    """Add one coalesced edge."""
    edges.add((family, source_id, target_id))


def _related_document_paths(capsule: dict[str, Any]) -> list[str]:
    """Return slice-1 participating related document paths from one capsule."""
    continuity = capsule.get("continuity")
    if not isinstance(continuity, dict):
        return []
    related_documents = continuity.get("related_documents")
    if not isinstance(related_documents, list):
        return []
    out: list[str] = []
    for entry in related_documents:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        if isinstance(path, str) and path != "":
            out.append(path)
    return out


def _load_task_candidate(path: Path) -> dict[str, Any] | None:
    """Load one task candidate, skipping unreadable or malformed artifacts."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _enumerate_task_candidates(repo_root: Path) -> list[Path]:
    """Enumerate the exact slice-1 task candidates."""
    candidates: list[Path] = []
    for rel in _TASK_ROOTS:
        root = repo_root / rel
        if root.exists() and not root.is_dir():
            raise OSError(f"Task root is not a directory: {rel}")
        if not root.exists():
            continue
        for path in root.iterdir():
            if path.is_symlink():
                continue
            if not path.is_file():
                continue
            if path.suffix != ".json":
                continue
            candidates.append(path)
    return candidates


def _load_continuity_candidate(repo_root: Path, rel: str) -> dict[str, Any] | None:
    """Load one continuity candidate and skip per-artifact failures."""
    try:
        if rel.startswith("memory/continuity/cold/index/"):
            frontmatter = _load_cold_stub(repo_root, rel)
            return {
                "subject_kind": frontmatter.get("subject_kind"),
                "subject_id": frontmatter.get("subject_id"),
            }
        if rel.startswith("memory/continuity/fallback/"):
            payload, _warnings = _load_fallback_envelope_payload_with_warnings(repo_root, rel)
            capsule = payload.get("capsule")
            return capsule if isinstance(capsule, dict) else None
        if rel.startswith("memory/continuity/archive/"):
            payload, _warnings = _load_archive_envelope_with_warnings(repo_root, rel)
            capsule = payload.get("capsule")
            return capsule if isinstance(capsule, dict) else None
        capsule, _warnings = _load_capsule_with_warnings(repo_root, rel)
        return capsule if isinstance(capsule, dict) else None
    except Exception:
        return None


def _enumerate_continuity_candidates(repo_root: Path) -> list[str]:
    """Enumerate the exact slice-1 continuity candidate paths."""
    candidates: list[str] = []
    for rel in _CONTINUITY_ROOTS:
        root = repo_root / rel
        if root.exists() and not root.is_dir():
            raise OSError(f"Continuity root is not a directory: {rel}")
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            candidates.append(str(path.relative_to(repo_root)).replace("\\", "/"))
    return candidates


def _derive_documents_and_reference_edges(
    *,
    anchor_id: str,
    anchor_family: str,
    capsules: list[dict[str, Any]],
    nodes: dict[str, str],
    edges: set[tuple[str, str, str]],
) -> None:
    """Derive document nodes and references_document edges from anchor capsules."""
    for capsule in capsules:
        try:
            paths = _related_document_paths(capsule)
        except Exception:
            continue
        for path in paths:
            _add_node(nodes, family="document", subject_id=path, anchor_id=anchor_id)
            _add_edge(
                edges,
                family="references_document",
                source_id=f"{anchor_family}:{capsule['subject_id']}",
                target_id=f"document:{path}",
            )


def _derive_supersedes_edges(
    *,
    anchor_id: str,
    anchor_family: str,
    capsules: list[dict[str, Any]],
    nodes: dict[str, str],
    edges: set[tuple[str, str, str]],
) -> None:
    """Derive supersedes neighbor data from anchor capsules."""
    for capsule in capsules:
        descriptor = capsule.get("thread_descriptor")
        if not isinstance(descriptor, dict):
            continue
        superseded_by = descriptor.get("superseded_by")
        if not _is_non_empty_non_whitespace_string(superseded_by):
            continue
        _add_node(nodes, family=anchor_family, subject_id=superseded_by, anchor_id=anchor_id)
        _add_edge(
            edges,
            family="supersedes",
            source_id=f"{anchor_family}:{superseded_by}",
            target_id=f"{anchor_family}:{capsule['subject_id']}",
        )


def derive_internal_graph_slice1(*, repo_root: Path, subject_kind: Any, subject_id: Any) -> dict[str, Any]:
    """Derive the internal-only #219 slice-1 one-hop explicit-link graph."""
    if not isinstance(subject_kind, str) or subject_kind not in _VALID_SUBJECT_KINDS:
        return _empty_graph_result("invalid_subject_kind")
    if not _is_non_empty_non_whitespace_string(subject_id):
        return _empty_graph_result("anchor_not_found")

    try:
        task_paths = _enumerate_task_candidates(repo_root)
        continuity_paths = _enumerate_continuity_candidates(repo_root)
    except Exception:
        return _empty_graph_result("graph_derivation_failed")

    try:
        tasks = [payload for path in task_paths if (payload := _load_task_candidate(path)) is not None]
        continuity_candidates = [
            capsule
            for rel in continuity_paths
            if (capsule := _load_continuity_candidate(repo_root, rel)) is not None
            and capsule.get("subject_kind") == subject_kind
        ]

        anchor_id = f"{subject_kind}:{subject_id}"
        anchor = _node(subject_kind, subject_id)

        if subject_kind == "thread":
            anchor_tasks = [task for task in tasks if task.get("thread_id") == subject_id]
            anchor_capsules = [capsule for capsule in continuity_candidates if capsule.get("subject_id") == subject_id]
        else:
            anchor_tasks = [task for task in tasks if task.get("task_id") == subject_id]
            anchor_capsules = [capsule for capsule in continuity_candidates if capsule.get("subject_id") == subject_id]

        if not anchor_tasks and not anchor_capsules:
            return _empty_graph_result("anchor_not_found")

        nodes: dict[str, str] = {}
        edges: set[tuple[str, str, str]] = set()

        if subject_kind == "thread":
            _derive_documents_and_reference_edges(
                anchor_id=anchor_id,
                anchor_family="thread",
                capsules=anchor_capsules,
                nodes=nodes,
                edges=edges,
            )
            _derive_supersedes_edges(
                anchor_id=anchor_id,
                anchor_family="thread",
                capsules=anchor_capsules,
                nodes=nodes,
                edges=edges,
            )
            for task in anchor_tasks:
                task_id_value = task.get("task_id")
                thread_id_value = task.get("thread_id")
                if not _is_non_empty_non_whitespace_string(task_id_value):
                    continue
                if not _is_non_empty_non_whitespace_string(thread_id_value):
                    continue
                _add_node(nodes, family="task", subject_id=task_id_value, anchor_id=anchor_id)
                _add_edge(
                    edges,
                    family="linked_to_thread",
                    source_id=f"task:{task_id_value}",
                    target_id=f"thread:{thread_id_value}",
                )
        else:
            for task in anchor_tasks:
                task_id_value = task.get("task_id")
                if not _is_non_empty_non_whitespace_string(task_id_value):
                    continue
                thread_id_value = task.get("thread_id")
                if _is_non_empty_non_whitespace_string(thread_id_value):
                    _add_node(nodes, family="thread", subject_id=thread_id_value, anchor_id=anchor_id)
                    _add_edge(
                        edges,
                        family="linked_to_thread",
                        source_id=f"task:{task_id_value}",
                        target_id=f"thread:{thread_id_value}",
                    )
                blocked_by = task.get("blocked_by")
                if not isinstance(blocked_by, list):
                    continue
                for dependency in blocked_by:
                    if not _is_non_empty_non_whitespace_string(dependency):
                        continue
                    _add_node(nodes, family="task", subject_id=dependency, anchor_id=anchor_id)
                    _add_edge(
                        edges,
                        family="depends_on",
                        source_id=f"task:{task_id_value}",
                        target_id=f"task:{dependency}",
                    )
            _derive_documents_and_reference_edges(
                anchor_id=anchor_id,
                anchor_family="task",
                capsules=anchor_capsules,
                nodes=nodes,
                edges=edges,
            )
            _derive_supersedes_edges(
                anchor_id=anchor_id,
                anchor_family="task",
                capsules=anchor_capsules,
                nodes=nodes,
                edges=edges,
            )

        sorted_nodes = [
            {"id": node_id, "family": nodes[node_id]}
            for node_id in sorted(nodes)
        ]
        sorted_edges = [
            {"family": family, "source_id": source_id, "target_id": target_id}
            for family, source_id, target_id in sorted(edges)
        ]
        return {
            "anchor": anchor,
            "nodes": sorted_nodes,
            "edges": sorted_edges,
            "warnings": [],
        }
    except Exception:
        return _empty_graph_result("graph_derivation_failed")
