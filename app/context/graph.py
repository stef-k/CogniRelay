"""Derived graph helpers for internal UI and agent runtime surfaces."""

from __future__ import annotations

import json
import posixpath
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.auth import AuthContext
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
CONTEXT_GRAPH_CAPS = {"nodes": 24, "edges": 32, "related_documents": 8, "blockers": 8}
STARTUP_GRAPH_CAPS = {"nodes": 12, "edges": 16, "related_documents": 4, "blockers": 4}

_GRAPH_WARNING_MESSAGES = {
    "graph_anchor_not_provided": "No graph anchor was provided.",
    "graph_anchor_not_supported": "The selected subject kind is not supported as a graph anchor.",
    "graph_anchor_not_found": "The selected graph anchor was not found.",
    "graph_derivation_failed": "Graph context could not be derived.",
    "graph_truncated": "Graph context was truncated to configured caps.",
    "graph_result_malformed": "Malformed graph helper results were skipped.",
    "graph_source_denied": "A graph source was omitted because access was denied.",
    "graph_suppressed_by_continuity_mode": "Graph context was suppressed because continuity mode is off.",
}
_GRAPH_WARNING_ORDER = {code: idx for idx, code in enumerate(_GRAPH_WARNING_MESSAGES)}


class GraphDerivationError(Exception):
    """Internal graph derivation failure with a public-safe reason."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class GraphInternalWarning(str):
    """String-compatible internal warning with optional public-safe metadata."""

    def __new__(cls, value: str, *, reason: str | None = None) -> GraphInternalWarning:
        obj = str.__new__(cls, value)
        obj.reason = reason
        return obj


def _empty_graph_result(warning: str, *, reason: str | None = None) -> dict[str, Any]:
    """Return the exact empty-result shape for one warning."""
    return {
        "anchor": None,
        "nodes": [],
        "edges": [],
        "warnings": [GraphInternalWarning(warning, reason=reason) if reason else warning],
    }


def graph_warning(code: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build one public graph warning object."""
    default_details: dict[str, Any]
    if code == "graph_anchor_not_supported":
        default_details = {"subject_kind": None}
    elif code == "graph_anchor_not_found":
        default_details = {"anchor_id": None, "kind": None, "subject_id": None}
    elif code == "graph_derivation_failed":
        default_details = {"reason": "unknown"}
    elif code == "graph_result_malformed":
        default_details = {"malformed_nodes": 0, "malformed_edges": 0, "malformed_anchors": 0}
    elif code == "graph_source_denied":
        default_details = {"source_class": "unknown", "path": None, "anchor_id": None}
    else:
        default_details = {}
    if details:
        default_details.update(details)
    return {
        "code": code,
        "message": _GRAPH_WARNING_MESSAGES.get(code, "Graph context warning."),
        "details": default_details,
    }


def _warning_sort_key(warning: dict[str, Any]) -> tuple[Any, ...]:
    details = warning.get("details") if isinstance(warning.get("details"), dict) else {}
    return (
        _GRAPH_WARNING_ORDER.get(str(warning.get("code")), 999),
        str(details.get("source_class") or ""),
        str(details.get("path") or ""),
        str(details.get("field") or ""),
        str(details.get("anchor_id") or ""),
    )


def _empty_public_graph(caps: dict[str, int], warnings: list[dict[str, Any]], anchor: dict[str, str] | None = None) -> dict[str, Any]:
    """Return one empty public graph section with cap metadata."""
    return {
        "anchor": anchor,
        "nodes": [],
        "edges": [],
        "related_documents": [],
        "blockers": [],
        "truncation": {
            name: {"limit": limit, "available": 0, "returned": 0, "truncated": False}
            for name, limit in caps.items()
        },
        "warnings": sorted(warnings, key=_warning_sort_key),
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
        if not root.exists():
            raise GraphDerivationError("task_root_missing")
        if root.exists() and not root.is_dir():
            raise GraphDerivationError("task_root_missing")
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
            _load_cold_stub(repo_root, rel)
            return None
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
    seen: set[str] = set()
    for rel in _CONTINUITY_ROOTS:
        root = repo_root / rel
        if not root.exists():
            raise GraphDerivationError("continuity_root_missing")
        if root.exists() and not root.is_dir():
            raise GraphDerivationError("continuity_root_missing")
        for path in root.rglob("*"):
            if path.is_symlink():
                continue
            if not path.is_file():
                continue
            candidate = str(path.relative_to(repo_root)).replace("\\", "/")
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


def _source_class_for_continuity_rel(rel: str) -> str:
    if rel.startswith("memory/continuity/cold/index/"):
        return "cold_index_artifact"
    if rel.startswith("memory/continuity/fallback/"):
        return "fallback_artifact"
    if rel.startswith("memory/continuity/archive/"):
        return "archive_artifact"
    return "continuity_capsule"


def _safe_warning_path(path: str) -> str | None:
    """Return path only when it is safe repo-relative warning metadata."""
    normalized = posixpath.normpath(path) if path else ""
    if not normalized or normalized == "." or normalized.startswith("/") or normalized.startswith(".."):
        return None
    return normalized


def _authorized_task_candidates(
    repo_root: Path,
    auth: AuthContext | None,
    anchor_id: str | None,
    source_denials: list[dict[str, Any]],
) -> list[Path]:
    candidates = _enumerate_task_candidates(repo_root)
    if auth is None:
        return candidates
    allowed: list[Path] = []
    for path in candidates:
        rel = str(path.relative_to(repo_root)).replace("\\", "/")
        try:
            auth.require_read_path(rel)
        except HTTPException:
            source_denials.append(
                graph_warning(
                    "graph_source_denied",
                    {"source_class": "task_artifact", "path": rel, "anchor_id": anchor_id},
                )
            )
            continue
        allowed.append(path)
    return allowed


def _authorized_continuity_candidates(
    repo_root: Path,
    auth: AuthContext | None,
    anchor_id: str | None,
    source_denials: list[dict[str, Any]],
) -> list[str]:
    candidates = _enumerate_continuity_candidates(repo_root)
    if auth is None:
        return candidates
    allowed: list[str] = []
    for rel in candidates:
        try:
            auth.require_read_path(rel)
        except HTTPException:
            source_denials.append(
                graph_warning(
                    "graph_source_denied",
                    {"source_class": _source_class_for_continuity_rel(rel), "path": rel, "anchor_id": anchor_id},
                )
            )
            continue
        allowed.append(rel)
    return allowed


def _derive_documents_and_reference_edges(
    *,
    anchor_id: str,
    anchor_family: str,
    capsules: list[dict[str, Any]],
    auth: AuthContext | None,
    source_denials: list[dict[str, Any]],
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
            if auth is not None:
                try:
                    auth.require_read_path(path)
                except HTTPException:
                    source_denials.append(
                        graph_warning(
                            "graph_source_denied",
                            {"source_class": "related_document", "path": _safe_warning_path(path), "anchor_id": anchor_id},
                        )
                    )
                    continue
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


def derive_internal_graph_slice1(
    *,
    repo_root: Path,
    subject_kind: Any = None,
    subject_id: Any = None,
    auth: AuthContext | None = None,
) -> dict[str, Any]:
    """Derive the internal-only #219 slice-1 one-hop explicit-link graph."""
    if not isinstance(subject_kind, str) or subject_kind not in _VALID_SUBJECT_KINDS:
        return _empty_graph_result("invalid_subject_kind")
    if not _is_non_empty_non_whitespace_string(subject_id):
        return _empty_graph_result("anchor_not_found")

    anchor_id = f"{subject_kind}:{subject_id}"
    source_denials: list[dict[str, Any]] = []
    try:
        task_paths: list[Path] = []
        try:
            if auth is not None:
                auth.require("read:files")
            task_paths = _authorized_task_candidates(repo_root, auth, anchor_id, source_denials)
        except HTTPException:
            source_denials.append(
                graph_warning(
                    "graph_source_denied",
                    {"source_class": "task_artifact", "path": None, "anchor_id": anchor_id},
                )
            )
        continuity_paths = _authorized_continuity_candidates(repo_root, auth, anchor_id, source_denials)
    except GraphDerivationError as exc:
        return _empty_graph_result("graph_derivation_failed", reason=exc.reason)
    except Exception:
        return _empty_graph_result("graph_derivation_failed", reason="helper_exception")

    try:
        tasks = [payload for path in task_paths if (payload := _load_task_candidate(path)) is not None]
        continuity_candidates = [
            capsule
            for rel in continuity_paths
            if (capsule := _load_continuity_candidate(repo_root, rel)) is not None
            and capsule.get("subject_kind") == subject_kind
        ]

        anchor = _node(subject_kind, subject_id)

        if subject_kind == "thread":
            anchor_tasks = [task for task in tasks if task.get("thread_id") == subject_id]
            anchor_capsules = [capsule for capsule in continuity_candidates if capsule.get("subject_id") == subject_id]
        else:
            anchor_tasks = [task for task in tasks if task.get("task_id") == subject_id]
            anchor_capsules = [capsule for capsule in continuity_candidates if capsule.get("subject_id") == subject_id]

        if not anchor_tasks and not anchor_capsules:
            if source_denials:
                return {
                    "anchor": None,
                    "nodes": [],
                    "edges": [],
                    "warnings": [],
                    "source_denials": source_denials,
                }
            return _empty_graph_result("anchor_not_found")

        nodes: dict[str, str] = {}
        edges: set[tuple[str, str, str]] = set()

        if subject_kind == "thread":
            _derive_documents_and_reference_edges(
                anchor_id=anchor_id,
                anchor_family="thread",
                capsules=anchor_capsules,
                auth=auth,
                source_denials=source_denials,
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
                auth=auth,
                source_denials=source_denials,
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
            **({"source_denials": source_denials} if source_denials else {}),
        }
    except GraphDerivationError as exc:
        return _empty_graph_result("graph_derivation_failed", reason=exc.reason)
    except Exception:
        return _empty_graph_result("graph_derivation_failed", reason="helper_exception")


def _public_node_from_id(node_id: Any) -> dict[str, str] | None:
    if not isinstance(node_id, str) or ":" not in node_id:
        return None
    kind, subject_id = node_id.split(":", 1)
    if kind not in {"thread", "task", "document"} or subject_id == "":
        return None
    return {"id": node_id, "kind": kind, "subject_id": subject_id}


def _public_node(node: Any) -> dict[str, str] | None:
    if not isinstance(node, dict):
        return None
    return _public_node_from_id(node.get("id"))


def _public_edge(edge: Any) -> dict[str, str] | None:
    if not isinstance(edge, dict):
        return None
    relationship = edge.get("family")
    source_id = edge.get("source_id")
    target_id = edge.get("target_id")
    if not isinstance(relationship, str) or not relationship:
        return None
    if _public_node_from_id(source_id) is None or _public_node_from_id(target_id) is None:
        return None
    return {"relationship": relationship, "source_id": source_id, "target_id": target_id}


def _map_internal_warning(
    warning: Any,
    *,
    kind: str | None,
    subject_id: str | None,
    derivation_failure_reason: str | None = None,
) -> dict[str, Any] | None:
    if warning == "invalid_subject_kind":
        return graph_warning("graph_anchor_not_supported", {"subject_kind": kind})
    if warning == "anchor_not_found":
        anchor_id = f"{kind}:{subject_id}" if kind and subject_id else None
        return graph_warning("graph_anchor_not_found", {"anchor_id": anchor_id, "kind": kind, "subject_id": subject_id})
    if warning == "graph_derivation_failed":
        warning_reason = getattr(warning, "reason", None)
        return graph_warning("graph_derivation_failed", {"reason": warning_reason or derivation_failure_reason or "unknown"})
    if isinstance(warning, dict) and warning.get("code"):
        return warning
    return None


def compact_agent_graph(
    helper_result: dict[str, Any],
    *,
    selected_kind: str | None,
    selected_subject_id: str | None,
    caps: dict[str, int],
) -> dict[str, Any]:
    """Convert the raw helper graph into the public bounded agent graph shape."""
    public_warnings: list[dict[str, Any]] = []
    derivation_failure_reason = None
    for item in helper_result.get("warnings", []):
        mapped = _map_internal_warning(
            item,
            kind=selected_kind,
            subject_id=selected_subject_id,
            derivation_failure_reason=derivation_failure_reason,
        )
        if mapped is not None:
            public_warnings.append(mapped)
    for item in helper_result.get("source_denials", []):
        if isinstance(item, dict) and item.get("code") == "graph_source_denied":
            public_warnings.append(item)

    if selected_kind not in _VALID_SUBJECT_KINDS or not selected_subject_id:
        return _empty_public_graph(caps, public_warnings)
    if any(warning.get("code") in {"graph_anchor_not_found", "graph_derivation_failed"} for warning in public_warnings):
        return _empty_public_graph(caps, public_warnings)
    if helper_result.get("anchor") is None and any(warning.get("code") == "graph_source_denied" for warning in public_warnings):
        return _empty_public_graph(caps, public_warnings)

    malformed_nodes = 0
    malformed_edges = 0
    malformed_anchors = 0
    if _public_node(helper_result.get("anchor")) is None:
        malformed_anchors += 1
    anchor = {"id": f"{selected_kind}:{selected_subject_id}", "kind": selected_kind, "subject_id": selected_subject_id}
    anchor_id = anchor["id"]

    nodes_by_id: dict[str, dict[str, str]] = {}
    for node in helper_result.get("nodes", []):
        public_node = _public_node(node)
        if public_node is None:
            malformed_nodes += 1
            continue
        if public_node["id"] == anchor_id:
            continue
        nodes_by_id[public_node["id"]] = public_node
    sorted_nodes = [nodes_by_id[node_id] for node_id in sorted(nodes_by_id)]
    node_available = len(sorted_nodes)
    retained_nodes = sorted_nodes[: caps["nodes"]]
    retained_node_ids = {node["id"] for node in retained_nodes}

    edges_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    for edge in helper_result.get("edges", []):
        public_edge = _public_edge(edge)
        if public_edge is None:
            malformed_edges += 1
            continue
        source_id = public_edge["source_id"]
        target_id = public_edge["target_id"]
        if source_id != anchor_id and source_id not in retained_node_ids:
            continue
        if target_id != anchor_id and target_id not in retained_node_ids:
            continue
        key = (public_edge["relationship"], source_id, target_id)
        edges_by_key[key] = public_edge
    sorted_edges = [edges_by_key[key] for key in sorted(edges_by_key)]
    edge_available = len(sorted_edges)
    retained_edges = sorted_edges[: caps["edges"]]

    retained_document_nodes = {node["id"] for node in retained_nodes if node["kind"] == "document"}
    related_documents = [
        {
            "path": edge["target_id"].split(":", 1)[1],
            "node_id": edge["target_id"],
            "source_id": edge["source_id"],
        }
        for edge in retained_edges
        if edge["relationship"] == "references_document" and edge["target_id"] in retained_document_nodes
    ]
    related_documents.sort(key=lambda item: (item["path"], item["source_id"], item["node_id"]))
    related_available = len(related_documents)
    retained_related = related_documents[: caps["related_documents"]]

    blockers = [
        {
            "task_id": edge["source_id"].split(":", 1)[1],
            "blocked_by_task_id": edge["target_id"].split(":", 1)[1],
            "source_id": edge["source_id"],
            "target_id": edge["target_id"],
        }
        for edge in retained_edges
        if edge["relationship"] == "depends_on"
        and edge["source_id"].startswith("task:")
        and edge["target_id"].startswith("task:")
    ]
    blockers.sort(key=lambda item: (item["task_id"], item["blocked_by_task_id"], item["source_id"], item["target_id"]))
    blocker_available = len(blockers)
    retained_blockers = blockers[: caps["blockers"]]

    truncation = {
        "nodes": {"limit": caps["nodes"], "available": node_available, "returned": len(retained_nodes), "truncated": node_available > len(retained_nodes)},
        "edges": {"limit": caps["edges"], "available": edge_available, "returned": len(retained_edges), "truncated": edge_available > len(retained_edges)},
        "related_documents": {
            "limit": caps["related_documents"],
            "available": related_available,
            "returned": len(retained_related),
            "truncated": related_available > len(retained_related),
        },
        "blockers": {"limit": caps["blockers"], "available": blocker_available, "returned": len(retained_blockers), "truncated": blocker_available > len(retained_blockers)},
    }
    if malformed_nodes or malformed_edges or malformed_anchors:
        public_warnings.append(
            graph_warning(
                "graph_result_malformed",
                {
                    "malformed_nodes": malformed_nodes,
                    "malformed_edges": malformed_edges,
                    "malformed_anchors": malformed_anchors,
                },
            )
        )
    for field, meta in truncation.items():
        if meta["truncated"]:
            public_warnings.append(graph_warning("graph_truncated", {"field": field, "limit": meta["limit"], "available": meta["available"]}))

    return {
        "anchor": anchor,
        "nodes": retained_nodes,
        "edges": retained_edges,
        "related_documents": retained_related,
        "blockers": retained_blockers,
        "truncation": truncation,
        "warnings": sorted(public_warnings, key=_warning_sort_key),
    }


def derive_agent_graph_context(
    *,
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    subject_id: str | None,
    caps: dict[str, int],
) -> dict[str, Any]:
    """Derive a public graph section, degrading failures into local warnings."""
    if subject_kind not in _VALID_SUBJECT_KINDS:
        return _empty_public_graph(caps, [graph_warning("graph_anchor_not_supported", {"subject_kind": subject_kind})])
    if not isinstance(subject_id, str) or subject_id == "":
        return _empty_public_graph(caps, [graph_warning("graph_anchor_not_provided")])
    try:
        result = derive_internal_graph_slice1(repo_root=repo_root, subject_kind=subject_kind, subject_id=subject_id, auth=auth)
    except Exception:
        return _empty_public_graph(caps, [graph_warning("graph_derivation_failed", {"reason": "helper_exception"})])
    return compact_agent_graph(result, selected_kind=subject_kind, selected_subject_id=subject_id, caps=caps)


def suppressed_graph_context(caps: dict[str, int]) -> dict[str, Any]:
    """Return the required graph section for continuity_mode='off'."""
    return _empty_public_graph(caps, [graph_warning("graph_suppressed_by_continuity_mode")])


def graph_anchor_not_provided(caps: dict[str, int]) -> dict[str, Any]:
    """Return the required graph section when no graph anchor is present."""
    return _empty_public_graph(caps, [graph_warning("graph_anchor_not_provided")])


def graph_anchor_not_supported(caps: dict[str, int], subject_kind: str | None) -> dict[str, Any]:
    """Return the required graph section for unsupported selected subjects."""
    return _empty_public_graph(caps, [graph_warning("graph_anchor_not_supported", {"subject_kind": subject_kind})])
