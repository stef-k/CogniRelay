"""Context, retrieval, indexing, and snapshot business logic."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from datetime import datetime, timezone
from heapq import nsmallest
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.context.graph import (
    CONTEXT_GRAPH_CAPS,
    derive_agent_graph_context,
    graph_anchor_not_provided,
    graph_anchor_not_supported,
    suppressed_graph_context,
)
from app.timestamps import format_compact, format_iso, parse_iso
from app.continuity import build_continuity_state, continuity_read_service
from app.config import get_settings
from app.indexer import TEXT_SUFFIXES, incremental_rebuild_index, list_recent_files, load_files_index, rebuild_index, search_index
from app.models import AppendRequest, ContextRetrieveRequest, ContextSnapshotRequest, ContinuityReadRequest, RecentRequest, SearchRequest, WriteRequest
from app.git_safety import safe_commit_new_file, safe_commit_updated_file, try_commit_file
from app.storage import StorageError, read_text_file, safe_path, write_text_file
from app.schedule import schedule_context_for_context_retrieve

_logger = logging.getLogger(__name__)

SNAPSHOT_DIR_REL = "snapshots/context"
SNAPSHOT_TEXT_SUFFIXES = {".md", ".json", ".jsonl", ".txt"}
SNAPSHOT_WORD_RE = re.compile(r"[A-Za-z0-9_\-]{2,}")
SNAPSHOT_FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
_INDEX_ARTIFACTS = [
    "index/files_index.json",
    "index/tags_index.json",
    "index/words_index.json",
    "index/types_index.json",
    "index/index_state.json",
    "index/search.db",
]
_CORE_MEMORY_PATHS = (
    "memory/core/identity.md",
    "memory/core/long_term_facts.json",
    "memory/core/values.md",
)
_PRIMARY_INDEX_ARTIFACTS = (
    "index/files_index.json",
    "index/search.db",
    "index/index_state.json",
)
_RAW_SCAN_CANDIDATE_LIMIT = 200


def _context_graph_anchor(req: ContextRetrieveRequest) -> tuple[str | None, str | None, str | None]:
    """Select the #256 graph anchor from primary fields and selectors."""
    saw_non_empty_unsupported: str | None = None
    if req.subject_kind in {"thread", "task"} and req.subject_id:
        return req.subject_kind, req.subject_id, None
    if req.subject_kind in {"user", "peer"} and req.subject_id:
        saw_non_empty_unsupported = req.subject_kind

    for selector in req.continuity_selectors:
        if selector.subject_kind in {"thread", "task"} and selector.subject_id:
            return selector.subject_kind, selector.subject_id, None
        if selector.subject_kind in {"user", "peer"} and selector.subject_id and saw_non_empty_unsupported is None:
            saw_non_empty_unsupported = selector.subject_kind

    if saw_non_empty_unsupported is not None:
        return None, None, saw_non_empty_unsupported
    return None, None, None


def _context_graph_context(repo_root: Path, auth: AuthContext, req: ContextRetrieveRequest) -> dict[str, Any]:
    """Build the graph_context response adjunct without changing base retrieval."""
    if req.continuity_mode == "off":
        return suppressed_graph_context(CONTEXT_GRAPH_CAPS)
    kind, subject_id, unsupported_kind = _context_graph_anchor(req)
    if kind and subject_id:
        return derive_agent_graph_context(
            repo_root=repo_root,
            auth=auth,
            subject_kind=kind,
            subject_id=subject_id,
            caps=CONTEXT_GRAPH_CAPS,
        )
    if unsupported_kind is not None:
        return graph_anchor_not_supported(CONTEXT_GRAPH_CAPS, unsupported_kind)
    return graph_anchor_not_provided(CONTEXT_GRAPH_CAPS)


def _is_continuity_cold_path(rel: str) -> bool:
    """Return whether a repo-relative path belongs to the continuity cold tier."""
    return rel.startswith("memory/continuity/cold/")


def _filter_search_results_for_auth(results: list[dict[str, Any]], auth: AuthContext) -> list[dict[str, Any]]:
    """Drop search results the caller cannot read."""
    out: list[dict[str, Any]] = []
    for row in results:
        rel = str(row.get("path", ""))
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        out.append(row)
    return out


def _exclude_continuity_cold_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop continuity cold-tier artifacts from context retrieval evidence."""
    return [row for row in results if not _is_continuity_cold_path(str(row.get("path", "")))]


def _deduplicate_by_path(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first same-class occurrence of each exact path value."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        path = row.get("path")
        if not isinstance(path, str):
            out.append(row)
            continue
        if path in seen:
            continue
        seen.add(path)
        out.append(row)
    return out


def _assemble_mixed_retrieval_bundle(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Assemble the bounded #213 mixed-retrieval bundle."""
    bundle: dict[str, list[dict[str, Any]]] = {
        "continuity": [],
        "supporting_documents": [],
        "search_hits": [],
    }
    if req.subject_kind not in {"thread", "task"} or not req.subject_id:
        return bundle

    capsule: dict[str, Any] | None = None
    try:
        continuity_result = continuity_read_service(
            repo_root=repo_root,
            auth=auth,
            req=ContinuityReadRequest(
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                allow_fallback=False,
            ),
            now=now,
            audit=lambda *_args, **_kwargs: None,
        )
        loaded_capsule = continuity_result.get("capsule")
        if isinstance(loaded_capsule, dict):
            capsule = loaded_capsule
            bundle["continuity"] = [loaded_capsule]
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    if capsule is not None:
        related_documents = capsule.get("continuity", {}).get("related_documents", [])
        if isinstance(related_documents, list):
            supporting_documents: list[dict[str, Any]] = []
            for entry in related_documents:
                if not isinstance(entry, dict):
                    continue
                path = entry.get("path")
                if not isinstance(path, str):
                    continue
                try:
                    supporting_document = read_file_service(
                        repo_root=repo_root,
                        auth=auth,
                        path=path,
                        audit=lambda *_args, **_kwargs: None,
                    )
                except Exception:
                    continue
                if isinstance(supporting_document, dict):
                    supporting_documents.append(supporting_document)
            bundle["supporting_documents"] = _deduplicate_by_path(supporting_documents)

    try:
        search_result = search_service(
            repo_root=repo_root,
            auth=auth,
            req=SearchRequest(
                query=req.subject_id,
                limit=req.limit,
                sort_by="relevance",
            ),
            audit=lambda *_args, **_kwargs: None,
        )
        results = search_result.get("results", [])
        if isinstance(results, list):
            bundle["search_hits"] = _deduplicate_by_path([row for row in results if isinstance(row, dict)])
    except Exception:
        bundle["search_hits"] = []

    return bundle


def _supports_internal_mixed_retrieval(req: ContextRetrieveRequest) -> bool:
    """Return whether the request should use the bounded #213 mixed retrieval path."""
    return req.subject_kind in {"thread", "task"} and bool(req.subject_id)


def _mixed_retrieval_recent_relevant(bundle: dict[str, list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    """Flatten the internal mixed-retrieval bundle into the external recent_relevant list.

    The external response shape stays unchanged, so the internal three-class
    retrieval is projected into one deterministic evidence list:
    supporting documents first, then search fallback hits. Cross-class
    duplicates are preserved because #213 forbids cross-class deduplication.
    """
    if limit <= 0:
        return []

    recent: list[dict[str, Any]] = []

    for row in bundle.get("supporting_documents", []):
        if not isinstance(row, dict):
            continue
        path = row.get("path")
        if not isinstance(path, str):
            continue
        content = row.get("content")
        snippet = _snippet_text(content) if isinstance(content, str) else ""
        if isinstance(content, str):
            item_type, _ = _record_type_importance(path, content)
        else:
            item_type = str(Path(path).suffix.lstrip(".") or (Path(path).parts[0] if Path(path).parts else "unknown"))
        projected = {
            "path": path,
            "type": item_type,
            "snippet": snippet,
            "score": 0.0,
        }
        warning = row.get("warning")
        if isinstance(warning, str) and warning:
            projected["warning"] = warning
        recent.append(projected)
        if len(recent) >= limit:
            return recent

    for row in bundle.get("search_hits", []):
        if not isinstance(row, dict):
            continue
        recent.append(row)
        if len(recent) >= limit:
            return recent

    return recent


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command inside the repository without raising on failure.

    Delegates to ``app.runtime.service.run_git`` — kept as a private alias
    for call-site compatibility.
    """
    from app.runtime.service import run_git

    return run_git(repo_root, *args)


def _commit_index_artifacts(repo_root: Path, gm: Any, message_prefix: str) -> list[str]:
    """Commit generated index artifacts and return the committed relative paths."""
    commits: list[str] = []
    for rel in _INDEX_ARTIFACTS:
        path = repo_root / rel
        if path.exists() and try_commit_file(
            path=path,
            gm=gm,
            commit_message=f"{message_prefix} {Path(rel).name}",
        ):
            commits.append(rel)
    return commits


def index_rebuild_service(*, repo_root: Path, gm: Any, auth: AuthContext, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Rebuild all derived search indexes and commit changed artifacts."""
    auth.require("read:index")
    payload = rebuild_index(repo_root)
    commits = _commit_index_artifacts(repo_root, gm, "index: update")
    audit(auth, "index_rebuild", {"file_count": payload.get("file_count", 0), "commits": commits})
    return {
        "ok": True,
        "file_count": payload.get("file_count", 0),
        "committed_files": commits,
        "latest_commit": gm.latest_commit(),
    }


def index_rebuild_incremental_service(*, repo_root: Path, gm: Any, auth: AuthContext, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Incrementally rebuild search indexes from repository changes."""
    auth.require("read:index")
    payload = incremental_rebuild_index(repo_root)
    commits = _commit_index_artifacts(repo_root, gm, "index: incremental update")
    audit(
        auth,
        "index_rebuild_incremental",
        {"file_count": payload.get("file_count", 0), "incremental": payload.get("incremental", {}), "commits": commits},
    )
    return {
        "ok": True,
        "file_count": payload.get("file_count", 0),
        "incremental": payload.get("incremental", {}),
        "committed_files": commits,
        "latest_commit": gm.latest_commit(),
    }


def index_status_service(*, repo_root: Path, auth: AuthContext) -> dict[str, Any]:
    """Return the current state of generated index artifacts."""
    auth.require("read:index")
    idx = load_files_index(repo_root)
    sqlite_path = repo_root / "index" / "search.db"
    state_path = repo_root / "index" / "index_state.json"
    return {
        "ok": True,
        "generated_at": idx.get("generated_at"),
        "file_count": idx.get("file_count", 0),
        "sqlite_fts": sqlite_path.exists(),
        "state_file": state_path.exists(),
    }


def write_file_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: WriteRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    scope_for_path: Callable[[str], str],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Write a text file into the repository with auth, limits, and audit handling."""
    enforce_rate_limit(settings, auth, "write")
    enforce_payload_limit(settings, {"path": req.path, "content": req.content}, "write")
    auth.require(scope_for_path(req.path))
    auth.require_write_path(req.path)
    try:
        path = safe_path(repo_root, req.path)
    except StorageError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    write_old_bytes = path.read_bytes() if path.exists() else None
    write_text_file(path, req.content)
    committed = safe_commit_updated_file(
        path=path,
        gm=gm,
        commit_message=req.commit_message or f"write: {req.path}",
        error_detail=f"Failed to commit write for {req.path}",
        old_bytes=write_old_bytes,
    )
    audit(auth, "write", {"path": req.path, "committed": committed})
    return {"ok": True, "path": req.path, "committed": committed, "latest_commit": gm.latest_commit()}


def read_file_service(*, repo_root: Path, auth: AuthContext, path: str, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Read a text file from the repository after scope and path checks."""
    auth.require("read:files")
    auth.require_read_path(path)
    try:
        file_path = safe_path(repo_root, path)
    except StorageError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    audit(auth, "read", {"path": path})
    return {"ok": True, "path": path, "content": read_text_file(file_path)}


def append_record_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: AppendRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    scope_for_path: Callable[[str], str],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Append one JSONL record to a repository file with commit-on-change behavior."""
    enforce_rate_limit(settings, auth, "append")
    enforce_payload_limit(settings, {"path": req.path, "record": req.record}, "append")
    auth.require(scope_for_path(req.path))
    auth.require_write_path(req.path)
    try:
        path = safe_path(repo_root, req.path)
    except StorageError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    append_old_bytes = path.read_bytes() if path.exists() else None
    from app.segment_history.append import SegmentHistoryAppendError, locked_append_jsonl

    try:
        locked_append_jsonl(path, req.record, repo_root=repo_root, gm=gm, settings=settings)
    except SegmentHistoryAppendError as exc:
        raise HTTPException(status_code=503, detail=f"Append failed: {exc.detail}") from exc
    committed = safe_commit_updated_file(
        path=path,
        gm=gm,
        commit_message=req.commit_message or f"append: {req.path}",
        error_detail=f"Failed to commit append for {req.path}",
        old_bytes=append_old_bytes,
    )
    audit(auth, "append", {"path": req.path, "committed": committed})
    return {"ok": True, "path": req.path, "committed": committed, "latest_commit": gm.latest_commit()}


def search_service(*, repo_root: Path, auth: AuthContext, req: SearchRequest, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Run an indexed search and filter results by caller visibility."""
    auth.require("search")
    results = search_index(
        repo_root,
        req.query,
        req.limit,
        include_types=req.include_types or None,
        sort_by=req.sort_by,
        time_window_hours=req.time_window_hours,
    )
    results = _filter_search_results_for_auth(results, auth)
    audit(auth, "search", {"query": req.query, "count": len(results), "sort_by": req.sort_by})
    return {"ok": True, "query": req.query, "sort_by": req.sort_by, "count": len(results), "results": results}


def recent_list_service(*, repo_root: Path, auth: AuthContext, req: RecentRequest, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """List recent repository files that the caller may read."""
    auth.require("search")
    results = list_recent_files(
        repo_root,
        req.limit,
        include_types=req.include_types or None,
        time_window_days=req.time_window_days,
        time_window_hours=req.time_window_hours,
    )
    results = _filter_search_results_for_auth(results, auth)
    audit(auth, "search", {"query": "", "count": len(results), "sort_by": "recent"})
    return {"ok": True, "count": len(results), "results": results}


def _load_core_memory(repo_root: Path, auth: AuthContext) -> list[dict[str, Any]]:
    """Load the fixed set of core memory files for context retrieval."""
    core_memory: list[dict[str, Any]] = []
    for rel in _CORE_MEMORY_PATHS:
        path = repo_root / rel
        if not path.exists() or not path.is_file():
            continue
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        core_memory.append({"path": rel, "snippet": read_text_file(path)[:300]})
    return core_memory


_parse_utc_iso = parse_iso


def _index_health(repo_root: Path, now: datetime) -> str:
    """Classify derived index health for retrieval fallback decisions."""
    for rel in _PRIMARY_INDEX_ARTIFACTS:
        if not (repo_root / rel).exists():
            return "missing"
    try:
        payload = json.loads((repo_root / "index" / "files_index.json").read_text(encoding="utf-8"))
    except Exception:
        return "stale"
    generated_at = _parse_utc_iso(str(payload.get("generated_at") or ""))
    if generated_at is None:
        return "stale"
    if (now - generated_at).total_seconds() > 86400:
        return "stale"
    return "healthy"


def _task_terms(task: str) -> list[str]:
    """Extract lowercased request terms preserving first appearance."""
    out: list[str] = []
    seen: set[str] = set()
    for term in SNAPSHOT_WORD_RE.findall(task):
        lowered = term.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(lowered)
    return out


def _select_top_raw_scan_candidates(
    candidates: list[tuple[float, str, Any]],
    limit: int = _RAW_SCAN_CANDIDATE_LIMIT,
) -> list[tuple[float, str, Any]]:
    """Keep a deterministic bounded recent slice without sorting the full population."""
    if limit <= 0:
        return []
    return nsmallest(limit, candidates)


def _raw_scan_candidate_paths(repo_root: Path) -> list[tuple[Path, float]]:
    """Enumerate deterministic raw-scan candidates using indexer eligibility rules."""
    candidates: list[tuple[float, str, tuple[Path, float]]] = []
    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        try:
            rel_path = path.relative_to(repo_root)
        except ValueError:
            continue
        rel = str(rel_path)
        if rel in _CORE_MEMORY_PATHS:
            continue
        if ".git" in path.parts:
            continue
        if len(rel_path.parts) >= 2 and rel_path.parts[0] == "memory" and rel_path.parts[1] == "continuity":
            continue
        if rel_path.parts and rel_path.parts[0] == "index":
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            stat = path.stat()
        except Exception:
            continue
        candidates.append((-stat.st_mtime, str(rel_path), (path, stat.st_mtime)))
    selected = _select_top_raw_scan_candidates(candidates)
    return [item[2] for item in selected]


def _raw_scan_recent_relevant(
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
) -> list[dict[str, Any]]:
    """Build bounded retrieval evidence directly from repository files."""
    include_set = {item.lower() for item in req.include_types if item}
    task_terms = _task_terms(req.task)
    rows: list[dict[str, Any]] = []
    for path, mtime in _raw_scan_candidate_paths(repo_root):
        rel = str(path.relative_to(repo_root))
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        if _is_continuity_cold_path(rel):
            continue
        try:
            raw = path.read_bytes()[:4096]
        except Exception:  # noqa: BLE001 — graceful degradation
            _logger.warning("Failed to read %s for context scan", path, exc_info=True)
            continue
        text = raw.decode("utf-8", errors="replace")
        if "\ufffd" in text:
            # Only the first 4096 bytes of the file are read; corruption beyond that is not detected here.
            _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
        record_type, importance = _record_type_importance(rel, text)
        if include_set and record_type.lower() not in include_set:
            continue
        low_rel = rel.lower()
        low_text = text.lower()
        path_matches = sum(1 for term in task_terms if term in low_rel)
        snippet_matches = sum(1 for term in task_terms if term in low_text)
        rows.append(
            {
                "path": rel,
                "type": record_type,
                "snippet": _snippet_text(text),
                "importance": importance,
                "modified_at": format_iso(datetime.fromtimestamp(mtime, tz=timezone.utc)),
                "score": (path_matches * 1000) + snippet_matches,
                "_path_matches": path_matches,
                "_snippet_matches": snippet_matches,
                "_mtime": mtime,
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["_path_matches"]),
            -int(row["_snippet_matches"]),
            -float(row["_mtime"]),
            str(row["path"]),
        )
    )
    trimmed = rows[: req.limit]
    for row in trimmed:
        row.pop("_path_matches", None)
        row.pop("_snippet_matches", None)
        row.pop("_mtime", None)
    return trimmed


def _pack_recent_relevant(results: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Sort evidence using the established packing order and score tie-breaks."""

    def _group(path: str) -> int:
        """Bucket paths into summary, message, then everything-else ordering groups."""
        if path.startswith("memory/summaries/"):
            return 0
        if path.startswith("messages/"):
            return 1
        return 2

    packed = sorted(
        results,
        key=lambda row: (
            _group(str(row.get("path", ""))),
            -float(row.get("score", 0) or 0),
            str(row.get("path", "")),
        ),
    )
    return packed[:limit]


def context_retrieve_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Assemble a continuation bundle from core memory, search, and continuity state."""
    auth.require("search")
    core_memory = _load_core_memory(repo_root, auth)
    continuity_state = build_continuity_state(repo_root=repo_root, auth=auth, req=req, now=now)
    if _supports_internal_mixed_retrieval(req):
        mixed_retrieval = _assemble_mixed_retrieval_bundle(
            repo_root=repo_root,
            auth=auth,
            req=req,
            now=now,
        )
        recent = _mixed_retrieval_recent_relevant(mixed_retrieval, req.limit)
    else:
        index_health = _index_health(repo_root, now)
        if index_health == "missing":
            recent = _raw_scan_recent_relevant(repo_root, auth, req)
            continuity_state["recovery_warnings"] = list(continuity_state.get("recovery_warnings", [])) + ["continuity_index_missing"]
        else:
            try:
                recent = search_index(
                    repo_root,
                    req.task,
                    req.limit,
                    include_types=req.include_types or None,
                    time_window_days=req.time_window_days,
                )
                recent = _filter_search_results_for_auth(recent, auth)
                recent = _exclude_continuity_cold_results(recent)
            except Exception:
                recent = _raw_scan_recent_relevant(repo_root, auth, req)
                index_health = "stale"
            if index_health == "stale":
                continuity_state["recovery_warnings"] = list(continuity_state.get("recovery_warnings", [])) + ["continuity_index_stale"]
        recent = _pack_recent_relevant(recent, req.limit)
    open_questions = [row["snippet"] for row in recent[:10] if "?" in row.get("snippet", "")]
    bundle = {
        "task": req.task,
        "generated_at": format_iso(now),
        "core_memory": core_memory,
        "recent_relevant": recent,
        "open_questions": open_questions[:5],
        "token_budget_hint": continuity_state["budget"]["token_budget_hint"],
        "time_window_days": req.time_window_days,
        "notes": [
            "For best continuity, run /v1/index/rebuild before retrieve if many files changed.",
            "Use include_types to reduce noise (e.g. ['journal_entry','messages']).",
            "Use /v1/recent for startup continuity when you need latest entries regardless of keyword relevance.",
        ],
        "continuity_state": continuity_state,
    }
    try:
        bundle["graph_context"] = _context_graph_context(repo_root, auth, req)
    except Exception:
        bundle["graph_context"] = {
            "anchor": None,
            "nodes": [],
            "edges": [],
            "related_documents": [],
            "blockers": [],
            "truncation": {
                name: {"limit": limit, "available": 0, "returned": 0, "truncated": False}
                for name, limit in CONTEXT_GRAPH_CAPS.items()
            },
            "warnings": [
                {
                    "code": "graph_derivation_failed",
                    "message": "Graph context could not be derived.",
                    "details": {"reason": "helper_exception"},
                }
            ],
        }
    if (req.subject_kind and req.subject_id) or req.continuity_selectors:
        settings = get_settings()
        bundle["schedule_context"] = schedule_context_for_context_retrieve(
            repo_root=repo_root,
            auth=auth,
            req=req,
            due_limit=settings.schedule_due_limit,
            upcoming_limit=settings.schedule_upcoming_limit,
            upcoming_window_hours=settings.schedule_upcoming_window_hours,
        )
    continuity_selectors = [
        {
            "subject_kind": item["subject_kind"],
            "subject_id": item["subject_id"],
            "source_state": item.get("source_state", "active"),
        }
        for item in continuity_state.get("capsules", [])
        if isinstance(item, dict) and item.get("subject_kind") and item.get("subject_id")
    ]
    audit(
        auth,
        "context_retrieve",
        {
            "task": req.task[:120],
            "count": len(recent),
            "continuity_selectors": continuity_selectors,
        },
    )
    return {"ok": True, "bundle": bundle}


def _snippet_text(text: str, limit: int = 280) -> str:
    """Collapse text into a one-line snippet with a hard character cap."""
    normalized = " ".join(text.split())
    return normalized[:limit] + ("..." if len(normalized) > limit else "")


def _parse_frontmatter_map(text: str) -> dict[str, str]:
    """Parse a minimal frontmatter block into a flat string map."""
    match = SNAPSHOT_FRONTMATTER_RE.match(text)
    if not match:
        return {}
    out: dict[str, str] = {}
    for line in match.group(1).splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        out[key.strip()] = value.strip()
    return out


def _record_type_importance(rel: str, content: str) -> tuple[str, float | None]:
    """Infer a record type and optional importance value from file content."""
    frontmatter = _parse_frontmatter_map(content) if rel.endswith(".md") else {}
    record_type = str(frontmatter.get("type") or (Path(rel).parts[0] if Path(rel).parts else "unknown"))
    importance = None
    if "importance" in frontmatter:
        try:
            importance = float(str(frontmatter["importance"]).strip())
        except Exception:
            importance = None
    return record_type, importance


def _task_score(task_terms: list[str], rel: str, content: str, importance: float | None) -> float:
    """Score a file for snapshot inclusion using task terms and importance."""
    score = 0.0
    if task_terms:
        low_path = rel.lower()
        low_text = content.lower()
        for term in task_terms:
            if term in low_path:
                score += 2.0
            if term in low_text:
                score += 1.0
    if importance is not None:
        score += float(importance)
    return round(score, 4)


def _read_commit_file(repo_root: Path, commit_ref: str, rel_path: str) -> str | None:
    """Read a file from a historical commit, returning ``None`` if absent."""
    cp = _run_git(repo_root, "show", f"{commit_ref}:{rel_path}")
    if cp.returncode != 0:
        return None
    return cp.stdout


def _resolve_as_of_ref(repo_root: Path, mode: str, value: str | None) -> dict[str, Any]:
    """Resolve a snapshot selector into a working-tree or commit reference."""
    if mode == "working_tree":
        return {"mode": "working_tree", "value": None}
    if mode == "commit":
        if not value:
            raise HTTPException(status_code=400, detail="as_of.value is required for mode=commit")
        cp = _run_git(repo_root, "rev-parse", "--verify", f"{value}^{{commit}}")
        if cp.returncode != 0:
            raise HTTPException(status_code=400, detail=f"Invalid commit ref: {value}")
        return {"mode": "commit", "value": cp.stdout.strip()}
    if mode == "timestamp":
        if not value:
            raise HTTPException(status_code=400, detail="as_of.value is required for mode=timestamp")
        cp = _run_git(repo_root, "rev-list", "-1", f"--before={value}", "HEAD")
        commit_ref = cp.stdout.strip() if cp.returncode == 0 else ""
        if not commit_ref:
            raise HTTPException(status_code=404, detail=f"No commit found before timestamp: {value}")
        return {"mode": "timestamp", "value": commit_ref}
    raise HTTPException(status_code=400, detail=f"Unsupported as_of.mode: {mode}")


def _build_snapshot_from_working_tree(
    repo_root: Path,
    auth: AuthContext,
    task: str,
    include_types: list[str],
    limit: int,
    include_core: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Build snapshot entries from the current working tree."""
    core_memory: list[dict[str, Any]] = []
    if include_core:
        for rel in _CORE_MEMORY_PATHS:
            path = repo_root / rel
            if not path.exists() or not path.is_file():
                continue
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            core_memory.append({"path": rel, "snippet": _snippet_text(read_text_file(path), limit=300)})

    recent = search_index(repo_root, task, limit, include_types=include_types or None)
    recent = _filter_search_results_for_auth(recent, auth)
    recent = sorted(recent, key=lambda row: (-float(row.get("score", 0) or 0), str(row.get("path", ""))))[:limit]
    open_questions = [row.get("snippet", "") for row in recent[:10] if "?" in str(row.get("snippet", ""))][:5]
    return core_memory, recent, open_questions


def _build_snapshot_from_commit(
    repo_root: Path,
    auth: AuthContext,
    task: str,
    include_types: list[str],
    limit: int,
    include_core: bool,
    commit_ref: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Build snapshot entries from a historical commit."""
    include_set = {item.lower() for item in include_types if item}
    task_terms = [term.lower() for term in SNAPSHOT_WORD_RE.findall(task)]
    cp = _run_git(repo_root, "ls-tree", "-r", "--name-only", commit_ref)
    if cp.returncode != 0:
        raise HTTPException(status_code=400, detail=f"Unable to list files at commit: {commit_ref}")
    rel_paths = [line.strip() for line in cp.stdout.splitlines() if line.strip()]

    core_memory: list[dict[str, Any]] = []
    if include_core:
        for rel in _CORE_MEMORY_PATHS:
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            content = _read_commit_file(repo_root, commit_ref, rel)
            if content is None:
                continue
            core_memory.append({"path": rel, "snippet": _snippet_text(content, limit=300)})

    items: list[dict[str, Any]] = []
    for rel in rel_paths:
        if rel.startswith("index/"):
            continue
        if Path(rel).suffix.lower() not in SNAPSHOT_TEXT_SUFFIXES:
            continue
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        content = _read_commit_file(repo_root, commit_ref, rel)
        if content is None:
            continue
        item_type, importance = _record_type_importance(rel, content)
        if include_set and item_type.lower() not in include_set:
            continue
        score = _task_score(task_terms, rel, content, importance)
        items.append(
            {
                "path": rel,
                "type": item_type,
                "snippet": _snippet_text(content),
                "importance": importance,
                "score": score,
                "source_ref": commit_ref,
            }
        )

    items.sort(key=lambda row: (-float(row.get("score", 0)), str(row.get("path", ""))))
    selected = items[:limit]
    open_questions = [item.get("snippet", "") for item in selected[:10] if "?" in str(item.get("snippet", ""))][:5]
    return core_memory, selected, open_questions


def context_snapshot_create_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: ContextSnapshotRequest,
    now: datetime,
    service_version: str,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create and persist a deterministic context snapshot."""
    auth.require("search")
    auth.require("write:projects")
    as_of = _resolve_as_of_ref(repo_root, req.as_of.mode, req.as_of.value)
    if as_of["mode"] == "working_tree":
        core_memory, items, open_questions = _build_snapshot_from_working_tree(repo_root, auth, req.task, req.include_types, req.limit, req.include_core)
    else:
        core_memory, items, open_questions = _build_snapshot_from_commit(repo_root, auth, req.task, req.include_types, req.limit, req.include_core, str(as_of["value"]))

    snapshot_id = f"snap_{format_compact(now)}_{uuid4().hex[:8]}"
    snapshot_rel = f"{SNAPSHOT_DIR_REL}/{snapshot_id}.json"
    auth.require_write_path(snapshot_rel)
    payload = {
        "schema_version": "1.0",
        "snapshot_id": snapshot_id,
        "task": req.task,
        "created_at": format_iso(now),
        "as_of": as_of,
        "filters": {"include_types": req.include_types, "limit": req.limit, "include_core": req.include_core},
        "core_memory": core_memory,
        "items": items,
        "open_questions": open_questions,
        "provenance": {"source": "context_snapshot_create", "service_version": service_version},
    }
    path = safe_path(repo_root, snapshot_rel)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = safe_commit_new_file(
        path=path,
        gm=gm,
        commit_message=f"snapshot: create {snapshot_id}",
        error_detail=f"Failed to commit snapshot {snapshot_id}",
    )
    audit(auth, "context_snapshot_create", {"snapshot_id": snapshot_id, "as_of_mode": as_of["mode"], "items": len(items)})
    return {
        "ok": True,
        "snapshot_id": snapshot_id,
        "path": snapshot_rel,
        "as_of": as_of,
        "item_count": len(items),
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }


def context_snapshot_get_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    snapshot_id: str,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Load a previously persisted context snapshot by id."""
    auth.require("read:files")
    rel = f"{SNAPSHOT_DIR_REL}/{snapshot_id}.json"
    auth.require_read_path(rel)
    path = safe_path(repo_root, rel)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Snapshot not found: {snapshot_id}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to parse snapshot: {exc}") from exc
    audit(auth, "context_snapshot_get", {"snapshot_id": snapshot_id})
    return {"ok": True, "snapshot": payload}
