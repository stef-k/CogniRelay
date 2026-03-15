from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.continuity import build_continuity_state
from app.indexer import incremental_rebuild_index, list_recent_files, load_files_index, rebuild_index, search_index
from app.models import ContextRetrieveRequest, ContextSnapshotRequest, RecentRequest, SearchRequest
from app.storage import read_text_file, safe_path, write_text_file

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


def _filter_search_results_for_auth(results: list[dict[str, Any]], auth: AuthContext) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in results:
        rel = str(row.get("path", ""))
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        out.append(row)
    return out


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo_root, text=True, capture_output=True, check=False)


def _commit_index_artifacts(repo_root: Path, gm: Any, message_prefix: str) -> list[str]:
    commits: list[str] = []
    for rel in _INDEX_ARTIFACTS:
        path = repo_root / rel
        if path.exists() and gm.commit_file(path, f"{message_prefix} {Path(rel).name}"):
            commits.append(rel)
    return commits


def index_rebuild_service(*, repo_root: Path, gm: Any, auth: AuthContext, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
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


def search_service(*, repo_root: Path, auth: AuthContext, req: SearchRequest, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
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


def context_retrieve_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    auth.require("search")
    core_memory = _load_core_memory(repo_root, auth)
    recent = search_index(
        repo_root,
        req.task,
        req.limit,
        include_types=req.include_types or None,
        time_window_days=req.time_window_days,
    )
    recent = _filter_search_results_for_auth(recent, auth)
    recent = sorted(
        recent,
        key=lambda row: (
            0 if str(row.get("path", "")).startswith("memory/summaries/") else 1,
            0 if str(row.get("path", "")).startswith("messages/") else 1,
            -float(row.get("score", 0) or 0),
        ),
    )[: req.limit]
    open_questions = [row["snippet"] for row in recent[:10] if "?" in row.get("snippet", "")]
    continuity_state = build_continuity_state(repo_root=repo_root, auth=auth, req=req, now=now)
    bundle = {
        "task": req.task,
        "generated_at": now.isoformat(),
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
    audit(auth, "context_retrieve", {"task": req.task[:120], "count": len(recent)})
    return {"ok": True, "bundle": bundle}


def _snippet_text(text: str, limit: int = 280) -> str:
    normalized = " ".join(text.split())
    return normalized[:limit] + ("..." if len(normalized) > limit else "")


def _parse_frontmatter_map(text: str) -> dict[str, str]:
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
    cp = _run_git(repo_root, "show", f"{commit_ref}:{rel_path}")
    if cp.returncode != 0:
        return None
    return cp.stdout


def _resolve_as_of_ref(repo_root: Path, mode: str, value: str | None) -> dict[str, Any]:
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
    auth.require("search")
    auth.require("write:projects")
    as_of = _resolve_as_of_ref(repo_root, req.as_of.mode, req.as_of.value)
    if as_of["mode"] == "working_tree":
        core_memory, items, open_questions = _build_snapshot_from_working_tree(
            repo_root, auth, req.task, req.include_types, req.limit, req.include_core
        )
    else:
        core_memory, items, open_questions = _build_snapshot_from_commit(
            repo_root, auth, req.task, req.include_types, req.limit, req.include_core, str(as_of["value"])
        )

    snapshot_id = f"snap_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    snapshot_rel = f"{SNAPSHOT_DIR_REL}/{snapshot_id}.json"
    auth.require_write_path(snapshot_rel)
    payload = {
        "schema_version": "1.0",
        "snapshot_id": snapshot_id,
        "task": req.task,
        "created_at": now.isoformat(),
        "as_of": as_of,
        "filters": {"include_types": req.include_types, "limit": req.limit, "include_core": req.include_core},
        "core_memory": core_memory,
        "items": items,
        "open_questions": open_questions,
        "provenance": {"source": "context_snapshot_create", "service_version": service_version},
    }
    path = safe_path(repo_root, snapshot_rel)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = gm.commit_file(path, f"snapshot: create {snapshot_id}")
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
