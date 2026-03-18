"""Task, patch, check, and merge business logic."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import (
    CodeCheckRunRequest,
    CodeMergeRequest,
    PatchApplyRequest,
    PatchProposeRequest,
    TaskCreateRequest,
    TaskUpdateRequest,
)
from app.git_safety import safe_commit_new_file, safe_commit_paths, safe_commit_updated_file, try_commit_file
from app.storage import safe_path, write_text_file

TASKS_OPEN_DIR_REL = "tasks/open"
TASKS_DONE_DIR_REL = "tasks/done"
PATCH_PROPOSALS_DIR_REL = "patches/proposals"
PATCH_APPLIED_DIR_REL = "patches/applied"
RUN_CHECKS_DIR_REL = "runs/checks"

TASK_STATUS_TRANSITIONS = {
    "open": {"open", "in_progress", "blocked", "done"},
    "in_progress": {"in_progress", "open", "blocked", "done"},
    "blocked": {"blocked", "open", "in_progress", "done"},
    "done": {"done"},
}

CHECK_PROFILE_COMMANDS = {
    "lint": ["python3", "-m", "compileall", "-q", "."],
    "test": ["python3", "-m", "unittest", "discover", "-s", "tests", "-v"],
    "build": ["python3", "-m", "compileall", "."],
}


def _resolve_commit_ref(repo_root: Path, ref: str, run_git: Callable[..., subprocess.CompletedProcess[str]]) -> str:
    """Resolve and validate a git commit reference."""
    cp = run_git(repo_root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if cp.returncode != 0:
        raise HTTPException(status_code=400, detail=f"Invalid git ref: {ref}")
    return cp.stdout.strip()


def _task_rel(task_id: str, status: str) -> str:
    """Return the repository-relative path for a task in the given status bucket."""
    base = TASKS_DONE_DIR_REL if status == "done" else TASKS_OPEN_DIR_REL
    return f"{base}/{task_id}.json"


def _find_task(repo_root: Path, task_id: str) -> tuple[str, Path, dict[str, Any]] | tuple[None, None, None]:
    """Find a task file and payload by task id."""
    for rel in (f"{TASKS_OPEN_DIR_REL}/{task_id}.json", f"{TASKS_DONE_DIR_REL}/{task_id}.json"):
        path = safe_path(repo_root, rel)
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return rel, path, payload
    return None, None, None


def _iter_task_files(repo_root: Path) -> list[tuple[str, Path]]:
    """List task files across open and done task directories."""
    out: list[tuple[str, Path]] = []
    for base in (TASKS_OPEN_DIR_REL, TASKS_DONE_DIR_REL):
        directory = safe_path(repo_root, base)
        if not directory.exists() or not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.json")):
            out.append((f"{base}/{path.name}", path))
    return out


def _extract_patch_paths(diff: str) -> set[str]:
    """Extract the repository paths touched by a unified diff payload."""
    paths: set[str] = set()
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            parts = line.split()
            if len(parts) >= 4:
                for raw in (parts[2], parts[3]):
                    if raw == "/dev/null":
                        continue
                    normalized = raw[2:] if raw.startswith("a/") or raw.startswith("b/") else raw
                    if normalized:
                        paths.add(normalized)
        elif line.startswith("--- ") or line.startswith("+++ "):
            raw = line.split(" ", 1)[1].strip()
            if raw == "/dev/null":
                continue
            normalized = raw[2:] if raw.startswith("a/") or raw.startswith("b/") else raw
            if normalized:
                paths.add(normalized)
    return paths


def _run_check_command(
    repo_root: Path,
    ref_resolved: str,
    profile: str,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[int, str, str]:
    """Run a check profile against a detached worktree at the target ref."""
    cmd = CHECK_PROFILE_COMMANDS[profile]
    tmp_dir = tempfile.mkdtemp(prefix="amr-check-")
    try:
        add_cp = run_git(repo_root, "worktree", "add", "--detach", tmp_dir, ref_resolved)
        if add_cp.returncode != 0:
            return (1, "", f"failed to create worktree: {add_cp.stderr.strip()}")
        cp = subprocess.run(cmd, cwd=tmp_dir, text=True, capture_output=True, check=False)
        return (cp.returncode, cp.stdout, cp.stderr)
    finally:
        run_git(repo_root, "worktree", "remove", "--force", tmp_dir)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def load_check_artifacts(repo_root: Path) -> list[dict[str, Any]]:
    """Load stored code-check artifacts from disk."""
    directory = safe_path(repo_root, RUN_CHECKS_DIR_REL)
    if not directory.exists() or not directory.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def tasks_create_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: TaskCreateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create and persist a new task record."""
    auth.require("write:projects")
    existing_rel, _, _ = _find_task(repo_root, req.task_id)
    if existing_rel:
        raise HTTPException(status_code=409, detail=f"Task already exists: {req.task_id}")

    now = datetime.now(timezone.utc).isoformat()
    payload = req.model_dump()
    payload["created_at"] = now
    payload["updated_at"] = now
    payload["task_id"] = req.task_id
    rel = _task_rel(req.task_id, req.status)
    auth.require_write_path(rel)
    path = safe_path(repo_root, rel)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = safe_commit_new_file(
        path=path, gm=gm,
        commit_message=f"tasks: create {req.task_id}",
        error_detail=f"Failed to commit new task {req.task_id}",
    )
    audit(auth, "tasks_create", {"task_id": req.task_id, "status": req.status})
    return {"ok": True, "task": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


def tasks_update_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    task_id: str,
    req: TaskUpdateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Update an existing task and move it if the status bucket changes."""
    auth.require("write:projects")
    rel, path, task = _find_task(repo_root, task_id)
    if not rel or not path or not isinstance(task, dict):
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    current_status = str(task.get("status") or "open")
    new_status = req.status or current_status
    allowed = TASK_STATUS_TRANSITIONS.get(current_status, {current_status})
    if new_status not in allowed:
        raise HTTPException(status_code=409, detail=f"Invalid task status transition: {current_status} -> {new_status}")

    updates = req.model_dump(exclude_unset=True)
    task.update({k: v for k, v in updates.items() if v is not None})
    task["status"] = new_status
    task["updated_at"] = datetime.now(timezone.utc).isoformat()
    task["task_id"] = task_id

    next_rel = _task_rel(task_id, new_status)
    auth.require_write_path(next_rel)
    next_path = safe_path(repo_root, next_rel)
    update_old_bytes = next_path.read_bytes() if next_path.exists() else None
    write_text_file(next_path, json.dumps(task, ensure_ascii=False, indent=2))
    committed_files: list[str] = []

    if path != next_path and path.exists():
        move_old_bytes = path.read_bytes()
        path.unlink()
        if safe_commit_paths(
            rollback_plan=[(next_path, update_old_bytes), (path, move_old_bytes)],
            gm=gm,
            commit_message=f"tasks: update and move {task_id}",
            error_detail=f"Failed to commit task update/move for {task_id}",
        ):
            committed_files.extend([next_rel, rel])
    elif safe_commit_updated_file(
        path=next_path, gm=gm,
        commit_message=f"tasks: update {task_id}",
        error_detail=f"Failed to commit task update for {task_id}",
        old_bytes=update_old_bytes,
    ):
        committed_files.append(next_rel)

    audit(auth, "tasks_update", {"task_id": task_id, "status": new_status})
    return {"ok": True, "task": task, "path": next_rel, "committed_files": committed_files, "latest_commit": gm.latest_commit()}


def tasks_query_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    status: str | None,
    owner_peer: str | None,
    collaborator: str | None,
    thread_id: str | None,
    limit: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Query tasks with optional filters and stable reverse-chronological ordering."""
    auth.require("read:files")
    auth.require_read_path(f"{TASKS_OPEN_DIR_REL}/x.json")
    auth.require_read_path(f"{TASKS_DONE_DIR_REL}/x.json")

    tasks: list[dict[str, Any]] = []
    for rel, path in _iter_task_files(repo_root):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        row.setdefault("task_id", path.stem)
        row.setdefault("status", "done" if rel.startswith(TASKS_DONE_DIR_REL + "/") else "open")
        if status and str(row.get("status")) != status:
            continue
        if owner_peer and str(row.get("owner_peer")) != owner_peer:
            continue
        if collaborator and collaborator not in set(str(x) for x in row.get("collaborators", []) if x):
            continue
        if thread_id and str(row.get("thread_id") or "") != thread_id:
            continue
        tasks.append(row)

    tasks.sort(key=lambda x: (str(x.get("updated_at", "")), str(x.get("task_id", ""))), reverse=True)
    out = tasks[:limit]
    audit(auth, "tasks_query", {"count": len(out)})
    return {"ok": True, "count": len(out), "tasks": out}


def _patch_propose_service(
    *,
    kind: str,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: PatchProposeRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Persist a patch proposal after validating target path and diff scope."""
    auth.require("write:projects")
    auth.require_write_path(req.target_path)
    safe_path(repo_root, req.target_path)
    if req.format != "unified_diff":
        raise HTTPException(status_code=400, detail=f"Unsupported patch format: {req.format}")
    if not req.diff.strip():
        raise HTTPException(status_code=400, detail="Patch diff must not be empty")
    diff_paths = _extract_patch_paths(req.diff)
    if diff_paths and diff_paths != {req.target_path}:
        raise HTTPException(status_code=400, detail=f"Patch must only target {req.target_path}; got {sorted(diff_paths)}")

    base_ref_resolved = _resolve_commit_ref(repo_root, req.base_ref, run_git)
    patch_id = req.patch_id or f"patch_{uuid4().hex[:12]}"
    rel = f"{PATCH_PROPOSALS_DIR_REL}/{patch_id}.json"
    auth.require_write_path(rel)
    path = safe_path(repo_root, rel)
    if path.exists():
        raise HTTPException(status_code=409, detail=f"Patch already exists: {patch_id}")

    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": "1.0",
        "patch_id": patch_id,
        "patch_type": kind,
        "status": "proposed",
        "target_path": req.target_path,
        "base_ref": req.base_ref,
        "base_ref_resolved": base_ref_resolved,
        "format": req.format,
        "diff": req.diff,
        "reason": req.reason,
        "thread_id": req.thread_id,
        "created_at": now,
        "created_by": auth.peer_id,
        "updated_at": now,
    }
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = safe_commit_new_file(
        path=path, gm=gm,
        commit_message=f"patches: propose {patch_id}",
        error_detail=f"Failed to commit patch proposal {patch_id}",
    )
    audit(auth, "patch_propose", {"patch_id": patch_id, "patch_type": kind, "target_path": req.target_path})
    return {"ok": True, "patch": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


def docs_patch_propose_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: PatchProposeRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create a documentation patch proposal."""
    return _patch_propose_service(kind="doc_patch", repo_root=repo_root, gm=gm, auth=auth, req=req, run_git=run_git, audit=audit)


def code_patch_propose_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: PatchProposeRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create a code patch proposal."""
    return _patch_propose_service(kind="code_patch", repo_root=repo_root, gm=gm, auth=auth, req=req, run_git=run_git, audit=audit)


def docs_patch_apply_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: PatchApplyRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    read_commit_file: Callable[[Path, str, str], str | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply a proposed patch or inline diff to a documentation target file."""
    auth.require("write:projects")

    proposal_rel = f"{PATCH_PROPOSALS_DIR_REL}/{req.patch_id}.json"
    auth.require_write_path(proposal_rel)
    proposal_path = safe_path(repo_root, proposal_rel)
    if not proposal_path.exists():
        raise HTTPException(status_code=404, detail=f"Patch not found: {req.patch_id}")
    try:
        proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Invalid patch proposal file: {exc}") from exc
    if not isinstance(proposal, dict):
        raise HTTPException(status_code=500, detail="Invalid patch proposal payload")
    if proposal.get("status") != "proposed":
        raise HTTPException(status_code=409, detail=f"Patch is not in proposed state: {proposal.get('status')}")

    target_path = str(proposal.get("target_path") or "")
    if not target_path:
        raise HTTPException(status_code=500, detail="Patch proposal missing target_path")
    auth.require_write_path(target_path)
    target_abs = safe_path(repo_root, target_path)

    expected_ref = str(proposal.get("base_ref_resolved") or "")
    if expected_ref:
        expected_target = read_commit_file(repo_root, expected_ref, target_path)
        current_target = read_commit_file(repo_root, "HEAD", target_path)
        if expected_target != current_target:
            head = _resolve_commit_ref(repo_root, "HEAD", run_git)
            raise HTTPException(status_code=409, detail=f"Patch base_ref mismatch: expected {expected_ref}, current {head}")

    status_cp = run_git(repo_root, "status", "--porcelain")
    if status_cp.stdout.strip():
        raise HTTPException(status_code=409, detail="Working tree must be clean before applying patch")

    diff_text = str(proposal.get("diff") or "")
    if not diff_text.strip():
        raise HTTPException(status_code=400, detail="Patch diff is empty")

    # Issue 1: capture target bytes BEFORE git apply mutates the file
    target_old_bytes = target_abs.read_bytes() if target_abs.exists() else None

    tmp_fd, tmp_path = tempfile.mkstemp(prefix="amr-patch-", suffix=".diff")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as handle:
            handle.write(diff_text)
        check_cp = run_git(repo_root, "apply", "--check", tmp_path)
        if check_cp.returncode != 0:
            raise HTTPException(status_code=409, detail=f"Patch apply check failed: {check_cp.stderr.strip()}")
        apply_cp = run_git(repo_root, "apply", tmp_path)
        if apply_cp.returncode != 0:
            raise HTTPException(status_code=409, detail=f"Patch apply failed: {apply_cp.stderr.strip()}")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    now = datetime.now(timezone.utc).isoformat()
    proposal["status"] = "applied"
    proposal["applied_at"] = now
    proposal["applied_by"] = auth.peer_id
    proposal["updated_at"] = now
    proposal["applied_commit"] = gm.latest_commit()
    proposal_old_bytes = proposal_path.read_bytes() if proposal_path.exists() else None
    write_text_file(proposal_path, json.dumps(proposal, ensure_ascii=False, indent=2))

    # Commit target + proposal atomically so partial state is impossible
    committed_files: list[str] = []
    commit_msg = req.commit_message or f"patches: apply {req.patch_id}"
    if safe_commit_paths(
        rollback_plan=[(target_abs, target_old_bytes), (proposal_path, proposal_old_bytes)],
        gm=gm,
        commit_message=commit_msg,
        error_detail=f"Failed to commit applied patch {req.patch_id} for {target_path}",
    ):
        committed_files.extend([target_path, proposal_rel])

    # Archive is non-critical — use try_commit_file so partial success doesn't abort
    applied_rel = f"{PATCH_APPLIED_DIR_REL}/{req.patch_id}.json"
    auth.require_write_path(applied_rel)
    applied_path = safe_path(repo_root, applied_rel)
    write_text_file(applied_path, json.dumps(proposal, ensure_ascii=False, indent=2))
    if try_commit_file(
        path=applied_path, gm=gm,
        commit_message=f"patches: archive applied {req.patch_id}",
    ):
        committed_files.append(applied_rel)

    audit(auth, "patch_apply", {"patch_id": req.patch_id, "target_path": target_path})
    return {"ok": True, "patch_id": req.patch_id, "target_path": target_path, "committed_files": committed_files, "latest_commit": gm.latest_commit()}


def code_checks_run_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: CodeCheckRunRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Run a check profile against a repository reference and persist the artifact."""
    auth.require("write:projects")
    ref_resolved = _resolve_commit_ref(repo_root, req.ref, run_git)
    rc, stdout_text, stderr_text = _run_check_command(repo_root, ref_resolved, req.profile, run_git)
    now = datetime.now(timezone.utc)
    run_id = f"run_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    status = "passed" if rc == 0 else "failed"
    payload = {
        "schema_version": "1.0",
        "run_id": run_id,
        "profile": req.profile,
        "ref": req.ref,
        "ref_resolved": ref_resolved,
        "status": status,
        "return_code": rc,
        "started_at": now.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "command": CHECK_PROFILE_COMMANDS[req.profile],
        "stdout": stdout_text[-12000:],
        "stderr": stderr_text[-12000:],
    }
    rel = f"{RUN_CHECKS_DIR_REL}/{run_id}.json"
    auth.require_write_path(rel)
    path = safe_path(repo_root, rel)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = safe_commit_new_file(
        path=path, gm=gm,
        commit_message=f"runs: check {run_id}",
        error_detail=f"Failed to commit check run {run_id}",
    )
    audit(auth, "code_checks_run", {"run_id": run_id, "profile": req.profile, "status": status, "ref": ref_resolved})
    return {"ok": True, "run": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


def code_merge_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: CodeMergeRequest,
    run_git: Callable[..., subprocess.CompletedProcess[str]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Merge a reviewed reference into a target branch after required checks pass."""
    auth.require("write:projects")
    if req.target_ref != "HEAD":
        raise HTTPException(status_code=400, detail="Only target_ref=HEAD is currently supported")

    source_resolved = _resolve_commit_ref(repo_root, req.source_ref, run_git)
    required = [str(profile) for profile in req.required_checks]
    artifacts = load_check_artifacts(repo_root)
    missing: list[str] = []
    for profile in required:
        ok = any(
            isinstance(artifact, dict)
            and str(artifact.get("profile")) == profile
            and str(artifact.get("ref_resolved")) == source_resolved
            and str(artifact.get("status")) == "passed"
            for artifact in artifacts
        )
        if not ok:
            missing.append(profile)
    if missing:
        raise HTTPException(status_code=409, detail=f"Required checks not passed for {source_resolved}: {missing}")

    status_cp = run_git(repo_root, "status", "--porcelain")
    if status_cp.stdout.strip():
        raise HTTPException(status_code=409, detail="Working tree must be clean before merge")

    head_before = _resolve_commit_ref(repo_root, "HEAD", run_git)
    merge_cp = run_git(repo_root, "merge", "--ff-only", source_resolved)
    if merge_cp.returncode != 0:
        raise HTTPException(status_code=409, detail=f"Merge failed: {merge_cp.stderr.strip()}")
    head_after = _resolve_commit_ref(repo_root, "HEAD", run_git)
    merged = head_before != head_after
    audit(auth, "code_merge", {"source_ref": source_resolved, "merged": merged, "required_checks": required})
    return {
        "ok": True,
        "merged": merged,
        "source_ref": source_resolved,
        "target_ref": "HEAD",
        "head_before": head_before,
        "head_after": head_after,
        "required_checks": required,
    }
