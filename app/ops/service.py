"""Host-local operations endpoints and scheduled job execution logic."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import OpsRunRequest
from app.storage import append_jsonl, safe_path

_log = logging.getLogger(__name__)

OPS_RUNS_REL = "logs/ops_runs.jsonl"
OPS_LOCKS_DIR_REL = "logs/ops_locks"
OPS_JOBS = {
    "index.rebuild_incremental",
    "metrics.poll_and_alarm_eval",
    "backup.create",
    "backup.restore_test",
    "replication.pull",
    "replication.push",
    "messages.replay_dead_letter_sweep",
    "security.rotation_check",
    "compact.plan",
}


def _is_local_client_ip(client_ip: str | None) -> bool:
    """Return whether the client IP belongs to the local host."""
    if not client_ip:
        return False
    value = str(client_ip).strip().lower()
    if not value:
        return False
    if value.startswith("::ffff:"):
        value = value[7:]
    if value in {"127.0.0.1", "::1", "localhost"}:
        return True
    return value.startswith("127.")


def _require_local_ops_access(auth: AuthContext) -> str:
    """Enforce the local-only policy for host operations endpoints."""
    auth.require("admin:peers")
    ip = getattr(auth, "client_ip", None)
    if not _is_local_client_ip(ip):
        raise HTTPException(status_code=403, detail="Host ops endpoints are local-only")
    return str(ip)


def _ops_runs_path(repo_root: Path) -> Path:
    """Return the repository path for ops run history."""
    return safe_path(repo_root, OPS_RUNS_REL)


def _ops_lock_path(repo_root: Path, job_id: str) -> Path:
    """Return the lockfile path for a job identifier."""
    safe_job = re.sub(r"[^A-Za-z0-9_.-]+", "_", job_id)
    return safe_path(repo_root, f"{OPS_LOCKS_DIR_REL}/{safe_job}.lock")


def _load_ops_runs(repo_root: Path, limit: int = 200) -> list[dict[str, Any]]:
    """Load recent ops run history entries."""
    path = _ops_runs_path(repo_root)
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    all_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    tail = all_lines[-max(1, int(limit)):]
    file_offset = len(all_lines) - len(tail)
    for idx, line in enumerate(tail):
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            _log.warning("malformed JSONL in ops runs (file line %d): %s", file_offset + idx + 1, line[:200])
            continue
        if not isinstance(row, dict):
            _log.debug("non-dict JSON in ops runs (file line %d), skipping", file_offset + idx + 1)
            continue
        out.append(row)
    return out


def _append_ops_run(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Append an ops run record to the run log."""
    path = _ops_runs_path(repo_root)
    append_jsonl(path, payload)
    return path


def _acquire_ops_lock(repo_root: Path, job_id: str, run_id: str, started_at: str) -> Path:
    """Acquire an exclusive lockfile for an ops job run."""
    path = _ops_lock_path(repo_root, job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=f"Ops job already running: {job_id}") from exc
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"job_id": job_id, "run_id": run_id, "started_at": started_at}, ensure_ascii=False))
    return path


def _release_ops_lock(lock_path: Path) -> None:
    """Release an ops job lockfile, logging cleanup failures."""
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        _log.warning("failed to release ops lock %s", lock_path, exc_info=True)


def _list_ops_locks(repo_root: Path) -> list[dict[str, Any]]:
    """Return normalized metadata for active ops lockfiles."""
    directory = safe_path(repo_root, OPS_LOCKS_DIR_REL)
    if not directory.exists() or not directory.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.lock")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            row = {"job_id": path.stem}
        if not isinstance(row, dict):
            row = {"job_id": path.stem}
        row["path"] = f"{OPS_LOCKS_DIR_REL}/{path.name}"
        out.append(row)
    return out


def _ops_job_catalog() -> list[dict[str, Any]]:
    """Return the static catalog of supported host ops jobs."""
    return [
        {
            "job_id": "index.rebuild_incremental",
            "description": "Incremental index refresh for retrieval freshness.",
            "local_only": True,
            "external_factors": ["host_resources"],
            "recommended_schedule": "every 1-5 minutes",
            "idempotent": False,
        },
        {
            "job_id": "metrics.poll_and_alarm_eval",
            "description": "Collect delivery/check/security/replication metrics and alarms.",
            "local_only": True,
            "external_factors": ["workload"],
            "recommended_schedule": "every 1-5 minutes",
            "idempotent": True,
        },
        {
            "job_id": "backup.create",
            "description": "Create deterministic backup archive + manifest.",
            "local_only": True,
            "external_factors": ["disk_capacity"],
            "recommended_schedule": "daily",
            "idempotent": False,
        },
        {
            "job_id": "backup.restore_test",
            "description": "Run restore drill using latest or explicit backup path.",
            "local_only": True,
            "external_factors": ["disk_capacity", "cpu"],
            "recommended_schedule": "daily/weekly",
            "idempotent": True,
        },
        {
            "job_id": "replication.pull",
            "description": "Ingest replication bundle from peer source.",
            "local_only": True,
            "external_factors": ["network", "peer_availability"],
            "recommended_schedule": "hourly or policy-driven",
            "idempotent": True,
        },
        {
            "job_id": "replication.push",
            "description": "Push replication bundle to peer target.",
            "local_only": True,
            "external_factors": ["network", "peer_availability"],
            "recommended_schedule": "hourly or policy-driven",
            "idempotent": True,
        },
        {
            "job_id": "messages.replay_dead_letter_sweep",
            "description": "Replay dead-letter deliveries with bounded sweep.",
            "local_only": True,
            "external_factors": ["peer_availability"],
            "recommended_schedule": "hourly",
            "idempotent": False,
        },
        {
            "job_id": "security.rotation_check",
            "description": "Check upcoming token expiry and active key status.",
            "local_only": True,
            "external_factors": ["time"],
            "recommended_schedule": "daily",
            "idempotent": True,
        },
        {
            "job_id": "compact.plan",
            "description": "Generate compaction planning report (host-controlled).",
            "local_only": True,
            "external_factors": ["token_budget", "context_budget", "host_resources"],
            "recommended_schedule": "daily/weekly",
            "idempotent": False,
        },
    ]


def _latest_backup_archive_rel(repo_root: Path, backups_dir_rel: str) -> str | None:
    """Return the newest backup archive relative path, if any."""
    directory = safe_path(repo_root, backups_dir_rel)
    if not directory.exists() or not directory.is_dir():
        return None
    candidates = sorted(directory.glob("backup_*.tar.gz"), key=lambda row: row.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    return f"{backups_dir_rel}/{candidates[0].name}"


def _ops_rotation_check(
    settings: Any,
    *,
    lookahead_hours: int,
    load_token_config: Callable[[Path], dict[str, Any]],
    parse_iso: Callable[[str | None], datetime | None],
    load_security_keys: Callable[[Path], dict[str, Any]],
) -> dict[str, Any]:
    """Inspect upcoming token expiry and active key status for host ops reporting."""
    now = datetime.now(timezone.utc)
    lookahead = now + timedelta(hours=max(1, int(lookahead_hours)))

    token_cfg = load_token_config(settings.repo_root)
    expiring_tokens: list[dict[str, Any]] = []
    for item in token_cfg.get("tokens", []):
        if not isinstance(item, dict):
            continue
        expires_at = parse_iso(item.get("expires_at"))
        if expires_at is None:
            continue
        if now <= expires_at <= lookahead and str(item.get("status") or "active") == "active":
            expiring_tokens.append(
                {
                    "peer_id": item.get("peer_id"),
                    "token_id": item.get("token_id"),
                    "expires_at": expires_at.isoformat(),
                }
            )

    key_payload = load_security_keys(settings.repo_root)
    active_key_id = key_payload.get("active_key_id")
    keys = key_payload.get("keys", {}) if isinstance(key_payload.get("keys"), dict) else {}
    active_key_status = None
    if isinstance(keys.get(active_key_id), dict):
        active_key_status = keys.get(active_key_id, {}).get("status")

    return {
        "ok": True,
        "checked_at": now.isoformat(),
        "lookahead_hours": int(lookahead_hours),
        "active_key_id": active_key_id,
        "active_key_status": active_key_status,
        "expiring_tokens": expiring_tokens,
        "expiring_token_count": len(expiring_tokens),
    }


def _ops_replay_dead_letter_sweep(
    settings: Any,
    auth: AuthContext,
    arguments: dict[str, Any],
    *,
    load_delivery_state: Callable[[Path], dict[str, Any]],
    effective_delivery_status: Callable[[dict[str, Any], datetime], str],
    replay_messages: Callable[..., dict[str, Any]],
    replay_request_factory: Callable[..., Any],
) -> dict[str, Any]:
    """Replay a bounded set of dead-letter deliveries as a scheduled ops job."""
    limit = int(arguments.get("limit", 20))
    force = bool(arguments.get("force", False))
    reason = str(arguments.get("reason") or "ops_dead_letter_sweep")

    state = load_delivery_state(settings.repo_root)
    now = datetime.now(timezone.utc)
    dead_ids: list[str] = []
    for message_id, row in state.get("records", {}).items():
        if not isinstance(row, dict):
            continue
        if effective_delivery_status(row, now) == "dead_letter":
            dead_ids.append(str(message_id))
    dead_ids = sorted(dead_ids)[: max(1, limit)]

    replayed: list[str] = []
    errors: list[dict[str, Any]] = []
    for message_id in dead_ids:
        try:
            replay_messages(req=replay_request_factory(message_id=message_id, reason=reason, force=force), auth=auth)
            replayed.append(message_id)
        except HTTPException as exc:
            errors.append({"message_id": message_id, "status_code": exc.status_code, "detail": str(exc.detail)})

    result: dict[str, Any] = {
        "ok": True,
        "dead_letter_candidates": len(dead_ids),
        "replayed_count": len(replayed),
        "replayed": replayed,
        "errors": errors,
        "force": force,
    }
    if state.get("warnings"):
        result["warnings"] = state["warnings"]
    return result


def _ops_execute_job(
    req: OpsRunRequest,
    auth: AuthContext,
    *,
    settings: Any,
    backups_dir_rel: str,
    index_rebuild_incremental: Callable[..., dict[str, Any]],
    metrics: Callable[..., dict[str, Any]],
    backup_create: Callable[..., dict[str, Any]],
    backup_create_request_factory: Callable[..., Any],
    backup_restore_test: Callable[..., dict[str, Any]],
    backup_restore_test_request_factory: Callable[..., Any],
    replication_pull: Callable[..., dict[str, Any]],
    replication_pull_request_factory: Callable[..., Any],
    replication_push: Callable[..., dict[str, Any]],
    replication_push_request_factory: Callable[..., Any],
    compact_run: Callable[..., dict[str, Any]],
    compact_request_factory: Callable[..., Any],
    load_token_config: Callable[[Path], dict[str, Any]],
    parse_iso: Callable[[str | None], datetime | None],
    load_security_keys: Callable[[Path], dict[str, Any]],
    load_delivery_state: Callable[[Path], dict[str, Any]],
    effective_delivery_status: Callable[[dict[str, Any], datetime], str],
    replay_messages: Callable[..., dict[str, Any]],
    replay_request_factory: Callable[..., Any],
) -> dict[str, Any]:
    """Dispatch a single host ops job and return its result payload."""
    args = dict(req.arguments or {})

    if req.dry_run:
        return {"ok": True, "dry_run": True, "job_id": req.job_id, "planned_arguments": args}

    if req.job_id == "index.rebuild_incremental":
        return index_rebuild_incremental(auth=auth)
    if req.job_id == "metrics.poll_and_alarm_eval":
        return metrics(auth=auth)
    if req.job_id == "backup.create":
        return backup_create(req=backup_create_request_factory(**args), auth=auth)
    if req.job_id == "backup.restore_test":
        req_args = dict(args)
        if not req_args.get("backup_path"):
            latest_rel = _latest_backup_archive_rel(settings.repo_root, backups_dir_rel)
            if not latest_rel:
                raise HTTPException(status_code=404, detail="No backup archive found for restore test")
            req_args["backup_path"] = latest_rel
        return backup_restore_test(req=backup_restore_test_request_factory(**req_args), auth=auth)
    if req.job_id == "replication.pull":
        return replication_pull(req=replication_pull_request_factory(**args), auth=auth)
    if req.job_id == "replication.push":
        return replication_push(req=replication_push_request_factory(**args), auth=auth)
    if req.job_id == "messages.replay_dead_letter_sweep":
        return _ops_replay_dead_letter_sweep(
            settings,
            auth,
            args,
            load_delivery_state=load_delivery_state,
            effective_delivery_status=effective_delivery_status,
            replay_messages=replay_messages,
            replay_request_factory=replay_request_factory,
        )
    if req.job_id == "security.rotation_check":
        return _ops_rotation_check(
            settings,
            lookahead_hours=int(args.get("lookahead_hours", 24)),
            load_token_config=load_token_config,
            parse_iso=parse_iso,
            load_security_keys=load_security_keys,
        )
    if req.job_id == "compact.plan":
        return compact_run(req=compact_request_factory(**args), auth=auth)
    raise HTTPException(status_code=400, detail=f"Unsupported ops job: {req.job_id}")


def ops_catalog_service(*, settings: Any, auth: AuthContext, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Return the supported host operations catalog."""
    ip = _require_local_ops_access(auth)
    jobs = _ops_job_catalog()
    audit(auth, "ops_catalog", {"count": len(jobs), "client_ip": ip})
    return {
        "ok": True,
        "local_only": True,
        "security": {
            "execution": "host-local",
            "required_scope": "admin:peers",
            "network_boundary": "loopback_or_unix_socket",
            "description": "Ops endpoints are for hosting-agent daemon/scheduler use only.",
        },
        "jobs": jobs,
    }


def ops_status_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    limit: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Return the current ops lock and recent run status summary."""
    ip = _require_local_ops_access(auth)
    runs = _load_ops_runs(repo_root, limit=limit)
    locks = _list_ops_locks(repo_root)

    by_job: dict[str, dict[str, Any]] = {}
    for row in runs:
        job_id = str(row.get("job_id") or "")
        if not job_id or job_id in by_job:
            continue
        by_job[job_id] = {
            "status": row.get("status"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "run_id": row.get("run_id"),
        }

    audit(auth, "ops_status", {"limit": limit, "client_ip": ip, "runs": len(runs), "locks": len(locks)})
    return {
        "ok": True,
        "local_only": True,
        "client_ip": ip,
        "active_locks": locks,
        "recent_runs": runs,
        "last_by_job": by_job,
    }


def ops_schedule_export_service(*, settings: Any, auth: AuthContext, format: str, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Render example cron or systemd schedules for the supported ops jobs."""
    ip = _require_local_ops_access(auth)
    fmt = str(format or "systemd").strip().lower()
    if fmt not in {"systemd", "cron"}:
        raise HTTPException(status_code=400, detail="format must be one of: systemd, cron")

    jobs = _ops_job_catalog()
    base_url = "http://127.0.0.1:8080"
    command = f"curl -sS -X POST {base_url}/v1/ops/run -H 'Authorization: Bearer $COGNIRELAY_OPS_TOKEN' -H 'Content-Type: application/json'"
    if fmt == "systemd":
        examples = {
            "service_unit": {
                "Description": "CogniRelay host ops runner",
                "ExecStart": f"/bin/sh -lc \"{command} -d '{{\"job_id\":\"metrics.poll_and_alarm_eval\"}}'\"",
            },
            "timer_unit": {"OnCalendar": "*:0/5", "Persistent": True},
        }
    else:
        examples = {
            "cron_examples": [
                f"*/5 * * * * {command} -d '{{\"job_id\":\"index.rebuild_incremental\"}}'",
                f"0 * * * * {command} -d '{{\"job_id\":\"metrics.poll_and_alarm_eval\"}}'",
                f"0 2 * * * {command} -d '{{\"job_id\":\"backup.create\"}}'",
                f"30 2 * * 0 {command} -d '{{\"job_id\":\"backup.restore_test\"}}'",
            ]
        }

    audit(auth, "ops_schedule_export", {"format": fmt, "client_ip": ip})
    return {"ok": True, "local_only": True, "format": fmt, "jobs": jobs, "examples": examples}


def ops_run_service(
    *,
    settings: Any,
    auth: AuthContext,
    req: OpsRunRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
    index_rebuild_incremental: Callable[..., dict[str, Any]],
    metrics: Callable[..., dict[str, Any]],
    backup_create: Callable[..., dict[str, Any]],
    backup_create_request_factory: Callable[..., Any],
    backup_restore_test: Callable[..., dict[str, Any]],
    backup_restore_test_request_factory: Callable[..., Any],
    replication_pull: Callable[..., dict[str, Any]],
    replication_pull_request_factory: Callable[..., Any],
    replication_push: Callable[..., dict[str, Any]],
    replication_push_request_factory: Callable[..., Any],
    compact_run: Callable[..., dict[str, Any]],
    compact_request_factory: Callable[..., Any],
    load_token_config: Callable[[Path], dict[str, Any]],
    parse_iso: Callable[[str | None], datetime | None],
    load_security_keys: Callable[[Path], dict[str, Any]],
    load_delivery_state: Callable[[Path], dict[str, Any]],
    effective_delivery_status: Callable[[dict[str, Any], datetime], str],
    replay_messages: Callable[..., dict[str, Any]],
    replay_request_factory: Callable[..., Any],
    backups_dir_rel: str,
) -> dict[str, Any]:
    """Execute a single local-only ops job with locking and audit recording."""
    enforce_rate_limit(settings, auth, "ops_run")
    enforce_payload_limit(settings, req.model_dump(), "ops_run")
    ip = _require_local_ops_access(auth)
    if req.job_id not in OPS_JOBS:
        raise HTTPException(status_code=400, detail=f"Unsupported ops job: {req.job_id}")

    started = datetime.now(timezone.utc)
    run_id = f"ops_{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    lock_path = _acquire_ops_lock(settings.repo_root, req.job_id, run_id, started.isoformat())

    status = "succeeded"
    job_result: dict[str, Any] | None = None
    error_detail: str | None = None
    try:
        job_result = _ops_execute_job(
            req,
            auth,
            settings=settings,
            backups_dir_rel=backups_dir_rel,
            index_rebuild_incremental=index_rebuild_incremental,
            metrics=metrics,
            backup_create=backup_create,
            backup_create_request_factory=backup_create_request_factory,
            backup_restore_test=backup_restore_test,
            backup_restore_test_request_factory=backup_restore_test_request_factory,
            replication_pull=replication_pull,
            replication_pull_request_factory=replication_pull_request_factory,
            replication_push=replication_push,
            replication_push_request_factory=replication_push_request_factory,
            compact_run=compact_run,
            compact_request_factory=compact_request_factory,
            load_token_config=load_token_config,
            parse_iso=parse_iso,
            load_security_keys=load_security_keys,
            load_delivery_state=load_delivery_state,
            effective_delivery_status=effective_delivery_status,
            replay_messages=replay_messages,
            replay_request_factory=replay_request_factory,
        )
    except HTTPException as exc:
        status = "failed"
        error_detail = f"HTTP {exc.status_code}: {exc.detail}"
        raise
    except Exception as exc:
        status = "failed"
        error_detail = str(exc)
        raise
    finally:
        finished = datetime.now(timezone.utc)
        run_row = {
            "schema_version": "1.0",
            "run_id": run_id,
            "job_id": req.job_id,
            "status": status,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "duration_seconds": round((finished - started).total_seconds(), 4),
            "dry_run": req.dry_run,
            "force": req.force,
            "client_ip": ip,
            "initiator": auth.peer_id,
            "error": error_detail,
            "result_summary": {
                "ok": bool(job_result.get("ok")) if isinstance(job_result, dict) else None,
                "keys": sorted(list(job_result.keys()))[:20] if isinstance(job_result, dict) else [],
            },
        }
        _append_ops_run(settings.repo_root, run_row)
        _release_ops_lock(lock_path)
        audit(auth, "ops_run", {"run_id": run_id, "job_id": req.job_id, "status": status, "client_ip": ip})

    return {"ok": True, "run_id": run_id, "job_id": req.job_id, "status": status, "local_only": True, "job_result": job_result}
