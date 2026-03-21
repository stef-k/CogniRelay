"""Replication, backup, metrics, and compaction business logic."""

from __future__ import annotations

import hashlib
import gzip
import json
import logging
import math
import re
import tarfile
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin
from urllib.request import Request as UrlRequest, urlopen
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.artifact_lifecycle.service import (
    _ARTIFACT_HISTORY_SCHEMA_TYPES_BY_FAMILY,
    _family_artifact_id_key,
    _family_source_rel,
    _family_summary,
    artifact_history_payload_rel_path_from_cold_artifact,
)
from app.config import DEFAULT_MAX_JSONL_READ_BYTES
from app.continuity.service import (
    CONTINUITY_ARCHIVE_SCHEMA_TYPE,
    CONTINUITY_ARCHIVE_SCHEMA_VERSION,
    CONTINUITY_COLD_DIR_REL,
    CONTINUITY_COLD_INDEX_DIR_REL,
    CONTINUITY_DIR_REL,
    CONTINUITY_FALLBACK_SCHEMA_TYPE,
    CONTINUITY_FALLBACK_SCHEMA_VERSION,
    _archive_rel_path_from_envelope,
    continuity_archive_rel_path_from_cold_artifact,
    continuity_cold_stub_rel_path,
    _load_cold_stub,
    continuity_fallback_rel_path,
    continuity_rel_path,
)
from app.git_safety import safe_commit_paths, try_commit_file, try_commit_paths
from app.models import BackupCreateRequest, BackupRestoreTestRequest, CompactRequest, ContinuityCapsule, ReplicationPullRequest, ReplicationPushRequest
from app.registry_lifecycle.service import externalize_superseded_pull, externalize_superseded_push
from app.storage import canonical_json, read_text_file, safe_path, write_bytes_file, write_text_file

_logger = logging.getLogger(__name__)

REPLICATION_STATE_REL = "peers/replication_state.json"
REPLICATION_ALLOWED_PREFIXES = {"journal", "essays", "projects", "memory", "messages", "tasks", "patches", "runs", "snapshots", "archive"}
REPLICATION_TOMBSTONES_REL = "peers/replication_tombstones.json"
BACKUPS_DIR_REL = "backups"

_ARTIFACT_HISTORY_SPECS: tuple[dict[str, str], ...] = (
    {
        "family": "handoff",
        "history_dir_rel": "memory/coordination/handoffs/history",
        "payload_schema_type": "handoff_history_unit",
    },
    {
        "family": "shared_history",
        "history_dir_rel": "memory/coordination/shared/history",
        "payload_schema_type": "shared_history_unit",
    },
    {
        "family": "reconciliation",
        "history_dir_rel": "memory/coordination/reconciliations/history",
        "payload_schema_type": "reconciliation_history_unit",
    },
    {
        "family": "task_done",
        "history_dir_rel": "tasks/history/done",
        "payload_schema_type": "task_done_history_unit",
    },
    {
        "family": "patch_applied",
        "history_dir_rel": "patches/history/applied",
        "payload_schema_type": "patch_applied_history_unit",
    },
)


def _capture_path_state(path: Path, rollback_plan: list[tuple[Path, bytes | None]], seen: set[Path]) -> None:
    """Record a path's prior bytes once for fail-hard rollback flows."""
    resolved = path.resolve()
    if resolved in seen:
        return
    seen.add(resolved)
    rollback_plan.append((path, path.read_bytes() if path.exists() else None))


def _restore_rollback_plan(rollback_plan: list[tuple[Path, bytes | None]]) -> None:
    """Best-effort restore of files mutated before a local write failure."""
    for path, old_bytes in rollback_plan:
        try:
            if old_bytes is None:
                path.unlink(missing_ok=True)
            else:
                write_bytes_file(path, old_bytes)
        except Exception:  # noqa: BLE001 - preserve the original failure
            _logger.exception("Rollback restore failed for %s", path)


def _continuity_included(include_prefixes: list[str]) -> bool:
    """Return whether the requested backup prefixes cover continuity artifacts."""
    return any(
        prefix == "memory"
        or prefix == CONTINUITY_DIR_REL
        or prefix.startswith(f"{CONTINUITY_DIR_REL}/")
        for prefix in include_prefixes
    )


def _continuity_counts(repo_root: Path) -> dict[str, int]:
    """Count continuity artifact classes in the repository."""
    active_dir = safe_path(repo_root, CONTINUITY_DIR_REL)
    fallback_dir = safe_path(repo_root, f"{CONTINUITY_DIR_REL}/fallback")
    archive_dir = safe_path(repo_root, f"{CONTINUITY_DIR_REL}/archive")
    cold_dir = safe_path(repo_root, CONTINUITY_COLD_DIR_REL)
    cold_index_dir = safe_path(repo_root, CONTINUITY_COLD_INDEX_DIR_REL)

    def _count_json(directory: Path, *, top_level_only: bool = False) -> int:
        """Count JSON artifacts in one directory, optionally without descending."""
        if not directory.exists() or not directory.is_dir():
            return 0
        iterator = directory.glob("*.json") if top_level_only else directory.rglob("*.json")
        return sum(
            1
            for path in iterator
            if path.is_file()
            and path.name != "refresh_state.json"
        )

    return {
        "active_capsules": _count_json(active_dir, top_level_only=True),
        "fallback_snapshots": _count_json(fallback_dir),
        "archive_envelopes": _count_json(archive_dir),
        "cold_payloads": sum(1 for path in cold_dir.glob("*.json.gz") if path.is_file()) if cold_dir.exists() and cold_dir.is_dir() else 0,
        "cold_stubs": sum(1 for path in cold_index_dir.glob("*.md") if path.is_file()) if cold_index_dir.exists() and cold_index_dir.is_dir() else 0,
    }


def _validate_active_continuity_payload(path: Path, restore_root: Path) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored active continuity capsule."""
    rel = str(path.relative_to(restore_root))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capsule = ContinuityCapsule.model_validate(payload).model_dump(mode="json", exclude_none=True)
        expected_rel = continuity_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"]))
        if rel != expected_rel:
            return False, None
        return True, capsule
    except Exception:
        return False, None


def _validate_fallback_snapshot_payload(path: Path, restore_root: Path) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored continuity fallback snapshot envelope."""
    rel = str(path.relative_to(restore_root))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_type") != CONTINUITY_FALLBACK_SCHEMA_TYPE:
            return False, None
        if payload.get("schema_version") != CONTINUITY_FALLBACK_SCHEMA_VERSION:
            return False, None
        capsule = ContinuityCapsule.model_validate(payload.get("capsule")).model_dump(mode="json", exclude_none=True)
        expected_rel = continuity_fallback_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"]))
        if rel != expected_rel:
            return False, None
        payload["capsule"] = capsule
        return True, payload
    except Exception:
        return False, None


def _validate_archive_envelope_payload(path: Path, restore_root: Path) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored continuity archive envelope, including its active-path match."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
            return False, None
        if payload.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
            return False, None
        capsule = ContinuityCapsule.model_validate(payload.get("capsule")).model_dump(mode="json", exclude_none=True)
        expected_active_rel = continuity_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"]))
        if str(payload.get("active_path") or "") != expected_active_rel:
            return False, None
        payload["capsule"] = capsule
        return True, payload
    except Exception:
        return False, None


def _validate_restored_continuity(restore_root: Path) -> dict[str, Any]:
    """Inspect restored continuity artifacts and return structured validation output."""
    active_dir = restore_root / CONTINUITY_DIR_REL
    fallback_dir = restore_root / CONTINUITY_DIR_REL / "fallback"
    archive_dir = restore_root / CONTINUITY_DIR_REL / "archive"
    cold_dir = restore_root / CONTINUITY_COLD_DIR_REL
    cold_index_dir = restore_root / CONTINUITY_COLD_INDEX_DIR_REL

    active_paths = sorted(
        path for path in active_dir.glob("*.json") if path.is_file() and path.name != "refresh_state.json"
    ) if active_dir.exists() and active_dir.is_dir() else []
    fallback_paths = sorted(
        path for path in fallback_dir.glob("*.json") if path.is_file()
    ) if fallback_dir.exists() and fallback_dir.is_dir() else []
    archive_paths = sorted(
        path for path in archive_dir.glob("*.json") if path.is_file()
    ) if archive_dir.exists() and archive_dir.is_dir() else []
    cold_payload_paths = sorted(
        path for path in cold_dir.glob("*.json.gz") if path.is_file()
    ) if cold_dir.exists() and cold_dir.is_dir() else []
    cold_stub_paths = sorted(
        path for path in cold_index_dir.glob("*.md") if path.is_file()
    ) if cold_index_dir.exists() and cold_index_dir.is_dir() else []

    invalid_capsules: list[str] = []
    invalid_fallbacks: list[str] = []
    invalid_archives: list[str] = []
    missing_fallbacks: list[str] = []
    invalid_cold_stubs: list[str] = []
    invalid_cold_payloads: list[str] = []
    unmatched_cold_stubs: list[str] = []
    unmatched_cold_payloads: list[str] = []
    warnings: list[str] = []
    valid_fallbacks: set[str] = set()
    valid_stub_by_payload: dict[str, str] = {}

    for path in fallback_paths:
        valid, payload = _validate_fallback_snapshot_payload(path, restore_root)
        rel = str(path.relative_to(restore_root))
        if not valid or not isinstance(payload, dict):
            invalid_fallbacks.append(rel)
            warnings.append(f"continuity_invalid_fallback:{rel}")
            continue
        capsule = payload["capsule"]
        valid_fallbacks.add(continuity_fallback_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"])))

    for path in archive_paths:
        valid, _payload = _validate_archive_envelope_payload(path, restore_root)
        if not valid:
            rel = str(path.relative_to(restore_root))
            invalid_archives.append(rel)
            warnings.append(f"continuity_invalid_archive:{rel}")

    for path in cold_stub_paths:
        rel = str(path.relative_to(restore_root))
        try:
            frontmatter = _load_cold_stub(restore_root, rel)
        except HTTPException:
            invalid_cold_stubs.append(rel)
            warnings.append(f"continuity_invalid_cold_stub:{rel}")
            continue
        payload_rel = frontmatter["cold_storage_path"]
        valid_stub_by_payload[payload_rel] = rel

    for path in cold_payload_paths:
        rel = str(path.relative_to(restore_root))
        expected_archive_rel = continuity_archive_rel_path_from_cold_artifact(rel)
        expected_stub_rel = continuity_cold_stub_rel_path(expected_archive_rel)
        if rel not in valid_stub_by_payload:
            unmatched_cold_payloads.append(rel)
            warnings.append(f"continuity_unmatched_cold_payload:{rel}")
            continue
        if valid_stub_by_payload[rel] != expected_stub_rel:
            invalid_cold_payloads.append(rel)
            warnings.append(f"continuity_invalid_cold_payload:{rel}")
            continue
        try:
            payload = gzip.decompress(path.read_bytes())
            decoded = json.loads(payload.decode("utf-8"))
            if decoded.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
                raise ValueError("wrong schema_type")
            if decoded.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
                raise ValueError("wrong schema_version")
            capsule = ContinuityCapsule.model_validate(decoded.get("capsule")).model_dump(mode="json", exclude_none=True)
            if str(decoded.get("active_path") or "") != continuity_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"])):
                raise ValueError("wrong active_path")
            if _archive_rel_path_from_envelope({**decoded, "capsule": capsule}) != expected_archive_rel:
                raise ValueError("wrong archive identity")
        except Exception:
            invalid_cold_payloads.append(rel)
            warnings.append(f"continuity_invalid_cold_payload:{rel}")

    for payload_rel, stub_rel in valid_stub_by_payload.items():
        if not safe_path(restore_root, payload_rel).exists():
            unmatched_cold_stubs.append(stub_rel)
            warnings.append(f"continuity_unmatched_cold_stub:{stub_rel}")

    for path in active_paths:
        valid, capsule = _validate_active_continuity_payload(path, restore_root)
        rel = str(path.relative_to(restore_root))
        if not valid or not isinstance(capsule, dict):
            invalid_capsules.append(rel)
            warnings.append(f"continuity_invalid_capsule:{rel}")
            continue
        fallback_rel = continuity_fallback_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"]))
        if fallback_rel not in valid_fallbacks:
            missing_fallbacks.append(rel)
            warnings.append(f"continuity_missing_fallback:{rel}")

    return {
        "ok": not (
            invalid_capsules
            or invalid_fallbacks
            or invalid_archives
            or missing_fallbacks
            or invalid_cold_stubs
            or invalid_cold_payloads
            or unmatched_cold_stubs
            or unmatched_cold_payloads
        ),
        "cold_payloads": len(cold_payload_paths),
        "cold_stubs": len(cold_stub_paths),
        "active_capsules": len(active_paths),
        "fallback_capsules": len(fallback_paths),
        "archive_envelopes": len(archive_paths),
        "invalid_capsules": invalid_capsules,
        "invalid_fallbacks": invalid_fallbacks,
        "invalid_archives": invalid_archives,
        "invalid_cold_stubs": invalid_cold_stubs,
        "invalid_cold_payloads": invalid_cold_payloads,
        "unmatched_cold_stubs": unmatched_cold_stubs,
        "unmatched_cold_payloads": unmatched_cold_payloads,
        "missing_fallbacks": missing_fallbacks,
        "warnings": warnings,
    }


def _is_iso_timestamp(value: Any) -> bool:
    """Return whether a value is a parseable ISO-8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _validate_artifact_history_payload(
    path: Path,
    restore_root: Path,
    *,
    family: str,
    schema_type: str,
) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored artifact-history payload file."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False, None
    if not isinstance(payload, dict):
        return False, None
    if payload.get("schema_type") != schema_type:
        return False, None
    if payload.get("schema_version") != "1.0":
        return False, None
    if payload.get("family") != family:
        return False, None
    history_id = payload.get("history_id")
    if not isinstance(history_id, str) or history_id != path.stem:
        return False, None
    if not isinstance(payload.get("artifact_id"), str) or not payload["artifact_id"].strip():
        return False, None
    if not isinstance(payload.get("source_path"), str) or not payload["source_path"].strip():
        return False, None
    if not _is_iso_timestamp(payload.get("cut_at")):
        return False, None
    if not isinstance(payload.get("artifact"), dict):
        return False, None
    if not isinstance(payload.get("summary"), dict):
        return False, None
    artifact = payload["artifact"]
    artifact_id = str(payload.get("artifact_id") or "")
    artifact_id_key = _family_artifact_id_key(family)
    if not artifact_id or str(artifact.get(artifact_id_key) or "") != artifact_id:
        return False, None
    if str(payload.get("source_path") or "") != _family_source_rel(family, artifact_id):
        return False, None
    if payload.get("summary") != _family_summary(family, artifact):
        return False, None
    payload["rel"] = str(path.relative_to(restore_root))
    return True, payload


def _validate_artifact_history_cold_payload(
    path: Path,
    restore_root: Path,
    *,
    family: str,
) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored cold artifact-history payload file."""
    try:
        payload_bytes = gzip.decompress(path.read_bytes())
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return False, None
    if not isinstance(payload, dict):
        return False, None
    hot_rel = artifact_history_payload_rel_path_from_cold_artifact(str(path.relative_to(restore_root)))
    if payload.get("schema_type") != _ARTIFACT_HISTORY_SCHEMA_TYPES_BY_FAMILY[family]:
        return False, None
    if payload.get("schema_version") != "1.0":
        return False, None
    if payload.get("family") != family:
        return False, None
    if payload.get("history_id") != Path(hot_rel).stem:
        return False, None
    if not _is_iso_timestamp(payload.get("cut_at")):
        return False, None
    artifact = payload.get("artifact")
    if not isinstance(artifact, dict):
        return False, None
    if not isinstance(payload.get("summary"), dict):
        return False, None
    artifact_id = str(payload.get("artifact_id") or "")
    artifact_id_key = _family_artifact_id_key(family)
    if not artifact_id or str(artifact.get(artifact_id_key) or "") != artifact_id:
        return False, None
    if str(payload.get("source_path") or "") != _family_source_rel(family, artifact_id):
        return False, None
    if payload.get("summary") != _family_summary(family, artifact):
        return False, None
    payload["rel"] = str(path.relative_to(restore_root))
    return True, payload


def _validate_artifact_history_stub(path: Path, restore_root: Path) -> tuple[bool, dict[str, Any] | None]:
    """Validate one restored artifact-history stub file."""
    try:
        stub = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False, None
    if not isinstance(stub, dict):
        return False, None
    if stub.get("schema_type") != "artifact_history_stub":
        return False, None
    if stub.get("schema_version") != "1.0":
        return False, None
    history_id = stub.get("history_id")
    if not isinstance(history_id, str) or history_id != path.stem:
        return False, None
    if not isinstance(stub.get("family"), str) or not stub["family"].strip():
        return False, None
    if not isinstance(stub.get("payload_path"), str) or not stub["payload_path"].strip():
        return False, None
    if not _is_iso_timestamp(stub.get("created_at")):
        return False, None
    if not isinstance(stub.get("source_path"), str) or not stub["source_path"].strip():
        return False, None
    if not isinstance(stub.get("summary"), dict):
        return False, None
    stub["rel"] = str(path.relative_to(restore_root))
    return True, stub


def _validate_restored_artifact_history(restore_root: Path) -> dict[str, Any]:
    """Inspect restored artifact-history payloads/stubs and validate symmetry."""
    payload_count = 0
    cold_payload_count = 0
    stub_count = 0
    invalid_payloads: list[str] = []
    invalid_cold_payloads: list[str] = []
    invalid_stubs: list[str] = []
    missing_payloads: list[str] = []
    unmatched_payloads: list[str] = []
    mismatched_stubs: list[str] = []
    warnings: list[str] = []
    valid_payloads: dict[str, dict[str, Any]] = {}
    valid_stubs: dict[str, dict[str, Any]] = {}

    for spec in _ARTIFACT_HISTORY_SPECS:
        history_dir = restore_root / spec["history_dir_rel"]
        if history_dir.exists() and history_dir.is_dir():
            for path in sorted(history_dir.glob("*.json")):
                if not path.is_file():
                    continue
                payload_count += 1
                valid, payload = _validate_artifact_history_payload(
                    path,
                    restore_root,
                    family=spec["family"],
                    schema_type=spec["payload_schema_type"],
                )
                rel = str(path.relative_to(restore_root))
                if not valid or payload is None:
                    invalid_payloads.append(rel)
                    warnings.append(f"artifact_history_invalid_payload:{rel}")
                    continue
                valid_payloads[rel] = payload

        cold_dir = history_dir / "cold"
        if cold_dir.exists() and cold_dir.is_dir():
            for path in sorted(cold_dir.glob("*.json.gz")):
                if not path.is_file():
                    continue
                cold_payload_count += 1
                valid, payload = _validate_artifact_history_cold_payload(
                    path,
                    restore_root,
                    family=spec["family"],
                )
                rel = str(path.relative_to(restore_root))
                if not valid or payload is None:
                    invalid_cold_payloads.append(rel)
                    warnings.append(f"artifact_history_invalid_cold_payload:{rel}")
                    continue
                valid_payloads[rel] = payload

        stub_dir = history_dir / "index"
        if stub_dir.exists() and stub_dir.is_dir():
            for path in sorted(stub_dir.glob("*.json")):
                if not path.is_file():
                    continue
                stub_count += 1
                valid, stub = _validate_artifact_history_stub(path, restore_root)
                rel = str(path.relative_to(restore_root))
                if not valid or stub is None:
                    invalid_stubs.append(rel)
                    warnings.append(f"artifact_history_invalid_stub:{rel}")
                    continue
                valid_stubs[rel] = stub

    for rel, stub in valid_stubs.items():
        try:
            payload_rel = str(safe_path(restore_root, stub["payload_path"]).relative_to(restore_root))
        except Exception:
            invalid_stubs.append(rel)
            warnings.append(f"artifact_history_invalid_stub_payload_path:{rel}")
            continue

        payload = valid_payloads.get(payload_rel)
        if payload is None:
            missing_payloads.append(rel)
            warnings.append(f"artifact_history_missing_payload:{rel}")
            continue

        if (
            stub.get("family") != payload.get("family")
            or stub.get("history_id") != payload.get("history_id")
            or stub.get("source_path") != payload.get("source_path")
            or stub.get("summary") != payload.get("summary")
        ):
            mismatched_stubs.append(rel)
            warnings.append(f"artifact_history_stub_mismatch:{rel}")

    for rel in valid_payloads:
        payload_path = Path(rel)
        if payload_path.suffix == ".gz":
            hot_rel = artifact_history_payload_rel_path_from_cold_artifact(rel)
            expected_stub_rel = str(Path(hot_rel).parent / "index" / Path(hot_rel).name)
        else:
            expected_stub_rel = str(payload_path.parent / "index" / payload_path.name)
        if expected_stub_rel not in valid_stubs:
            unmatched_payloads.append(rel)
            warnings.append(f"artifact_history_missing_stub:{rel}")

    return {
        "ok": not (invalid_payloads or invalid_cold_payloads or invalid_stubs or missing_payloads or unmatched_payloads or mismatched_stubs),
        "payloads": payload_count,
        "cold_payloads": cold_payload_count,
        "stubs": stub_count,
        "invalid_payloads": invalid_payloads,
        "invalid_cold_payloads": invalid_cold_payloads,
        "invalid_stubs": invalid_stubs,
        "missing_payloads": missing_payloads,
        "unmatched_payloads": unmatched_payloads,
        "mismatched_stubs": mismatched_stubs,
        "warnings": warnings,
    }


def _load_replication_tombstones(repo_root: Path) -> dict[str, Any]:
    """Load replication tombstone state with a normalized fallback payload."""
    path = safe_path(repo_root, REPLICATION_TOMBSTONES_REL)
    if not path.exists():
        return {"schema_version": "1.0", "entries": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "entries": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "entries": {}}
    entries = data.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    return {"schema_version": "1.0", "entries": entries}


def _write_replication_tombstones(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Persist the replication tombstone payload."""
    path = safe_path(repo_root, REPLICATION_TOMBSTONES_REL)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _parse_dt_or_epoch(iso_value: str | None, fallback_epoch: float, *, parse_iso: Callable[[str | None], datetime | None]) -> float:
    """Return a parsed timestamp or the provided fallback epoch seconds."""
    dt = parse_iso(iso_value)
    if dt is None:
        return fallback_epoch
    return dt.timestamp()


def load_replication_state(repo_root: Path) -> dict[str, Any]:
    """Load normalized replication state from disk."""
    path = safe_path(repo_root, REPLICATION_STATE_REL)
    if not path.exists():
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    if not isinstance(data.get("last_pull_by_source"), dict):
        data["last_pull_by_source"] = {}
    if not isinstance(data.get("pull_idempotency"), dict):
        data["pull_idempotency"] = {}
    return data


def _write_replication_state(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Persist replication state to disk."""
    path = safe_path(repo_root, REPLICATION_STATE_REL)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _sha256_text(content: str) -> str:
    """Return the SHA-256 digest for text content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def iter_replication_files(repo_root: Path, include_prefixes: list[str], max_files: int, include_deleted: bool = True) -> list[dict[str, Any]]:
    """Enumerate replication candidates and optional tombstones under allowed prefixes."""
    prefixes = []
    for raw in include_prefixes:
        rel = str(raw or "").strip().strip("/")
        if not rel:
            continue
        top = Path(rel).parts[0] if Path(rel).parts else ""
        if top not in REPLICATION_ALLOWED_PREFIXES:
            continue
        prefixes.append(rel)
    if not prefixes:
        prefixes = ["memory", "messages", "projects", "essays", "journal", "tasks", "patches", "runs", "snapshots"]

    items = []
    for prefix in prefixes:
        base = safe_path(repo_root, prefix)
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or ".git" in path.parts:
                continue
            rel = str(path.relative_to(repo_root))
            try:
                content = path.read_text(encoding="utf-8")
            except Exception:
                continue
            stat = path.stat()
            items.append(
                {
                    "path": rel,
                    "content": content,
                    "sha256": _sha256_text(content),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "deleted": False,
                    "tombstone_at": None,
                }
            )
            if len(items) >= max_files:
                return items

    if include_deleted and len(items) < max_files:
        tombstones = _load_replication_tombstones(repo_root)
        entries = tombstones.get("entries", {})
        if isinstance(entries, dict):
            for path, row in sorted(entries.items(), key=lambda x: x[0]):
                top = Path(str(path)).parts[0] if Path(str(path)).parts else ""
                if top not in REPLICATION_ALLOWED_PREFIXES:
                    continue
                if prefixes and not any(str(path).startswith(f"{p}/") or str(path) == p for p in prefixes):
                    continue
                if not isinstance(row, dict):
                    continue
                items.append(
                    {
                        "path": str(path),
                        "content": None,
                        "sha256": None,
                        "modified_at": row.get("tombstone_at"),
                        "deleted": True,
                        "tombstone_at": row.get("tombstone_at"),
                    }
                )
                if len(items) >= max_files:
                    return items
    return items


def metrics_service(
    *,
    settings: Any,
    auth: AuthContext,
    load_delivery_state: Callable[[Path], dict[str, Any]],
    delivery_record_view: Callable[[dict[str, Any], datetime], dict[str, Any]],
    load_check_artifacts: Callable[[Path], list[dict[str, Any]]],
    load_rate_limit_state: Callable[[Path], dict[str, Any]],
    parse_iso: Callable[[str | None], datetime | None],
    max_jsonl_read_bytes: int = DEFAULT_MAX_JSONL_READ_BYTES,
) -> dict:
    """Assemble operational metrics, summaries, and alarm conditions."""
    auth.require("read:index")
    auth.require_read_path("messages/state/delivery_index.json")
    auth.require_read_path("logs/api_audit.jsonl")
    now = datetime.now(timezone.utc)

    state = load_delivery_state(settings.repo_root)
    delivery_summary: dict[str, int] = {}
    by_recipient: dict[str, dict[str, int]] = {}
    for row in state.get("records", {}).values():
        if not isinstance(row, dict):
            continue
        view = delivery_record_view(row, now)
        eff = str(view.get("effective_status") or "unknown")
        delivery_summary[eff] = delivery_summary.get(eff, 0) + 1
        recipient = str(view.get("to") or "unknown")
        rec = by_recipient.setdefault(recipient, {"total": 0, "pending": 0, "acked": 0, "dead_letter": 0})
        rec["total"] += 1
        if eff == "pending_ack":
            rec["pending"] += 1
        elif eff == "acked":
            rec["acked"] += 1
        elif eff == "dead_letter":
            rec["dead_letter"] += 1

    acked = delivery_summary.get("acked", 0)
    dead_letter = delivery_summary.get("dead_letter", 0)
    ack_denom = acked + dead_letter
    ack_success_ratio = (acked / ack_denom) if ack_denom > 0 else 1.0

    event_counts: dict[str, int] = {}
    peer_counts: dict[str, int] = {}
    metrics_warnings: list[str] = []
    audit_raw: str | None = None
    audit_path = settings.repo_root / "logs" / "api_audit.jsonl"
    if audit_path.exists():
        try:
            audit_file_size = audit_path.stat().st_size
        except OSError:
            _logger.warning("stat() failed on audit log %s; skipping audit metrics", audit_path, exc_info=True)
            metrics_warnings.append("audit_stat_failed: unable to determine audit log size; audit metrics unavailable")
        else:
            if audit_file_size > max_jsonl_read_bytes:
                _logger.warning(
                    "Audit log %s is %d bytes (limit %d); skipping audit metrics",
                    audit_path, audit_file_size, max_jsonl_read_bytes,
                )
                metrics_warnings.append(
                    f"audit_too_large: file is {audit_file_size} bytes, exceeds {max_jsonl_read_bytes} byte safety limit; "
                    "audit metrics unavailable until file is compacted or truncated"
                )
            else:
                try:
                    audit_raw = audit_path.read_text(encoding="utf-8", errors="replace")
                except MemoryError:
                    _logger.critical("OOM while reading audit log %s", audit_path, exc_info=True)
                    raise
                except Exception:  # noqa: BLE001 — mission-critical degradation
                    _logger.warning("Failed to read audit log %s for metrics", audit_path, exc_info=True)
                    metrics_warnings.append("audit_read_failed: I/O error reading audit log; audit metrics unavailable")
    if audit_raw:
        if "\ufffd" in audit_raw:
            _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", audit_path)
        for line in audit_raw.splitlines()[-10000:]:
            try:
                item = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            ev = str(item.get("event") or "unknown")
            event_counts[ev] = event_counts.get(ev, 0) + 1
            peer = str(item.get("peer_id") or "unknown")
            peer_counts[peer] = peer_counts.get(peer, 0) + 1

    check_artifacts = load_check_artifacts(settings.repo_root)
    check_summary: dict[str, int] = {}
    for row in check_artifacts:
        profile = str(row.get("profile") or "unknown")
        status = str(row.get("status") or "unknown")
        key = f"{profile}:{status}"
        check_summary[key] = check_summary.get(key, 0) + 1

    replication_state = load_replication_state(settings.repo_root)

    rate_state = load_rate_limit_state(settings.repo_root)
    verification_failures_recent = 0
    fail_cutoff = now - timedelta(seconds=int(settings.verify_failure_window_seconds))
    for row in rate_state.get("verification_failures", []):
        if not isinstance(row, dict):
            continue
        at = parse_iso(row.get("at"))
        if at is not None and at >= fail_cutoff:
            verification_failures_recent += 1

    alarms: list[dict[str, Any]] = []
    backlog_depth = delivery_summary.get("pending_ack", 0)
    if backlog_depth > int(settings.backlog_alarm_threshold):
        alarms.append(
            {
                "type": "delivery_backlog_growth",
                "severity": "warning",
                "message": f"Pending backlog depth {backlog_depth} exceeds threshold {settings.backlog_alarm_threshold}",
                "metric": "delivery.backlog_depth",
            }
        )

    if verification_failures_recent > int(settings.verification_alarm_threshold):
        alarms.append(
            {
                "type": "verification_failures",
                "severity": "warning",
                "message": (
                    f"Verification failures in last {settings.verify_failure_window_seconds}s: "
                    f"{verification_failures_recent} (threshold {settings.verification_alarm_threshold})"
                ),
                "metric": "security.verification_failures_recent",
            }
        )

    drift_threshold = int(settings.replication_drift_max_age_seconds)
    last_push = replication_state.get("last_push")
    if isinstance(last_push, dict):
        pushed_at = parse_iso(last_push.get("pushed_at"))
        if pushed_at is not None and (now - pushed_at).total_seconds() > drift_threshold:
            alarms.append(
                {
                    "type": "replication_drift",
                    "severity": "warning",
                    "message": f"Last replication push is stale (> {drift_threshold}s)",
                    "metric": "replication.last_push",
                }
            )

    pulls = replication_state.get("last_pull_by_source", {})
    if isinstance(pulls, dict):
        for source, row in pulls.items():
            if not isinstance(row, dict):
                continue
            pulled_at = parse_iso(row.get("pulled_at"))
            if pulled_at is not None and (now - pulled_at).total_seconds() > drift_threshold:
                alarms.append(
                    {
                        "type": "replication_drift",
                        "severity": "warning",
                        "message": f"Replication pull from {source} is stale (> {drift_threshold}s)",
                        "metric": "replication.last_pull_by_source",
                        "source_peer": source,
                    }
                )

    result = {
        "ok": True,
        "generated_at": now.isoformat(),
        "delivery": {
            "summary": delivery_summary,
            "backlog_depth": backlog_depth,
            "ack_success_ratio": round(ack_success_ratio, 4),
            "by_recipient": by_recipient,
        },
        "checks": {"summary": check_summary, "artifact_count": len(check_artifacts)},
        "audit": {"event_counts": event_counts, "peer_counts": peer_counts},
        "security": {
            "verification_failures_recent": verification_failures_recent,
            "verification_failure_window_seconds": int(settings.verify_failure_window_seconds),
        },
        "replication": {
            "last_push": replication_state.get("last_push"),
            "last_pull_by_source": replication_state.get("last_pull_by_source", {}),
        },
        "alarms": alarms,
    }
    all_warnings = list(state.get("warnings") or []) + metrics_warnings
    if all_warnings:
        result["warnings"] = all_warnings
        result["degraded"] = True
    return result


def replication_pull_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: ReplicationPullRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Pull a replication bundle from a peer and apply accepted file updates."""
    enforce_rate_limit(settings, auth, "replication_pull")
    enforce_payload_limit(settings, req.model_dump(), "replication_pull")
    auth.require("admin:peers")

    state = load_replication_state(settings.repo_root)
    idempotency_key = (req.idempotency_key or "").strip() or None
    idem_ref = f"{req.source_peer}|{idempotency_key}" if idempotency_key else None
    if idem_ref:
        previous = state.get("pull_idempotency", {}).get(idem_ref)
        if isinstance(previous, dict):
            return {
                "ok": True,
                "idempotent_replay": True,
                "source_peer": req.source_peer,
                "received_count": int(previous.get("received_count") or 0),
                "changed_count": int(previous.get("changed_count") or 0),
                "deleted_count": int(previous.get("deleted_count") or 0),
                "conflict_count": int(previous.get("conflict_count") or 0),
                "skipped_count": int(previous.get("skipped_count") or 0),
                "committed_files": [],
                "latest_commit": gm.latest_commit(),
            }

    committed_files: list[str] = []
    rollback_plan: list[tuple[Path, bytes | None]] = []
    seen_paths: set[Path] = set()
    changed_paths: list[Path] = []
    changed_rels: list[str] = []
    changed = 0
    deleted = 0
    skipped = 0
    conflicts = 0
    tombstones = _load_replication_tombstones(settings.repo_root)
    tomb_entries = tombstones.setdefault("entries", {})
    if not isinstance(tomb_entries, dict):
        tomb_entries = {}
        tombstones["entries"] = tomb_entries

    def track_change(path: Path, rel: str) -> None:
        _capture_path_state(path, rollback_plan, seen_paths)
        if path not in changed_paths:
            changed_paths.append(path)
            changed_rels.append(rel)

    now = datetime.now(timezone.utc)
    try:
        for file_row in req.files:
            top = Path(file_row.path).parts[0] if Path(file_row.path).parts else ""
            if top not in REPLICATION_ALLOWED_PREFIXES:
                raise HTTPException(status_code=400, detail=f"Replication path namespace not allowed: {file_row.path}")
            auth.require_write_path(file_row.path)

            path = safe_path(settings.repo_root, file_row.path)
            local_exists = path.exists() and path.is_file()
            local_content = read_text_file(path) if local_exists else None
            local_epoch = path.stat().st_mtime if local_exists else 0.0
            remote_epoch = _parse_dt_or_epoch(file_row.modified_at, now.timestamp(), parse_iso=parse_iso)

            if file_row.deleted:
                if req.conflict_policy == "target_wins" and local_exists:
                    conflicts += 1
                    skipped += 1
                    continue
                if req.conflict_policy == "error" and local_exists:
                    raise HTTPException(status_code=409, detail=f"Replication conflict on delete: {file_row.path}")

                if local_exists:
                    track_change(path, file_row.path)
                    try:
                        path.unlink()
                        deleted += 1
                        changed += 1
                    except Exception as e:
                        raise HTTPException(status_code=500, detail=f"Failed to delete replicated path {file_row.path}: {e}") from e
                else:
                    skipped += 1

                tomb_entries[file_row.path] = {
                    "tombstone_at": file_row.tombstone_at or now.isoformat(),
                    "source_peer": req.source_peer,
                    "idempotency_key": idempotency_key,
                }
                continue

            if file_row.content is None or file_row.sha256 is None:
                raise HTTPException(status_code=400, detail=f"Replication file payload requires content+sha256 for upsert: {file_row.path}")
            if _sha256_text(file_row.content) != file_row.sha256:
                raise HTTPException(status_code=400, detail=f"Replication sha256 mismatch for {file_row.path}")

            if req.mode == "upsert" and local_exists and local_content == file_row.content:
                skipped += 1
                continue

            should_write = True
            if local_exists and local_content != file_row.content:
                if req.conflict_policy == "target_wins":
                    should_write = False
                    conflicts += 1
                elif req.conflict_policy == "error":
                    raise HTTPException(status_code=409, detail=f"Replication conflict on path: {file_row.path}")
                elif req.conflict_policy == "last_write_wins" and remote_epoch < local_epoch:
                    should_write = False
                    conflicts += 1

            if not should_write:
                skipped += 1
                continue

            track_change(path, file_row.path)
            write_text_file(path, file_row.content)
            changed += 1
            tomb_entries.pop(file_row.path, None)

        # Synchronous pre-write capture per #112: externalize superseded pull row
        _pull_history_extra: list[str] = []
        pull_by_source = state.setdefault("last_pull_by_source", {})
        previous_pull = pull_by_source.get(req.source_peer)
        if isinstance(previous_pull, dict) and previous_pull:
            try:
                ext_result = externalize_superseded_pull(
                    repo_root=settings.repo_root,
                    now=now,
                    source_peer=req.source_peer,
                    previous_row=previous_pull,
                    hot_retention_days=int(getattr(settings, "replication_history_hot_retention_days", 14)),
                )
                if ext_result:
                    _pull_history_extra.extend([ext_result["shard_path"], ext_result["stub_path"]])
                    # Update history_meta per #112
                    hm = state.setdefault("history_meta", {})
                    if not isinstance(hm, dict):
                        hm = {}
                        state["history_meta"] = hm
                    rs = hm.setdefault("replication_state", {})
                    if not isinstance(rs, dict):
                        rs = {}
                        hm["replication_state"] = rs
                    rs["last_cut_at"] = now.isoformat()
                    rs["last_cut_pull_count"] = rs.get("last_cut_pull_count", 0) + 1 if isinstance(rs.get("last_cut_pull_count"), int) else 1
            except Exception:
                _logger.error(
                    "Failed to externalize superseded pull row for %s; row data will be lost on overwrite: %s",
                    req.source_peer,
                    json.dumps(previous_pull, ensure_ascii=False, default=str),
                    exc_info=True,
                )

        pull_by_source[req.source_peer] = {
            "pulled_at": now.isoformat(),
            "received_count": len(req.files),
            "changed_count": changed,
            "deleted_count": deleted,
            "conflict_count": conflicts,
            "mode": req.mode,
            "conflict_policy": req.conflict_policy,
            "idempotency_key": idempotency_key,
        }
        if idem_ref:
            pull_map = state.setdefault("pull_idempotency", {})
            if not isinstance(pull_map, dict):
                pull_map = {}
                state["pull_idempotency"] = pull_map
            pull_map[idem_ref] = {
                "at": now.isoformat(),
                "received_count": len(req.files),
                "changed_count": changed,
                "deleted_count": deleted,
                "conflict_count": conflicts,
                "skipped_count": skipped,
            }

        tomb_path = safe_path(settings.repo_root, REPLICATION_TOMBSTONES_REL)
        track_change(tomb_path, REPLICATION_TOMBSTONES_REL)
        _write_replication_tombstones(settings.repo_root, tombstones)

        # Track history shard/stub paths for rollback if head write fails.
        # These files were just created by externalize_superseded_pull, so we
        # record them with prior bytes=None so rollback *deletes* them rather
        # than restoring the new content (which would orphan shards the head
        # no longer references).
        for extra_rel in _pull_history_extra:
            extra_path = safe_path(settings.repo_root, extra_rel)
            resolved = extra_path.resolve()
            if resolved not in seen_paths:
                seen_paths.add(resolved)
                rollback_plan.append((extra_path, None))
            if extra_path not in changed_paths:
                changed_paths.append(extra_path)
                changed_rels.append(extra_rel)

        state_path = safe_path(settings.repo_root, REPLICATION_STATE_REL)
        track_change(state_path, REPLICATION_STATE_REL)
        _write_replication_state(settings.repo_root, state)
    except Exception as exc:
        _restore_rollback_plan(rollback_plan)
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"Failed replication pull write for {req.source_peer}: {exc}") from exc

    if safe_commit_paths(
        rollback_plan=rollback_plan,
        gm=gm,
        commit_message=req.commit_message or f"replication: pull {req.source_peer}",
        error_detail=f"Failed to durably commit replication pull for {req.source_peer}",
    ):
        committed_files.extend(changed_rels)

    audit(
        auth,
        "replication_pull",
        {
            "source_peer": req.source_peer,
            "received": len(req.files),
            "changed": changed,
            "deleted": deleted,
            "conflicts": conflicts,
            "mode": req.mode,
            "conflict_policy": req.conflict_policy,
            "idempotency_key": idempotency_key,
        },
    )
    return {
        "ok": True,
        "idempotent_replay": False,
        "source_peer": req.source_peer,
        "received_count": len(req.files),
        "changed_count": changed,
        "deleted_count": deleted,
        "conflict_count": conflicts,
        "skipped_count": skipped,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


def replication_push_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: ReplicationPushRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    load_peers_registry: Callable[[Path], dict[str, Any]],
    urlopen_fn: Callable[..., Any] | None = None,
    url_request_factory: Callable[..., Any] | None = None,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Push a replication bundle to a peer target using the current repository state."""
    if urlopen_fn is None:
        urlopen_fn = urlopen
    if url_request_factory is None:
        url_request_factory = UrlRequest

    enforce_rate_limit(settings, auth, "replication_push")
    auth.require("admin:peers")

    files = iter_replication_files(settings.repo_root, req.include_prefixes, req.max_files, include_deleted=req.include_deleted)
    for row in files:
        auth.require_read_path(str(row.get("path", "")))

    by_prefix: dict[str, int] = {}
    for row in files:
        top = Path(str(row["path"])).parts[0] if Path(str(row["path"])).parts else ""
        by_prefix[top] = by_prefix.get(top, 0) + 1

    target_base = req.base_url
    if not target_base and req.peer_id:
        registry = load_peers_registry(settings.repo_root)
        peer = registry.get("peers", {}).get(req.peer_id)
        if isinstance(peer, dict):
            target_base = str(peer.get("base_url") or "").strip() or None

    push_id_source = req.idempotency_key or canonical_json(
        {
            "peer": auth.peer_id,
            "target": target_base,
            "path": req.target_path,
            "policy": req.conflict_policy,
            "files": [{"path": f.get("path"), "sha256": f.get("sha256"), "deleted": bool(f.get("deleted"))} for f in files],
        }
    )
    push_id = "push_" + hashlib.sha256(push_id_source.encode("utf-8")).hexdigest()[:24]

    if req.dry_run or not target_base:
        return {
            "ok": True,
            "dry_run": True,
            "idempotency_key": push_id,
            "file_count": len(files),
            "by_prefix": by_prefix,
            "target_base_url": target_base,
            "target_path": req.target_path,
            "sample_paths": [row["path"] for row in files[:20]],
            "include_deleted": req.include_deleted,
            "conflict_policy": req.conflict_policy,
        }

    target_url = urljoin(target_base.rstrip("/") + "/", req.target_path.lstrip("/"))
    request_payload = {
        "source_peer": auth.peer_id,
        "files": files,
        "mode": "upsert",
        "conflict_policy": req.conflict_policy,
        "idempotency_key": push_id,
    }
    enforce_payload_limit(settings, request_payload, "replication_push")

    body = canonical_json(request_payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if req.target_token:
        headers["Authorization"] = f"Bearer {req.target_token}"
    try:
        with urlopen_fn(url_request_factory(target_url, data=body, headers=headers, method="POST"), timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            remote_payload = json.loads(raw) if raw else {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed replication push: {e}") from e

    auth.require_write_path(REPLICATION_STATE_REL)
    state = load_replication_state(settings.repo_root)

    # Synchronous pre-write capture per #112: externalize superseded push row
    push_now = datetime.now(timezone.utc)  # single timestamp for shard cut_at and state pushed_at
    _history_extra_paths: list[str] = []
    previous_push = state.get("last_push")
    if isinstance(previous_push, dict) and previous_push:
        try:
            ext_result = externalize_superseded_push(
                repo_root=settings.repo_root,
                now=push_now,
                previous_row=previous_push,
                hot_retention_days=int(getattr(settings, "replication_history_hot_retention_days", 14)),
            )
            if ext_result:
                _history_extra_paths.extend([ext_result["shard_path"], ext_result["stub_path"]])
                # Update history_meta per #112
                hm = state.setdefault("history_meta", {})
                if not isinstance(hm, dict):
                    hm = {}
                    state["history_meta"] = hm
                rs = hm.setdefault("replication_state", {})
                if not isinstance(rs, dict):
                    rs = {}
                    hm["replication_state"] = rs
                rs["last_cut_at"] = push_now.isoformat()
                rs["last_cut_push_count"] = rs.get("last_cut_push_count", 0) + 1 if isinstance(rs.get("last_cut_push_count"), int) else 1
        except Exception:
            _logger.error(
                "Failed to externalize superseded push row; row data will be lost on overwrite: %s",
                json.dumps(previous_push, ensure_ascii=False, default=str),
                exc_info=True,
            )

    state["last_push"] = {
        "pushed_at": push_now.isoformat(),
        "target_url": target_url,
        "file_count": len(files),
        "by_prefix": by_prefix,
        "idempotency_key": push_id,
        "conflict_policy": req.conflict_policy,
        "include_deleted": req.include_deleted,
    }
    committed_files = []
    try:
        state_path = _write_replication_state(settings.repo_root, state)
    except Exception:
        # Clean up shard/stub files created by externalize_superseded_push
        # to avoid orphaned shards the head doesn't reference.
        for p in _history_extra_paths:
            safe_path(settings.repo_root, p).unlink(missing_ok=True)
        raise
    warnings: list[str] = []
    if _history_extra_paths:
        push_commit_paths = [state_path] + [safe_path(settings.repo_root, p) for p in _history_extra_paths]
        if try_commit_paths(paths=push_commit_paths, gm=gm, commit_message="replication: update push state"):
            committed_files.append(REPLICATION_STATE_REL)
            committed_files.extend(_history_extra_paths)
        else:
            warnings.append("replication_push_not_durable: state is on disk but not durably committed to git")
    elif try_commit_file(path=state_path, gm=gm, commit_message="replication: update push state"):
        committed_files.append(REPLICATION_STATE_REL)
    else:
        warnings.append("replication_push_not_durable: state is on disk but not durably committed to git")

    audit(
        auth,
        "replication_push",
        {
            "target_url": target_url,
            "file_count": len(files),
            "idempotency_key": push_id,
            "conflict_policy": req.conflict_policy,
            "include_deleted": req.include_deleted,
        },
    )
    result = {
        "ok": True,
        "dry_run": False,
        "idempotency_key": push_id,
        "target_url": target_url,
        "file_count": len(files),
        "by_prefix": by_prefix,
        "remote": remote_payload,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
    if warnings:
        result["warnings"] = warnings
    return result


def backup_create_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: BackupCreateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Create a deterministic backup archive and accompanying manifest."""
    enforce_rate_limit(settings, auth, "backup_create")
    enforce_payload_limit(settings, req.model_dump(), "backup_create")
    auth.require("admin:peers")

    allowed = set(REPLICATION_ALLOWED_PREFIXES) | {"config", "logs", "peers"}
    include = []
    for raw in req.include_prefixes:
        rel = str(raw or "").strip().strip("/")
        if not rel:
            continue
        top = Path(rel).parts[0] if Path(rel).parts else ""
        if top in allowed:
            include.append(rel)
    if not include:
        include = ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots", "peers", "config", "logs"]

    now = datetime.now(timezone.utc)
    backup_id = f"backup_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    backup_rel = f"{BACKUPS_DIR_REL}/{backup_id}.tar.gz"
    manifest_rel = f"{BACKUPS_DIR_REL}/{backup_id}.json"
    backup_path = safe_path(settings.repo_root, backup_rel)
    manifest_path = safe_path(settings.repo_root, manifest_rel)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    included_paths: list[str] = []
    rollback_plan = [(backup_path, None), (manifest_path, None)]
    try:
        with tarfile.open(backup_path, mode="w:gz") as tf:
            for prefix in include:
                path = safe_path(settings.repo_root, prefix)
                if not path.exists():
                    continue
                tf.add(path, arcname=prefix)
                included_paths.append(prefix)

        manifest_payload = {
            "schema_version": "1.0",
            "backup_id": backup_id,
            "created_at": now.isoformat(),
            "created_by": auth.peer_id,
            "include_prefixes": included_paths,
            "note": req.note,
            "contract_version": settings.contract_version,
        }
        if _continuity_included(included_paths):
            manifest_payload["continuity_counts"] = _continuity_counts(settings.repo_root)
        write_text_file(manifest_path, json.dumps(manifest_payload, ensure_ascii=False, indent=2))
    except Exception as exc:
        _restore_rollback_plan(rollback_plan)
        raise HTTPException(status_code=500, detail=f"Failed to create backup {backup_id}: {exc}") from exc

    committed_files = []
    if safe_commit_paths(
        rollback_plan=rollback_plan,
        gm=gm,
        commit_message=f"backup: create {backup_id}",
        error_detail=f"Failed to durably commit backup {backup_id}",
    ):
        committed_files.extend([backup_rel, manifest_rel])

    audit(auth, "backup_create", {"backup_id": backup_id, "include_prefixes": included_paths})
    return {
        "ok": True,
        "backup_id": backup_id,
        "backup_path": backup_rel,
        "manifest_path": manifest_rel,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


def _validate_segment_history(restore_root: Path) -> dict[str, Any]:
    """Validate segment-history artifacts in a restored backup.

    Per spec, scans all 6 family stub directories, validates stub JSON
    structure including ``segment_id``, ``source_path``, and ``summary``,
    checks hot/cold payload existence, decompresses cold payloads for CRC
    integrity, and verifies ``byte_size`` where applicable.

    Returns the spec-mandated ``segment_history_validation`` response shape
    with per-family rows.
    """
    from app.segment_history.families import FAMILIES, discover_active_sources
    from app.segment_history.service import (
        _FAMILY_EXTENSION,
        _rehydrate_hot_path,
        _validate_segment_id,
    )

    any_failure = False
    families_results: list[dict[str, Any]] = []

    for family_name, config in FAMILIES.items():
        active_sources_checked = 0
        hot_stubs_checked = 0
        cold_stubs_checked = 0
        rolled_payloads_checked = 0
        cold_payloads_checked = 0
        family_warnings: list[dict[str, Any]] = []
        has_failure = False

        # Step 1: scan active source roots
        try:
            active_sources = discover_active_sources(family_name, restore_root)
        except Exception:
            active_sources = []
        for src in active_sources:
            active_sources_checked += 1
            if not src.is_file():
                has_failure = True
                family_warnings.append({
                    "code": "segment_history_active_source_missing",
                    "detail": f"Active source missing: {src.name}",
                    "path": str(src.relative_to(restore_root)),
                    "segment_id": None,
                })
            else:
                try:
                    src.open("rb").close()
                except OSError:
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_active_source_unreadable",
                        "detail": f"Active source present but unreadable: {src.name}",
                        "path": str(src.relative_to(restore_root)),
                        "segment_id": None,
                    })

        # Step 2: collect stub dirs for this family
        stub_dirs: list[Path] = []
        if family_name == "journal":
            journal_history = restore_root / "journal" / "history"
            if journal_history.is_dir():
                for year_dir in sorted(journal_history.iterdir()):
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        stub_dirs.append(year_dir / "index")
        elif family_name == "message_stream":
            for kind in ("inbox", "outbox", "relay", "acks"):
                stub_dirs.append(
                    restore_root / "messages" / "history" / kind / "index"
                )
        else:
            stub_dirs.append(restore_root / config.stub_dir)

        # Step 3: scan stubs and validate
        for stub_dir in stub_dirs:
            if not stub_dir.is_dir():
                continue
            for entry in sorted(stub_dir.iterdir()):
                if not entry.name.endswith(".json") or not entry.is_file():
                    continue

                rel = str(entry.relative_to(restore_root))

                try:
                    stub = json.loads(entry.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_invalid_stub",
                        "detail": f"Stub not parseable: {rel}",
                        "path": rel,
                        "segment_id": None,
                    })
                    continue

                # Validate required stub fields
                if stub.get("schema_type") != "segment_history_stub":
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_invalid_stub_type",
                        "detail": f"Wrong schema_type: {rel}",
                        "path": rel,
                        "segment_id": None,
                    })
                    continue

                if stub.get("schema_version") != "1.0":
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_version_mismatch",
                        "detail": f"Unexpected schema_version: {rel}",
                        "path": rel,
                        "segment_id": None,
                    })
                    continue

                if stub.get("family") != family_name:
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_family_mismatch",
                        "detail": f"Family mismatch in stub: {rel}",
                        "path": rel,
                        "segment_id": None,
                    })
                    continue

                seg_id = stub.get("segment_id")
                if not seg_id or not isinstance(seg_id, str):
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_missing_segment_id",
                        "detail": f"Missing or invalid segment_id: {rel}",
                        "path": rel,
                        "segment_id": None,
                    })
                    continue

                if _validate_segment_id(family_name, seg_id) is None:
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_invalid_segment_id",
                        "detail": f"segment_id fails validation: {seg_id}",
                        "path": rel,
                        "segment_id": seg_id,
                    })
                    continue

                if not stub.get("source_path"):
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_missing_source_path",
                        "detail": f"Missing source_path: {rel}",
                        "path": rel,
                        "segment_id": seg_id,
                    })
                    continue

                if not isinstance(stub.get("summary"), dict):
                    has_failure = True
                    family_warnings.append({
                        "code": "segment_history_stub_missing_summary",
                        "detail": f"Missing or invalid summary: {rel}",
                        "path": rel,
                        "segment_id": seg_id,
                    })
                    continue

                payload_rel = stub.get("payload_path", "")
                cold_stored = stub.get("cold_stored_at")

                if not cold_stored:
                    # Hot stub
                    hot_stubs_checked += 1
                    if payload_rel:
                        hot_path = restore_root / payload_rel
                        rolled_payloads_checked += 1
                        if not hot_path.is_file():
                            has_failure = True
                            family_warnings.append({
                                "code": "segment_history_missing_hot_payload",
                                "detail": f"Rolled payload missing: {payload_rel}",
                                "path": rel,
                                "segment_id": seg_id,
                            })
                        # Verify payload path suffix matches family contract
                        expected_ext = _FAMILY_EXTENSION.get(family_name, ".jsonl")
                        if not payload_rel.endswith(expected_ext):
                            has_failure = True
                            family_warnings.append({
                                "code": "segment_history_payload_suffix_mismatch",
                                "detail": (
                                    f"Hot payload path suffix does not match "
                                    f"family extension {expected_ext}: {payload_rel}"
                                ),
                                "path": rel,
                                "segment_id": seg_id,
                            })
                    else:
                        has_failure = True
                        family_warnings.append({
                            "code": "segment_history_stub_missing_payload_path",
                            "detail": f"Hot stub has no payload_path: {rel}",
                            "path": rel,
                            "segment_id": seg_id,
                        })
                else:
                    # Cold stub
                    cold_stubs_checked += 1
                    if not payload_rel:
                        has_failure = True
                        family_warnings.append({
                            "code": "segment_history_stub_missing_payload_path",
                            "detail": f"Cold stub has no payload_path: {rel}",
                            "path": rel,
                            "segment_id": seg_id,
                        })
                        continue

                    cold_path = restore_root / payload_rel
                    cold_payloads_checked += 1
                    if not cold_path.is_file():
                        has_failure = True
                        family_warnings.append({
                            "code": "segment_history_missing_cold_payload",
                            "detail": f"Cold payload missing: {payload_rel}",
                            "path": rel,
                            "segment_id": seg_id,
                        })
                        continue

                    # Verify canonical hot restoration target derivation
                    source_path_str = stub.get("source_path", "")
                    try:
                        _rehydrate_hot_path(
                            family_name, seg_id, source_path_str, restore_root,
                        )
                    except Exception:
                        has_failure = True
                        family_warnings.append({
                            "code": "segment_history_cold_derivation_failed",
                            "detail": (
                                f"Cannot derive canonical hot restoration target "
                                f"from stub: {rel}"
                            ),
                            "path": rel,
                            "segment_id": seg_id,
                        })

                    # Decompress for CRC integrity check
                    try:
                        raw = cold_path.read_bytes()
                        decompressed = gzip.decompress(raw)
                    except Exception:
                        has_failure = True
                        family_warnings.append({
                            "code": "segment_history_cold_payload_corrupt",
                            "detail": f"Cold payload decompression failed: {payload_rel}",
                            "path": rel,
                            "segment_id": seg_id,
                        })
                        continue

                    # Verify byte_size if present in summary
                    summary_byte_size = stub.get("summary", {}).get("byte_size")
                    if summary_byte_size is not None:
                        if len(decompressed) != summary_byte_size:
                            has_failure = True
                            family_warnings.append({
                                "code": "segment_history_cold_byte_size_mismatch",
                                "detail": (
                                    f"Decompressed size {len(decompressed)} != "
                                    f"summary.byte_size {summary_byte_size}: {payload_rel}"
                                ),
                                "path": rel,
                                "segment_id": seg_id,
                            })

        if has_failure:
            any_failure = True

        families_results.append({
            "family": family_name,
            "active_sources_checked": active_sources_checked,
            "hot_stubs_checked": hot_stubs_checked,
            "cold_stubs_checked": cold_stubs_checked,
            "rolled_payloads_checked": rolled_payloads_checked,
            "cold_payloads_checked": cold_payloads_checked,
            "warnings": family_warnings,
        })

    return {
        "ok": not any_failure,
        "families_checked": len(families_results),
        "families": families_results,
    }


def backup_restore_test_service(
    *,
    settings: Any,
    auth: AuthContext,
    req: BackupRestoreTestRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    rebuild_index: Callable[[Path], dict[str, Any]],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Run a restore drill against an existing backup archive inside a temp directory."""
    enforce_rate_limit(settings, auth, "backup_restore_test")
    enforce_payload_limit(settings, req.model_dump(), "backup_restore_test")
    auth.require("admin:peers")

    rel = str(req.backup_path or "").strip()
    if not rel:
        raise HTTPException(status_code=400, detail="backup_path is required")
    if Path(rel).is_absolute():
        raise HTTPException(status_code=400, detail="backup_path must be repo-relative")
    if not rel.startswith(f"{BACKUPS_DIR_REL}/"):
        raise HTTPException(status_code=400, detail="backup_path must be under backups/")

    backup_path = safe_path(settings.repo_root, rel)
    if not backup_path.exists() or not backup_path.is_file():
        raise HTTPException(status_code=404, detail="Backup file not found")

    extracted_files = 0
    extracted_prefixes: set[str] = set()
    with tempfile.TemporaryDirectory() as td:
        restore_root = Path(td) / "restore"
        restore_root.mkdir(parents=True, exist_ok=True)
        try:
            with tarfile.open(backup_path, mode="r:gz") as tf:
                members = tf.getmembers()
                restore_root_resolved = restore_root.resolve()
                for m in members:
                    if m.issym() or m.islnk():
                        raise HTTPException(status_code=400, detail=f"Invalid backup archive: symbolic links are not allowed ({m.name})")
                    target = (restore_root / m.name).resolve()
                    if target != restore_root_resolved and restore_root_resolved not in target.parents:
                        raise HTTPException(status_code=400, detail=f"Invalid backup archive: unsafe path ({m.name})")
                tf.extractall(path=restore_root, filter="data")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid backup archive: {e}") from e

        for m in members:
            if not m.isfile():
                continue
            extracted_files += 1
            top = Path(m.name).parts[0] if Path(m.name).parts else ""
            if top:
                extracted_prefixes.add(top)

        index_validation = None
        if req.verify_index_rebuild:
            try:
                payload = rebuild_index(restore_root)
                index_validation = {"ok": True, "file_count": int(payload.get("file_count") or 0)}
            except Exception as e:
                index_validation = {"ok": False, "error": str(e)}

        continuity_validation = None
        if req.verify_continuity:
            continuity_validation = _validate_restored_continuity(restore_root)

        artifact_history_validation = _validate_restored_artifact_history(restore_root)

        segment_history_validation = _validate_segment_history(restore_root)

    ok = (
        extracted_files > 0
        and (index_validation is None or bool(index_validation.get("ok")))
        and (continuity_validation is None or bool(continuity_validation.get("ok")))
        and bool(artifact_history_validation.get("ok"))
        and bool(segment_history_validation.get("ok"))
    )
    audit(
        auth,
        "backup_restore_test",
        {
            "backup_path": rel,
            "ok": ok,
            "extracted_files": extracted_files,
            "continuity_ok": None if continuity_validation is None else bool(continuity_validation.get("ok")),
        },
    )
    return {
        "ok": ok,
        "backup_path": rel,
        "extracted_files": extracted_files,
        "extracted_prefixes": sorted(extracted_prefixes),
        "index_validation": index_validation,
        "continuity_validation": continuity_validation,
        "artifact_history_validation": artifact_history_validation,
        "segment_history_validation": segment_history_validation,
    }


def _load_access_stats(
    repo_root: Path,
    *,
    max_jsonl_read_bytes: int = DEFAULT_MAX_JSONL_READ_BYTES,
) -> tuple[dict[str, dict], list[str]]:
    """Load normalized access statistics used by compaction planning."""
    out: dict[str, dict] = {}
    warnings: list[str] = []
    path = repo_root / "logs" / "api_audit.jsonl"
    if not path.exists():
        return out, warnings
    try:
        file_size = path.stat().st_size
    except OSError:
        _logger.warning("stat() failed on audit log %s; skipping access stats", path, exc_info=True)
        warnings.append("access_stats_stat_failed: unable to determine audit log size; compaction planning ignores access recency")
        return out, warnings
    if file_size > max_jsonl_read_bytes:
        _logger.warning(
            "Audit log %s is %d bytes (limit %d); skipping access stats to avoid OOM",
            path, file_size, max_jsonl_read_bytes,
        )
        warnings.append(
            f"access_stats_too_large: audit log is {file_size} bytes, exceeds {max_jsonl_read_bytes} byte safety limit; "
            "compaction planning ignores access recency until the log is compacted or truncated"
        )
        return out, warnings
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except MemoryError:
        _logger.critical("OOM while reading audit log %s", path, exc_info=True)
        raise
    except Exception:  # noqa: BLE001 — mission-critical degradation
        _logger.warning("Failed to read audit log %s for access stats", path, exc_info=True)
        warnings.append("access_stats_read_failed: I/O error reading audit log; compaction planning ignores access recency")
        return out, warnings
    if "\ufffd" in raw:
        _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
    for line in raw.splitlines()[-5000:]:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if row.get("event") not in {"read", "messages_inbox", "search", "context_retrieve"}:
            continue
        detail = row.get("detail") or {}
        rel = detail.get("path")
        if not rel:
            continue
        stat = out.setdefault(rel, {"access_count": 0, "last_access_at": None})
        stat["access_count"] += 1
        ts = row.get("ts")
        if ts and (stat["last_access_at"] is None or ts > stat["last_access_at"]):
            stat["last_access_at"] = ts
    return out, warnings


def _memory_class_for_path(rel: str) -> str:
    """Classify a repository path into the compaction memory buckets."""
    if rel.startswith("memory/core/"):
        return "core"
    if rel.startswith("memory/summaries/") or rel.startswith("messages/threads/") or rel.startswith("projects/"):
        return "durable" if rel.startswith("memory/summaries/") else "working"
    if rel.startswith("journal/") or rel.startswith("messages/inbox/") or rel.startswith("messages/outbox/") or rel.startswith("logs/"):
        return "ephemeral"
    if rel.startswith("memory/episodic/"):
        return "ephemeral"
    return "working"


def _candidate_policy(repo_root: Path, path: Path, access_stats: dict[str, dict], *, parse_iso: Callable[[str | None], datetime | None]) -> dict | None:
    """Build the compaction policy candidate for a single repository file."""
    rel = str(path.relative_to(repo_root))
    if rel.startswith("index/") or ".git" in path.parts:
        return None
    try:
        st = path.stat()
    except MemoryError:
        raise
    except Exception:
        _logger.debug("Skipping compaction candidate %s because stat() failed", path, exc_info=True)
        return None
    now = datetime.now(timezone.utc)
    age_days = max(0.0, (now - datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)).total_seconds() / 86400.0)
    size_bytes = int(st.st_size)
    ns = Path(rel).parts[0] if Path(rel).parts else ""
    mem_class = _memory_class_for_path(rel)
    importance = 0.0
    snippet = ""
    text = ""
    if path.suffix.lower() in {".md", ".json", ".jsonl", ".txt"}:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            if "\ufffd" in text:
                _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
            snippet = " ".join(text.split())[:240]
        except Exception:  # noqa: BLE001 — mission-critical degradation
            _logger.warning("Failed to read %s for compaction policy", path, exc_info=True)
            text = ""
        if path.suffix.lower() == ".md":
            m = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
            if m:
                for line in m.group(1).splitlines():
                    if line.strip().startswith("importance:"):
                        try:
                            importance = float(line.split(":", 1)[1].strip())
                        except MemoryError:
                            raise
                        except (ValueError, IndexError):
                            _logger.debug("Ignoring malformed importance value in %s: %r", path, line, exc_info=True)
    a = access_stats.get(rel, {})
    access_count = int(a.get("access_count") or 0)
    last_access_dt = parse_iso(a.get("last_access_at"))
    last_access_days = 9999.0 if not last_access_dt else max(0.0, (now - last_access_dt).total_seconds() / 86400.0)

    type_weight = {"ephemeral": 1.0, "working": 0.35, "durable": 0.12, "core": -5.0}.get(mem_class, 0.2)
    age_pressure = min(1.5, age_days / 14.0)
    size_pressure = min(1.0, math.log10(max(10, size_bytes)) / 8.0)
    recency_relief = 0.9 if last_access_days < 3 else (0.35 if last_access_days < 14 else 0.0)
    frequency_relief = min(1.0, access_count / 12.0) * 0.75
    importance_relief = min(1.0, max(0.0, importance)) * 1.2
    active_link_relief = 0.6 if rel.startswith("messages/threads/") or rel.startswith("projects/") else 0.0

    candidate_score = round(type_weight + age_pressure + size_pressure - recency_relief - frequency_relief - importance_relief - active_link_relief, 4)

    promote_signals = []
    low = (text or "").lower()
    for kw in ["identity", "relationship", "trusted", "values", "preference", "decision"]:
        if kw in low:
            promote_signals.append(kw)
    if access_count >= 5:
        promote_signals.append("reused")
    if importance >= 0.7:
        promote_signals.append("high_importance")
    if mem_class in {"core"}:
        promote_signals.append("core_namespace")

    return {
        "path": rel,
        "namespace": ns,
        "memory_class": mem_class,
        "age_days": round(age_days, 2),
        "size_bytes": size_bytes,
        "importance": importance if importance else None,
        "access_count": access_count,
        "last_access_days": None if last_access_days >= 9999 else round(last_access_days, 2),
        "candidate_score": candidate_score,
        "promote_signals": sorted(set(promote_signals)),
        "snippet": snippet,
    }


def compact_run_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: CompactRequest,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Generate a compaction planning report for eligible repository files."""
    auth.require("compact:trigger")
    auth.require_write_path("memory/summaries/weekly/x.md")

    now = datetime.now(timezone.utc)
    report_id = f"compact_{now.strftime('%Y%m%dT%H%M%SZ')}"
    source_rel = req.source_path or "(policy-scan)"

    access_stats, access_warnings = _load_access_stats(
        settings.repo_root,
        max_jsonl_read_bytes=settings.max_jsonl_read_bytes,
    )
    candidates = []
    for path in settings.repo_root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".md", ".json", ".jsonl", ".txt"}:
            continue
        c = _candidate_policy(settings.repo_root, path, access_stats, parse_iso=parse_iso)
        if c:
            candidates.append(c)

    summarize_now = []
    archive_after_summary = []
    promote_to_core_candidates = []
    keep_hot = []
    review_manually = []

    for c in sorted(candidates, key=lambda x: (-float(x["candidate_score"]), x["path"])):
        cls = c["memory_class"]
        score = float(c["candidate_score"])
        if cls == "core":
            keep_hot.append(c)
            continue
        if c.get("promote_signals") and cls in {"working", "durable"}:
            promote_to_core_candidates.append(c)
            if score > 0.4:
                summarize_now.append(c)
            else:
                keep_hot.append(c)
            continue
        if cls == "ephemeral":
            if score >= 0.4:
                summarize_now.append(c)
                if score >= 0.9:
                    archive_after_summary.append(c)
            else:
                keep_hot.append(c)
        elif cls == "working":
            if score >= 0.8:
                summarize_now.append(c)
            elif score >= 0.4:
                review_manually.append(c)
            else:
                keep_hot.append(c)
        elif cls == "durable":
            if score >= 1.0:
                review_manually.append(c)
            else:
                keep_hot.append(c)
        else:
            review_manually.append(c)

    summary_paths = [x["path"] for x in summarize_now[:20]]
    promote_paths = [x["path"] for x in promote_to_core_candidates[:10]]
    archive_paths = [x["path"] for x in archive_after_summary[:20]]
    keep_hot_paths = [x["path"] for x in keep_hot[:20]]

    report_md_rel = f"memory/summaries/weekly/{report_id}.md"
    report_json_rel = f"memory/summaries/weekly/{report_id}.json"
    report_path = safe_path(settings.repo_root, report_md_rel)
    report_json_path = safe_path(settings.repo_root, report_json_rel)
    body = f"""---
id: {report_id}
type: compaction_report
created_at: {now.isoformat()}
source: {source_rel}
---

# Compaction Report

This endpoint is an **orchestrator/planner**, not an LLM summarizer. It proposes candidates and categories.

## Policy (class-aware decay + promotion)
- Inputs: age, size, namespace/class, declared importance, access count, access recency
- Classes: ephemeral / working / durable / core
- Core is kept hot; durable is rarely compacted; ephemeral decays fastest
- Promotion candidates can *increase* in importance over time (identity/relationship/decision facts)

## Summary counts
- Candidates scanned: {len(candidates)}
- summarize_now: {len(summarize_now)}
- archive_after_summary: {len(archive_after_summary)}
- promote_to_core_candidates: {len(promote_to_core_candidates)}
- keep_hot: {len(keep_hot)}
- review_manually: {len(review_manually)}

## Summarize now (top)
{chr(10).join(f"- `{p}`" for p in summary_paths) if summary_paths else "- None"}

## Promote to core candidates (top)
{chr(10).join(f"- `{p}`" for p in promote_paths) if promote_paths else "- None"}

## Archive after summary (top)
{chr(10).join(f"- `{p}`" for p in archive_paths) if archive_paths else "- None"}

## Keep hot (sample)
{chr(10).join(f"- `{p}`" for p in keep_hot_paths) if keep_hot_paths else "- None"}

## Operator note
{req.note or 'N/A'}
"""

    payload = {
        "id": report_id,
        "type": "compaction_report",
        "created_at": now.isoformat(),
        "source": source_rel,
        "planner_only": True,
        "compaction_semantics": {
            "summarizes_content": False,
            "expected_ai_action": "Read candidate lists, generate summaries, then POST /v1/write or /v1/append",
        },
        "indexing_note": {
            "incremental_index_default": "working_tree",
            "can_include_uncommitted_changes": True,
            "future_mode": ["working_tree", "head_commit"],
        },
        "policy": {
            "inputs": ["age_days", "size_bytes", "memory_class", "importance", "access_count", "last_access_days"],
            "classes": ["ephemeral", "working", "durable", "core"],
            "decay": {
                "ephemeral": "fast",
                "working": "slow-while-active, faster-after-inactive",
                "durable": "very_slow",
                "core": "no_age_decay_retrieval_only",
            },
            "promotion_principle": "some memories gain importance over time via reuse/identity/relationship signals",
        },
        "summary_counts": {
            "candidates_scanned": len(candidates),
            "summarize_now": len(summarize_now),
            "archive_after_summary": len(archive_after_summary),
            "promote_to_core_candidates": len(promote_to_core_candidates),
            "keep_hot": len(keep_hot),
            "review_manually": len(review_manually),
        },
        "actions": {
            "summarize_now": summarize_now[:20],
            "archive_after_summary": archive_after_summary[:20],
            "promote_to_core_candidates": promote_to_core_candidates[:15],
            "keep_hot": keep_hot[:20],
            "review_manually": review_manually[:20],
        },
        "operator_note": req.note,
    }

    write_text_file(report_path, body)
    write_text_file(report_json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    committed = []
    for rel in [report_md_rel, report_json_rel]:
        path = safe_path(settings.repo_root, rel)
        if try_commit_file(
            path=path,
            gm=gm,
            commit_message=f"memory: add compaction {report_id}",
        ):
            committed.append(rel)

    audit(auth, "compact_run", {"report_id": report_id, "source": source_rel, "candidates": len(candidates)})
    result = {
        "ok": True,
        "report_id": report_id,
        "paths": [report_md_rel, report_json_rel],
        "committed_files": committed,
        "latest_commit": gm.latest_commit(),
        "planner_only": True,
        "summary_counts": payload["summary_counts"],
    }
    if access_warnings:
        result["degraded"] = True
        result["warnings"] = access_warnings
    return result
