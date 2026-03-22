"""Registry lifecycle: externalize, prune, and stub management for current-state registries.

Implements the namespace-specific execution contract defined in issue #112.
Each registry family has its own maintenance pass logic; the shared substrate
(shard naming, stub creation, rollback) is defined in shared helpers at the
top of this module.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException

from app.timestamps import format_compact, format_iso, parse_iso as _parse_iso, iso_now as _iso_now

from app.git_safety import try_commit_paths
from app.lifecycle_warnings import make_error_detail, make_lock_error, make_warning
from app.segment_history.locking import (
    LockInfrastructureError,
    SegmentHistoryLockTimeout,
    segment_history_source_lock,
)
from app.storage import build_cold_gzip_bytes, safe_path, write_bytes_file, write_text_file

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

DELIVERY_STATE_REL = "messages/state/delivery_index.json"
DELIVERY_HISTORY_DIR_REL = "messages/state/history/delivery"
DELIVERY_STUB_DIR_REL = "messages/state/history/delivery/index"

NONCE_INDEX_REL = "messages/security/nonce_index.json"

PEERS_REGISTRY_REL = "peers/registry.json"
PEER_TRUST_HISTORY_DIR_REL = "peers/history/registry"
PEER_TRUST_STUB_DIR_REL = "peers/history/registry/index"

REPLICATION_STATE_REL = "peers/replication_state.json"
REPLICATION_STATE_HISTORY_DIR_REL = "peers/history/replication_state"
REPLICATION_STATE_STUB_DIR_REL = "peers/history/replication_state/index"

REPLICATION_TOMBSTONES_REL = "peers/replication_tombstones.json"
REPLICATION_TOMBSTONE_HISTORY_DIR_REL = "peers/history/replication_tombstones"
REPLICATION_TOMBSTONE_STUB_DIR_REL = "peers/history/replication_tombstones/index"

# Terminal delivery states per spec
_TERMINAL_DELIVERY_STATES = frozenset({"acked", "delivered", "dead_letter"})

# Per-family schema types for shards
_REGISTRY_SHARD_SCHEMA_TYPES_BY_FAMILY: dict[str, str] = {
    "delivery": "delivery_history_shard",
    "peer_trust": "peer_trust_history_shard",
    "replication_state": "replication_state_history_shard",
    "replication_tombstones": "replication_tombstone_shard",
}

# Per-family directory mappings for cold-store operations
_REGISTRY_HISTORY_DIRS_BY_FAMILY: dict[str, str] = {
    "delivery": DELIVERY_HISTORY_DIR_REL,
    "peer_trust": PEER_TRUST_HISTORY_DIR_REL,
    "replication_state": REPLICATION_STATE_HISTORY_DIR_REL,
    "replication_tombstones": REPLICATION_TOMBSTONE_HISTORY_DIR_REL,
}
_REGISTRY_STUB_DIRS_BY_FAMILY: dict[str, str] = {
    "delivery": DELIVERY_STUB_DIR_REL,
    "peer_trust": PEER_TRUST_STUB_DIR_REL,
    "replication_state": REPLICATION_STATE_STUB_DIR_REL,
    "replication_tombstones": REPLICATION_TOMBSTONE_STUB_DIR_REL,
}


# ---------------------------------------------------------------------------
# Shared shard naming
# ---------------------------------------------------------------------------

_shard_timestamp_str = format_compact


def _next_shard_id(family: str, cut_at: datetime, shard_dir: Path) -> str:
    """Allocate the next shard_id for a family + timestamp pair."""
    ts_str = _shard_timestamp_str(cut_at)
    prefix = f"{family}__{ts_str}__"
    existing_seqs: list[int] = []
    if shard_dir.exists() and shard_dir.is_dir():
        for child in shard_dir.iterdir():
            name = child.stem if child.suffix == ".json" else child.name
            if name.startswith(prefix):
                suffix = name[len(prefix):]
                try:
                    seq = int(suffix)
                    existing_seqs.append(seq)
                except ValueError:
                    _logger.warning("Malformed shard sequence suffix in %s", child.name)
    next_seq = max(existing_seqs, default=0) + 1
    if next_seq < 1:
        next_seq = 1
    return f"{prefix}{next_seq:04d}"


# ---------------------------------------------------------------------------
# Shared stub creation
# ---------------------------------------------------------------------------

def _create_stub(
    *,
    family: str,
    shard_id: str,
    payload_path: str,
    created_at: datetime,
    source_head_path: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    """Build a registry_history_stub per the shared contract."""
    return {
        "schema_type": "registry_history_stub",
        "schema_version": "1.0",
        "family": family,
        "shard_id": shard_id,
        "payload_path": payload_path,
        "created_at": format_iso(created_at),
        "source_head_path": source_head_path,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Shared rollback helpers
# ---------------------------------------------------------------------------

def _capture_rollback(paths: list[Path]) -> list[tuple[Path, bytes | None]]:
    """Capture prior bytes for rollback."""
    plan: list[tuple[Path, bytes | None]] = []
    seen: set[Path] = set()
    for p in paths:
        resolved = p.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        plan.append((p, p.read_bytes() if p.exists() else None))
    return plan


def _restore_rollback(plan: list[tuple[Path, bytes | None]]) -> None:
    """Best-effort restore from rollback plan."""
    for path, old_bytes in plan:
        try:
            if old_bytes is None:
                path.unlink(missing_ok=True)
            else:
                write_bytes_file(path, old_bytes)
        except Exception:
            _logger.exception("Rollback restore failed for %s", path)


# ---------------------------------------------------------------------------
# Shared JSON I/O with graceful degradation
# ---------------------------------------------------------------------------

def _load_json_head(path: Path, empty_default: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Load a JSON head file with graceful degradation. Returns (data, warnings)."""
    warnings: list[str] = []
    if not path.exists():
        return dict(empty_default), warnings
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        _logger.warning("Corrupt registry head at %s; returning empty", path)
        warnings.append(f"registry_head_corrupt:{path.name}")
        return dict(empty_default), warnings
    except Exception:
        _logger.error("Unreadable registry head at %s", path, exc_info=True)
        warnings.append(f"registry_head_unreadable:{path.name}")
        return dict(empty_default), warnings
    if not isinstance(data, dict):
        warnings.append(f"registry_head_not_dict:{path.name}")
        return dict(empty_default), warnings
    return data, warnings


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON data to path atomically."""
    write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))


def _write_json_exclusive(path: Path, data: dict[str, Any]) -> None:
    """Write JSON data to path, failing if the file already exists.

    Uses ``O_CREAT | O_EXCL`` to claim the filename atomically, then
    delegates to ``write_text_file`` for durable content (temp+fsync+rename).
    If the durable write fails, the empty sentinel is removed so a future
    retry can reclaim the sequence.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    os.close(fd)
    try:
        write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        path.unlink(missing_ok=True)
        raise


_SHARD_WRITE_MAX_RETRIES = 3


def _write_shard_pair_exclusive(
    *,
    family: str,
    cut_at: datetime,
    shard_dir: Path,
    stub_dir: Path,
    shard_dir_rel: str,
    stub_dir_rel: str,
    shard_payload: dict[str, Any],
    stub_payload: dict[str, Any],
) -> tuple[str, str, str]:
    """Allocate a shard ID and write shard+stub atomically with retry on collision.

    Returns ``(shard_id, shard_rel, stub_rel)`` on success.
    Raises ``RuntimeError`` if all retries are exhausted.
    """
    shard_dir.mkdir(parents=True, exist_ok=True)
    stub_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(_SHARD_WRITE_MAX_RETRIES):
        shard_id = _next_shard_id(family, cut_at, shard_dir)
        shard_rel = f"{shard_dir_rel}/{shard_id}.json"
        stub_rel = f"{stub_dir_rel}/{shard_id}.json"
        shard_path = shard_dir / f"{shard_id}.json"
        stub_path = stub_dir / f"{shard_id}.json"

        # Update shard_id in payloads (they reference their own ID)
        shard_payload["shard_id"] = shard_id
        stub_payload["shard_id"] = shard_id
        stub_payload["payload_path"] = shard_rel

        try:
            _write_json_exclusive(shard_path, shard_payload)
        except FileExistsError:
            if attempt == _SHARD_WRITE_MAX_RETRIES - 1:
                raise RuntimeError(f"Shard ID collision exhausted {_SHARD_WRITE_MAX_RETRIES} retries for {family}")
            _logger.debug("Shard ID collision on %s (attempt %d), retrying", shard_id, attempt + 1)
            continue

        try:
            _write_json_exclusive(stub_path, stub_payload)
        except FileExistsError:
            shard_path.unlink(missing_ok=True)
            if attempt == _SHARD_WRITE_MAX_RETRIES - 1:
                raise RuntimeError(f"Stub ID collision exhausted {_SHARD_WRITE_MAX_RETRIES} retries for {family}")
            _logger.debug("Stub ID collision on %s (attempt %d), retrying", shard_id, attempt + 1)
            continue
        except Exception:
            shard_path.unlink(missing_ok=True)
            raise

        return shard_id, shard_rel, stub_rel

    raise RuntimeError(f"Shard write exhausted {_SHARD_WRITE_MAX_RETRIES} retries for {family}")


# ---------------------------------------------------------------------------
# Effective delivery status (mirrors messages/service.py logic)
# ---------------------------------------------------------------------------

def _effective_delivery_status(record: dict[str, Any], now: datetime) -> str:
    """Return the effective delivery status for a record at the given time."""
    status = str(record.get("status") or "pending_ack")
    if status != "pending_ack":
        return status
    ack_deadline = _parse_iso(record.get("ack_deadline"))
    if ack_deadline and now > ack_deadline:
        return "dead_letter"
    return status


# ---------------------------------------------------------------------------
# Delivery retention timestamp derivation (per spec table)
# ---------------------------------------------------------------------------

def _delivery_retention_timestamp(record: dict[str, Any], effective_status: str) -> tuple[datetime | None, str | None]:
    """Derive the retention timestamp and its kind from a delivery record.

    Returns (timestamp, warning_or_none). If the timestamp cannot be derived,
    returns (None, warning_string).
    """
    acks = record.get("acks")
    if not isinstance(acks, list):
        acks = []

    def _latest_ack_at(status_filter: str | None = None) -> datetime | None:
        candidates: list[datetime] = []
        for ack in acks:
            if not isinstance(ack, dict):
                continue
            if status_filter and str(ack.get("status") or "") != status_filter:
                continue
            dt = _parse_iso(ack.get("ack_at"))
            if dt is not None:
                candidates.append(dt)
        return max(candidates) if candidates else None

    if effective_status == "acked":
        ts = _latest_ack_at("accepted")
        if ts is None:
            return None, "delivery_retention_missing:acked record has no parseable accepted ack_at"
        return ts, None

    if effective_status == "dead_letter":
        stored_status = str(record.get("status") or "")
        if stored_status == "pending_ack":
            # Effective dead-letter because ack_deadline passed
            dl = _parse_iso(record.get("ack_deadline"))
            if dl is None:
                return None, "delivery_retention_missing:effective dead_letter has no parseable ack_deadline"
            return dl, None
        # Stored dead_letter
        ts = _latest_ack_at("rejected")
        if ts is not None:
            return ts, None
        # Fallback: any parseable ack_at
        ts = _latest_ack_at()
        if ts is not None:
            return ts, None
        return None, "delivery_retention_missing:dead_letter record has no parseable ack timestamps"

    if effective_status == "delivered":
        ts = _parse_iso(record.get("sent_at"))
        if ts is None:
            return None, "delivery_retention_missing:delivered record has no parseable sent_at"
        return ts, None

    return None, f"delivery_retention_unknown_status:{effective_status}"


# ===================================================================
# FAMILY: delivery
# ===================================================================

def delivery_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    terminal_retention_days: int,
    idempotency_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for delivery_index.json.

    Returns a result dict with ok, warnings, shard_id (if created), and counts.
    """
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock("registry:delivery_index", lock_dir=lock_dir)
    except LockInfrastructureError:
        return {"ok": False, "family": "delivery", "warnings": [make_warning("registry_head_lock_unavailable", "Lock infrastructure unavailable for delivery_index", path="delivery_index")]}
    with lock_ctx:
        return _delivery_maintenance_pass_locked(
            repo_root=repo_root, now=now,
            terminal_retention_days=terminal_retention_days,
            idempotency_retention_days=idempotency_retention_days,
            batch_limit=batch_limit,
        )


def _delivery_maintenance_pass_locked(
    *,
    repo_root: Path,
    now: datetime,
    terminal_retention_days: int,
    idempotency_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Inner implementation of delivery_maintenance_pass, called under head-file lock."""
    warnings: list[str] = []
    head_path = safe_path(repo_root, DELIVERY_STATE_REL)
    head, load_warnings = _load_json_head(
        head_path,
        {"version": "1", "records": {}, "idempotency": {}},
    )
    warnings.extend(load_warnings)

    records = head.get("records", {})
    if not isinstance(records, dict):
        records = {}
    idempotency = head.get("idempotency", {})
    if not isinstance(idempotency, dict):
        idempotency = {}

    cutoff = now - timedelta(days=terminal_retention_days)
    idem_cutoff = now - timedelta(days=idempotency_retention_days)

    # --- Select terminal records eligible for externalization ---
    eligible_records: list[tuple[str, dict[str, Any], datetime]] = []
    for msg_id in sorted(records.keys()):
        row = records[msg_id]
        if not isinstance(row, dict):
            continue
        eff = _effective_delivery_status(row, now)
        if eff not in _TERMINAL_DELIVERY_STATES:
            continue
        ret_ts, warn = _delivery_retention_timestamp(row, eff)
        if warn:
            warnings.append(warn)
        if ret_ts is None:
            continue
        if ret_ts > cutoff:
            continue
        eligible_records.append((msg_id, row, ret_ts))
        if len(eligible_records) >= batch_limit:
            break

    # --- Select idempotency mappings to prune ---
    idem_to_externalize: dict[str, str] = {}  # keys co-externalized with terminal rows
    idem_to_prune: list[str] = []  # keys pruned independently
    externalized_msg_ids = {msg_id for msg_id, _, _ in eligible_records}
    remaining_budget = batch_limit - len(eligible_records)

    for idem_key in sorted(idempotency.keys()):
        target_id = idempotency[idem_key]
        # Co-externalize with terminal row
        if target_id in externalized_msg_ids:
            idem_to_externalize[idem_key] = target_id
            continue
        # Orphan check: target absent from head and not being externalized
        if target_id not in records:
            if remaining_budget > 0:
                idem_to_prune.append(idem_key)
                remaining_budget -= 1
                warnings.append(f"delivery_idempotency_orphan_pruned:{idem_key}")
            continue
        # Age-based prune: target still hot but idempotency mapping is old
        target_row = records.get(target_id)
        if isinstance(target_row, dict):
            sent_at = _parse_iso(target_row.get("sent_at"))
            if sent_at and sent_at < idem_cutoff and remaining_budget > 0:
                idem_to_prune.append(idem_key)
                remaining_budget -= 1

    if not eligible_records and not idem_to_prune:
        return {
            "ok": True,
            "family": "delivery",
            "records_externalized": 0,
            "idempotency_externalized": 0,
            "idempotency_pruned": 0,
            "shard_id": None,
            "written_paths": [],
            "warnings": warnings,
        }

    # --- Build shard if there are records to externalize ---
    shard_id = None
    shard_rel = None
    stub_rel = None
    cut_records: dict[str, Any] = {}
    cut_idempotency: dict[str, str] = {}

    if eligible_records:
        for msg_id, row, _ in eligible_records:
            cut_records[msg_id] = row
        cut_idempotency = dict(idem_to_externalize)

        # Build summary
        status_counts = {"acked": 0, "delivered": 0, "dead_letter": 0}
        retention_timestamps: list[datetime] = []
        for msg_id, row, ret_ts in eligible_records:
            eff = _effective_delivery_status(row, now)
            if eff in status_counts:
                status_counts[eff] += 1
            retention_timestamps.append(ret_ts)

        summary = {
            "record_count": len(cut_records),
            "idempotency_count": len(cut_idempotency),
            "effective_status_counts": status_counts,
            "oldest_retention_timestamp": format_iso(min(retention_timestamps)) if retention_timestamps else None,
            "newest_retention_timestamp": format_iso(max(retention_timestamps)) if retention_timestamps else None,
        }

        shard_payload = {
            "schema_type": "delivery_history_shard",
            "schema_version": "1.0",
            "shard_id": "",
            "source_head_path": DELIVERY_STATE_REL,
            "cut_at": format_iso(now),
            "records": cut_records,
            "idempotency": cut_idempotency,
            "summary": summary,
        }

        stub_payload = _create_stub(
            family="delivery",
            shard_id="",
            payload_path="",
            created_at=now,
            source_head_path=DELIVERY_STATE_REL,
            summary=summary,
        )

        shard_id, shard_rel, stub_rel = _write_shard_pair_exclusive(
            family="delivery",
            cut_at=now,
            shard_dir=safe_path(repo_root, DELIVERY_HISTORY_DIR_REL),
            stub_dir=safe_path(repo_root, DELIVERY_STUB_DIR_REL),
            shard_dir_rel=DELIVERY_HISTORY_DIR_REL,
            stub_dir_rel=DELIVERY_STUB_DIR_REL,
            shard_payload=shard_payload,
            stub_payload=stub_payload,
        )

    # --- Apply changes to head ---
    # Remove externalized records
    for msg_id in cut_records:
        records.pop(msg_id, None)
    # Remove co-externalized idempotency keys
    for idem_key in idem_to_externalize:
        idempotency.pop(idem_key, None)
    # Remove independently pruned idempotency keys
    for idem_key in idem_to_prune:
        idempotency.pop(idem_key, None)

    # Update history_meta
    history_meta = head.setdefault("history_meta", {})
    if not isinstance(history_meta, dict):
        history_meta = {}
        head["history_meta"] = history_meta
    dm = history_meta.setdefault("delivery", {})
    if not isinstance(dm, dict):
        dm = {}
        history_meta["delivery"] = dm
    if eligible_records:
        dm["last_cut_at"] = format_iso(now)
        dm["last_cut_record_count"] = len(cut_records)
        dm["last_cut_effective_status_counts"] = summary["effective_status_counts"]  # type: ignore[possibly-undefined]
    dm["hot_record_count"] = len(records)
    dm["hot_idempotency_count"] = len(idempotency)

    head["records"] = records
    head["idempotency"] = idempotency

    # --- Write head with rollback; shard+stub already written exclusively ---
    written_rels: list[str] = []
    rollback_plan: list[tuple[Path, bytes | None]] = []
    if shard_rel:
        rollback_plan.append((safe_path(repo_root, shard_rel), None))
        written_rels.append(shard_rel)
    if stub_rel:
        rollback_plan.append((safe_path(repo_root, stub_rel), None))
        written_rels.append(stub_rel)
    rollback_plan.extend(_capture_rollback([head_path]))
    written_rels.append(DELIVERY_STATE_REL)

    try:
        _write_json(head_path, head)
    except Exception:
        _restore_rollback(rollback_plan)
        raise

    return {
        "ok": True,
        "family": "delivery",
        "records_externalized": len(cut_records),
        "idempotency_externalized": len(cut_idempotency),
        "idempotency_pruned": len(idem_to_prune),
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
        "written_paths": written_rels,
        "warnings": warnings,
        "audit_events": [{"event": "registry_delivery_maintenance", "detail": {
            "family": "delivery",
            "records_externalized": len(cut_records),
            "idempotency_externalized": len(cut_idempotency),
            "idempotency_pruned": len(idem_to_prune),
            "shard_id": shard_id,
        }}],
    }


# ===================================================================
# FAMILY: nonce
# ===================================================================

def nonce_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    nonce_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for nonce_index.json (prune-only)."""
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock("registry:nonce_index", lock_dir=lock_dir)
    except LockInfrastructureError:
        return {"ok": False, "family": "nonce", "warnings": [make_warning("registry_head_lock_unavailable", "Lock infrastructure unavailable for nonce_index", path="nonce_index")]}
    with lock_ctx:
        return _nonce_maintenance_pass_locked(
            repo_root=repo_root, now=now,
            nonce_retention_days=nonce_retention_days, batch_limit=batch_limit,
        )


def _nonce_maintenance_pass_locked(
    *,
    repo_root: Path,
    now: datetime,
    nonce_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Inner implementation of nonce_maintenance_pass, called under head-file lock."""
    warnings: list[str] = []
    head_path = safe_path(repo_root, NONCE_INDEX_REL)
    head, load_warnings = _load_json_head(
        head_path,
        {"schema_version": "1.0", "entries": {}},
    )
    warnings.extend(load_warnings)

    entries = head.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}

    fallback_cutoff = now - timedelta(days=nonce_retention_days)
    prune_keys: list[str] = []

    for key in sorted(entries.keys()):
        if len(prune_keys) >= batch_limit:
            break
        row = entries[key]
        if not isinstance(row, dict):
            prune_keys.append(key)
            warnings.append(f"nonce_malformed_pruned:{key}")
            continue

        expires_at = _parse_iso(row.get("expires_at"))
        if expires_at is not None:
            if now > expires_at:
                prune_keys.append(key)
            continue

        # Missing or unparsable expires_at
        first_seen = _parse_iso(row.get("first_seen_at"))
        if first_seen is not None:
            if first_seen < fallback_cutoff:
                prune_keys.append(key)
                warnings.append(f"nonce_no_expiry_pruned:{key}")
        else:
            # Missing both expires_at and first_seen_at
            prune_keys.append(key)
            warnings.append(f"nonce_malformed_pruned:{key}")

    if not prune_keys:
        return {
            "ok": True,
            "family": "nonce",
            "pruned": 0,
            "written_paths": [],
            "warnings": warnings,
        }

    for key in prune_keys:
        entries.pop(key, None)

    # Update history_meta
    history_meta = head.setdefault("history_meta", {})
    if not isinstance(history_meta, dict):
        history_meta = {}
        head["history_meta"] = history_meta
    nm = history_meta.setdefault("nonce", {})
    if not isinstance(nm, dict):
        nm = {}
        history_meta["nonce"] = nm
    nm["last_pruned_at"] = format_iso(now)
    nm["last_pruned_count"] = len(prune_keys)
    nm["hot_entry_count"] = len(entries)

    head["entries"] = entries

    rollback = _capture_rollback([head_path])
    try:
        _write_json(head_path, head)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "nonce",
        "pruned": len(prune_keys),
        "written_paths": [NONCE_INDEX_REL],
        "warnings": warnings,
        "audit_events": [{"event": "registry_nonce_maintenance", "detail": {
            "family": "nonce", "pruned": len(prune_keys),
        }}],
    }


# ===================================================================
# FAMILY: peer trust history
# ===================================================================

def peer_trust_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    max_hot_entries: int,
    hot_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for peers/registry.json trust history."""
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock("registry:peers_registry", lock_dir=lock_dir)
    except LockInfrastructureError:
        return {"ok": False, "family": "peer_trust", "warnings": [make_warning("registry_head_lock_unavailable", "Lock infrastructure unavailable for peers_registry", path="peers_registry")]}
    with lock_ctx:
        return _peer_trust_maintenance_pass_locked(
            repo_root=repo_root, now=now,
            max_hot_entries=max_hot_entries, hot_retention_days=hot_retention_days,
            batch_limit=batch_limit,
        )


def _peer_trust_maintenance_pass_locked(
    *,
    repo_root: Path,
    now: datetime,
    max_hot_entries: int,
    hot_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Inner implementation of peer_trust_maintenance_pass, called under head-file lock."""
    warnings: list[str] = []
    head_path = safe_path(repo_root, PEERS_REGISTRY_REL)
    head, load_warnings = _load_json_head(
        head_path,
        {"schema_version": "1.0", "updated_at": None, "peers": {}},
    )
    warnings.extend(load_warnings)

    peers = head.get("peers", {})
    if not isinstance(peers, dict):
        peers = {}

    age_cutoff = now - timedelta(days=hot_retention_days)
    total_externalized = 0
    shard_results: list[dict[str, Any]] = []

    # Process peers in sorted order
    for peer_id in sorted(peers.keys()):
        if total_externalized >= batch_limit:
            break
        row = peers[peer_id]
        if not isinstance(row, dict):
            continue
        history = row.get("trust_history")
        if not isinstance(history, list) or len(history) <= max_hot_entries:
            continue

        # Find transitions eligible for cut: older than age cutoff
        # and beyond the newest max_hot_entries
        keep_tail = history[-max_hot_entries:]  # always stay hot
        candidate_prefix = history[:-max_hot_entries]

        eligible: list[dict[str, Any]] = []
        remaining_prefix: list[dict[str, Any]] = []
        budget = batch_limit - total_externalized
        for transition in candidate_prefix:
            if not isinstance(transition, dict):
                continue
            at = _parse_iso(transition.get("at"))
            if at is not None and at < age_cutoff and len(eligible) < budget:
                eligible.append(transition)
            else:
                remaining_prefix.append(transition)

        if not eligible:
            continue

        # Build shard
        transition_timestamps: list[datetime] = [dt for t in eligible if (dt := _parse_iso(t.get("at"))) is not None]
        final_trust_after = str(row.get("trust_level") or "untrusted")

        summary = {
            "transition_count": len(eligible),
            "oldest_transition_at": format_iso(min(transition_timestamps)) if transition_timestamps else None,
            "newest_transition_at": format_iso(max(transition_timestamps)) if transition_timestamps else None,
            "final_hot_trust_level_after_cut": final_trust_after,
        }

        shard_payload = {
            "schema_type": "peer_trust_history_shard",
            "schema_version": "1.0",
            "shard_id": "",
            "source_head_path": PEERS_REGISTRY_REL,
            "peer_id": peer_id,
            "cut_at": format_iso(now),
            "transitions": eligible,
            "summary": summary,
        }

        stub_summary = dict(summary)
        stub_summary["peer_id"] = peer_id

        stub_payload = _create_stub(
            family="peer_trust",
            shard_id="",
            payload_path="",
            created_at=now,
            source_head_path=PEERS_REGISTRY_REL,
            summary=stub_summary,
        )

        shard_id, shard_rel, stub_rel = _write_shard_pair_exclusive(
            family="peer_trust",
            cut_at=now,
            shard_dir=safe_path(repo_root, PEER_TRUST_HISTORY_DIR_REL),
            stub_dir=safe_path(repo_root, PEER_TRUST_STUB_DIR_REL),
            shard_dir_rel=PEER_TRUST_HISTORY_DIR_REL,
            stub_dir_rel=PEER_TRUST_STUB_DIR_REL,
            shard_payload=shard_payload,
            stub_payload=stub_payload,
        )

        # Update peer row: remaining prefix + keep tail
        row["trust_history"] = remaining_prefix + keep_tail
        total_externalized += len(eligible)

        shard_results.append({
            "peer_id": peer_id,
            "shard_id": shard_id,
            "shard_rel": shard_rel,
            "stub_rel": stub_rel,
            "transition_count": len(eligible),
        })

    if not shard_results:
        return {
            "ok": True,
            "family": "peer_trust",
            "transitions_externalized": 0,
            "shards_created": 0,
            "written_paths": [],
            "warnings": warnings,
        }

    # Update history_meta
    history_meta = head.setdefault("history_meta", {})
    if not isinstance(history_meta, dict):
        history_meta = {}
        head["history_meta"] = history_meta
    pr_meta = history_meta.setdefault("peer_registry", {})
    if not isinstance(pr_meta, dict):
        pr_meta = {}
        history_meta["peer_registry"] = pr_meta

    last_result = shard_results[-1]
    pr_meta["last_cut_at"] = format_iso(now)
    pr_meta["last_cut_peer_id"] = last_result["peer_id"]
    pr_meta["last_cut_transition_count"] = total_externalized
    pr_meta["hot_peer_count"] = len(peers)

    # Update total_externalized_transition_count
    prev_total = pr_meta.get("total_externalized_transition_count", 0)
    if not isinstance(prev_total, int):
        prev_total = 0
    pr_meta["total_externalized_transition_count"] = prev_total + total_externalized

    # Per-peer metadata
    by_peer = pr_meta.setdefault("by_peer", {})
    if not isinstance(by_peer, dict):
        by_peer = {}
        pr_meta["by_peer"] = by_peer
    for sr in shard_results:
        pid = sr["peer_id"]
        pm = by_peer.setdefault(pid, {})
        if not isinstance(pm, dict):
            pm = {}
            by_peer[pid] = pm
        prev_ext = pm.get("trust_history_externalized_count", 0)
        if not isinstance(prev_ext, int):
            prev_ext = 0
        pm["trust_history_externalized_count"] = prev_ext + sr["transition_count"]
        pm["last_trust_cut_at"] = format_iso(now)
        # Compute total count
        peer_row = peers.get(pid, {})
        hot_count = len(peer_row.get("trust_history", [])) if isinstance(peer_row, dict) else 0
        pm["trust_history_total_count"] = hot_count + pm["trust_history_externalized_count"]

    head["peers"] = peers

    # --- Write head with rollback; shards+stubs already written exclusively ---
    written_rels: list[str] = []
    rollback_plan: list[tuple[Path, bytes | None]] = []
    for sr in shard_results:
        rollback_plan.append((safe_path(repo_root, sr["shard_rel"]), None))
        rollback_plan.append((safe_path(repo_root, sr["stub_rel"]), None))
        written_rels.extend([sr["shard_rel"], sr["stub_rel"]])
    rollback_plan.extend(_capture_rollback([head_path]))
    written_rels.append(PEERS_REGISTRY_REL)

    try:
        _write_json(head_path, head)
    except Exception:
        _restore_rollback(rollback_plan)
        raise

    return {
        "ok": True,
        "family": "peer_trust",
        "transitions_externalized": total_externalized,
        "shards_created": len(shard_results),
        "shards": [{"peer_id": sr["peer_id"], "shard_id": sr["shard_id"], "transition_count": sr["transition_count"]} for sr in shard_results],
        "written_paths": written_rels,
        "warnings": warnings,
        "audit_events": [{"event": "registry_peer_trust_maintenance", "detail": {
            "family": "peer_trust",
            "transitions_externalized": total_externalized,
            "shards_created": len(shard_results),
        }}],
    }


# ===================================================================
# FAMILY: replication state (synchronous pre-write capture)
# ===================================================================

def externalize_superseded_push(
    *,
    repo_root: Path,
    now: datetime,
    previous_row: dict[str, Any],
    hot_retention_days: int,
) -> dict[str, Any] | None:
    """Externalize a superseded last_push row during replacement.

    Called synchronously during the mutating write. Returns the shard result
    dict if a shard was created, or None if the row was within the hot window.
    """
    pushed_at = _parse_iso(previous_row.get("pushed_at"))
    if pushed_at is None:
        _logger.warning("Superseded last_push has no parseable pushed_at; not externalizing")
        return None
    cutoff = now - timedelta(days=hot_retention_days)
    if pushed_at > cutoff:
        return None  # Still within hot window

    summary = {
        "push_event_count": 1,
        "pull_event_count": 0,
        "oldest_event_at": format_iso(pushed_at),
        "newest_event_at": format_iso(pushed_at),
    }

    shard_payload = {
        "schema_type": "replication_state_history_shard",
        "schema_version": "1.0",
        "shard_id": "",
        "source_head_path": REPLICATION_STATE_REL,
        "cut_at": format_iso(now),
        "push_events": [{"superseded_at": format_iso(now), "row": previous_row}],
        "pull_events": [],
        "summary": summary,
    }

    stub_payload = _create_stub(
        family="replication_state",
        shard_id="",
        payload_path="",
        created_at=now,
        source_head_path=REPLICATION_STATE_REL,
        summary=summary,
    )

    shard_id, shard_rel, stub_rel = _write_shard_pair_exclusive(
        family="replication_state",
        cut_at=now,
        shard_dir=safe_path(repo_root, REPLICATION_STATE_HISTORY_DIR_REL),
        stub_dir=safe_path(repo_root, REPLICATION_STATE_STUB_DIR_REL),
        shard_dir_rel=REPLICATION_STATE_HISTORY_DIR_REL,
        stub_dir_rel=REPLICATION_STATE_STUB_DIR_REL,
        shard_payload=shard_payload,
        stub_payload=stub_payload,
    )

    return {
        "ok": True,
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
        "warnings": [],
    }


def externalize_superseded_pull(
    *,
    repo_root: Path,
    now: datetime,
    source_peer: str,
    previous_row: dict[str, Any],
    hot_retention_days: int,
) -> dict[str, Any] | None:
    """Externalize a superseded last_pull_by_source row during replacement."""
    pulled_at = _parse_iso(previous_row.get("pulled_at"))
    if pulled_at is None:
        _logger.warning("Superseded last_pull for %s has no parseable pulled_at; not externalizing", source_peer)
        return None
    cutoff = now - timedelta(days=hot_retention_days)
    if pulled_at > cutoff:
        return None

    summary = {
        "push_event_count": 0,
        "pull_event_count": 1,
        "oldest_event_at": format_iso(pulled_at),
        "newest_event_at": format_iso(pulled_at),
    }

    shard_payload = {
        "schema_type": "replication_state_history_shard",
        "schema_version": "1.0",
        "shard_id": "",
        "source_head_path": REPLICATION_STATE_REL,
        "cut_at": format_iso(now),
        "push_events": [],
        "pull_events": [{"source_peer": source_peer, "superseded_at": format_iso(now), "row": previous_row}],
        "summary": summary,
    }

    stub_payload = _create_stub(
        family="replication_state",
        shard_id="",
        payload_path="",
        created_at=now,
        source_head_path=REPLICATION_STATE_REL,
        summary=summary,
    )

    shard_id, shard_rel, stub_rel = _write_shard_pair_exclusive(
        family="replication_state",
        cut_at=now,
        shard_dir=safe_path(repo_root, REPLICATION_STATE_HISTORY_DIR_REL),
        stub_dir=safe_path(repo_root, REPLICATION_STATE_STUB_DIR_REL),
        shard_dir_rel=REPLICATION_STATE_HISTORY_DIR_REL,
        stub_dir_rel=REPLICATION_STATE_STUB_DIR_REL,
        shard_payload=shard_payload,
        stub_payload=stub_payload,
    )

    return {
        "ok": True,
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
        "warnings": [],
    }


def replication_state_prune_idempotency(
    *,
    repo_root: Path,
    now: datetime,
    pull_idempotency_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Prune expired pull_idempotency entries from replication_state.json."""
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock("registry:replication_state", lock_dir=lock_dir)
    except LockInfrastructureError:
        return {
            "ok": False, "family": "replication_state",
            "warnings": [make_warning(
                "registry_head_lock_unavailable",
                "Lock infrastructure unavailable for replication_state",
                path="replication_state",
            )],
        }
    with lock_ctx:
        return _replication_state_prune_idempotency_locked(
            repo_root=repo_root, now=now,
            pull_idempotency_retention_days=pull_idempotency_retention_days,
            batch_limit=batch_limit,
        )


def _replication_state_prune_idempotency_locked(
    *,
    repo_root: Path,
    now: datetime,
    pull_idempotency_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Inner implementation, called under head-file lock."""
    warnings: list[str] = []
    head_path = safe_path(repo_root, REPLICATION_STATE_REL)
    head, load_warnings = _load_json_head(
        head_path,
        {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}},
    )
    warnings.extend(load_warnings)

    pull_idem = head.get("pull_idempotency", {})
    if not isinstance(pull_idem, dict):
        pull_idem = {}

    cutoff = now - timedelta(days=pull_idempotency_retention_days)
    prune_keys: list[str] = []

    for key in sorted(pull_idem.keys()):
        if len(prune_keys) >= batch_limit:
            break
        row = pull_idem[key]
        if not isinstance(row, dict):
            prune_keys.append(key)
            warnings.append(f"replication_pull_idempotency_malformed:{key}")
            continue
        at = _parse_iso(row.get("at"))
        if at is None:
            prune_keys.append(key)
            warnings.append(f"replication_pull_idempotency_malformed:{key}")
            continue
        if at < cutoff:
            prune_keys.append(key)

    if not prune_keys:
        return {"ok": True, "family": "replication_state", "pruned": 0, "written_paths": [], "warnings": warnings}

    for key in prune_keys:
        pull_idem.pop(key, None)
    head["pull_idempotency"] = pull_idem

    # Update history_meta
    history_meta = head.setdefault("history_meta", {})
    if not isinstance(history_meta, dict):
        history_meta = {}
        head["history_meta"] = history_meta
    rs_meta = history_meta.setdefault("replication_state", {})
    if not isinstance(rs_meta, dict):
        rs_meta = {}
        history_meta["replication_state"] = rs_meta

    pulls = head.get("last_pull_by_source", {})
    rs_meta["hot_pull_source_count"] = len(pulls) if isinstance(pulls, dict) else 0
    rs_meta["hot_pull_idempotency_count"] = len(pull_idem)

    rollback = _capture_rollback([head_path])
    try:
        _write_json(head_path, head)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "replication_state",
        "pruned": len(prune_keys),
        "written_paths": [REPLICATION_STATE_REL],
        "warnings": warnings,
        "audit_events": [{"event": "registry_replication_state_maintenance", "detail": {
            "family": "replication_state", "pruned": len(prune_keys),
        }}],
    }


# ===================================================================
# FAMILY: replication tombstones
# ===================================================================

def tombstone_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    grace_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for replication_tombstones.json."""
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock("registry:replication_tombstones", lock_dir=lock_dir)
    except LockInfrastructureError:
        return {
            "ok": False, "family": "replication_tombstones",
            "warnings": [make_warning(
                "registry_head_lock_unavailable",
                "Lock infrastructure unavailable for replication_tombstones",
                path="replication_tombstones",
            )],
        }
    with lock_ctx:
        return _tombstone_maintenance_pass_locked(
            repo_root=repo_root, now=now, grace_days=grace_days, batch_limit=batch_limit,
        )


def _tombstone_maintenance_pass_locked(
    *,
    repo_root: Path,
    now: datetime,
    grace_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Inner implementation, called under head-file lock."""
    warnings: list[str] = []
    head_path = safe_path(repo_root, REPLICATION_TOMBSTONES_REL)
    head, load_warnings = _load_json_head(
        head_path,
        {"schema_version": "1.0", "entries": {}},
    )
    warnings.extend(load_warnings)

    entries = head.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}

    grace_cutoff = now - timedelta(days=grace_days)

    # Select tombstones older than the grace window
    eligible: list[tuple[str, dict[str, Any], datetime]] = []
    for path_key in sorted(entries.keys()):
        if len(eligible) >= batch_limit:
            break
        row = entries[path_key]
        if not isinstance(row, dict):
            continue
        ts = _parse_iso(row.get("tombstone_at"))
        if ts is None:
            continue
        if ts < grace_cutoff:
            eligible.append((path_key, row, ts))

    if not eligible:
        return {
            "ok": True,
            "family": "replication_tombstones",
            "entries_externalized": 0,
            "shard_id": None,
            "written_paths": [],
            "warnings": warnings,
        }

    # Build shard
    cut_entries: dict[str, Any] = {}
    tombstone_timestamps: list[datetime] = []
    for path_key, row, ts in eligible:
        cut_entries[path_key] = row
        tombstone_timestamps.append(ts)

    summary = {
        "entry_count": len(cut_entries),
        "oldest_tombstone_at": format_iso(min(tombstone_timestamps)) if tombstone_timestamps else None,
        "newest_tombstone_at": format_iso(max(tombstone_timestamps)) if tombstone_timestamps else None,
    }

    shard_payload = {
        "schema_type": "replication_tombstone_shard",
        "schema_version": "1.0",
        "shard_id": "",
        "source_head_path": REPLICATION_TOMBSTONES_REL,
        "cut_at": format_iso(now),
        "entries": cut_entries,
        "summary": summary,
    }

    stub_payload = _create_stub(
        family="replication_tombstone",
        shard_id="",
        payload_path="",
        created_at=now,
        source_head_path=REPLICATION_TOMBSTONES_REL,
        summary=summary,
    )

    shard_id, shard_rel, stub_rel = _write_shard_pair_exclusive(
        family="replication_tombstone",
        cut_at=now,
        shard_dir=safe_path(repo_root, REPLICATION_TOMBSTONE_HISTORY_DIR_REL),
        stub_dir=safe_path(repo_root, REPLICATION_TOMBSTONE_STUB_DIR_REL),
        shard_dir_rel=REPLICATION_TOMBSTONE_HISTORY_DIR_REL,
        stub_dir_rel=REPLICATION_TOMBSTONE_STUB_DIR_REL,
        shard_payload=shard_payload,
        stub_payload=stub_payload,
    )

    # Remove from head
    for path_key in cut_entries:
        entries.pop(path_key, None)

    # Update history_meta
    history_meta = head.setdefault("history_meta", {})
    if not isinstance(history_meta, dict):
        history_meta = {}
        head["history_meta"] = history_meta
    tm = history_meta.setdefault("replication_tombstones", {})
    if not isinstance(tm, dict):
        tm = {}
        history_meta["replication_tombstones"] = tm
    tm["last_cut_at"] = format_iso(now)
    tm["last_cut_entry_count"] = len(cut_entries)
    tm["hot_entry_count"] = len(entries)

    # Compute oldest_hot_tombstone_at
    oldest_hot: datetime | None = None
    for row in entries.values():
        if isinstance(row, dict):
            ts = _parse_iso(row.get("tombstone_at"))
            if ts and (oldest_hot is None or ts < oldest_hot):
                oldest_hot = ts
    tm["oldest_hot_tombstone_at"] = format_iso(oldest_hot) if oldest_hot else None

    head["entries"] = entries

    # --- Write head with rollback; shard+stub already written exclusively ---
    rollback_plan: list[tuple[Path, bytes | None]] = [
        (safe_path(repo_root, shard_rel), None),
        (safe_path(repo_root, stub_rel), None),
    ]
    rollback_plan.extend(_capture_rollback([head_path]))

    try:
        _write_json(head_path, head)
    except Exception:
        _restore_rollback(rollback_plan)
        raise

    return {
        "ok": True,
        "family": "replication_tombstones",
        "entries_externalized": len(cut_entries),
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
        "written_paths": [shard_rel, stub_rel, REPLICATION_TOMBSTONES_REL],
        "warnings": warnings,
        "audit_events": [{"event": "registry_tombstone_maintenance", "detail": {
            "family": "replication_tombstones",
            "entries_externalized": len(cut_entries),
            "shard_id": shard_id,
        }}],
    }


# ===================================================================
# Orchestrator: run registry maintenance
# ===================================================================

def registry_maintenance_service(
    *,
    repo_root: Path,
    gm: Any,
    now: datetime | None = None,
    families: list[str] | None = None,
    settings: Any,
    audit: Callable[[Any, str, dict[str, Any]], None] | None = None,
    auth: Any = None,
) -> dict[str, Any]:
    """Run registry lifecycle maintenance for the requested families.

    Processes families in the spec-defined order, stopping after one family
    reaches the batch limit. Returns aggregated results.
    """
    if now is None:
        now = _iso_now()

    all_families = ["delivery", "nonce", "peer_trust", "replication_tombstones"]
    requested = families if families else all_families
    # Enforce spec order
    ordered = [f for f in all_families if f in requested]

    results: dict[str, Any] = {}
    all_warnings: list[str] = []
    all_written: list[str] = []
    batch_limit = int(settings.registry_history_batch_limit)

    for family in ordered:
        try:
            if family == "delivery":
                result = delivery_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    terminal_retention_days=int(settings.delivery_terminal_retention_days),
                    idempotency_retention_days=int(settings.delivery_idempotency_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "nonce":
                result = nonce_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    nonce_retention_days=int(settings.nonce_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "peer_trust":
                result = peer_trust_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    max_hot_entries=int(settings.peer_trust_history_max_hot_entries),
                    hot_retention_days=int(settings.peer_trust_history_hot_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "replication_tombstones":
                result = tombstone_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    grace_days=int(settings.replication_tombstone_grace_days),
                    batch_limit=batch_limit,
                )
            else:
                continue
        except Exception:
            _logger.error("Registry maintenance failed for family %s; continuing with remaining families", family, exc_info=True)
            results[family] = {"ok": False, "family": family, "error": f"maintenance_failed:{family}"}
            all_warnings.append(f"registry_maintenance_failed:{family}")
            continue

        results[family] = result
        all_warnings.extend(result.get("warnings", []))
        written = result.get("written_paths", [])
        all_written.extend(written)

        # Spec: stop after one family reaches the batch limit
        processed = (
            result.get("records_externalized", 0)
            + result.get("idempotency_externalized", 0)
            + result.get("idempotency_pruned", 0)
            + result.get("pruned", 0)
            + result.get("transitions_externalized", 0)
            + result.get("entries_externalized", 0)
        )
        if processed >= batch_limit:
            break

    # Also prune pull_idempotency (not part of spec family order, handled separately)
    if "replication_state" in requested or not families:
        idem_result = replication_state_prune_idempotency(
            repo_root=repo_root,
            now=now,
            pull_idempotency_retention_days=int(settings.replication_pull_idempotency_retention_days),
            batch_limit=batch_limit,
        )
        results["replication_state_idempotency"] = idem_result
        all_warnings.extend(idem_result.get("warnings", []))
        all_written.extend(idem_result.get("written_paths", []))

    # Git commit all written paths
    committed_files: list[str] = []
    durable = True
    if all_written and gm is not None:
        commit_paths = [safe_path(repo_root, rel) for rel in all_written]
        if try_commit_paths(
            paths=commit_paths,
            gm=gm,
            commit_message="registry-lifecycle: maintenance pass",
        ):
            committed_files = list(all_written)
        else:
            durable = False
            all_warnings.append(
                make_warning(
                    "registry_maintenance_not_durable",
                    "Data written to disk but not committed to git",
                )
            )

    any_family_failed = any(not r.get("ok", True) for r in results.values())

    response: dict[str, Any] = {
        "ok": not any_family_failed,
        "durable": durable,
        "families": results,
        "committed_files": committed_files,
        "warnings": all_warnings if all_warnings else [],
    }
    if not durable:
        response["at_risk_paths"] = list(all_written)
    if gm is not None:
        response["latest_commit"] = gm.latest_commit()

    if audit and auth:
        audit(auth, "registry_maintenance", {
            "families": list(results.keys()),
            "committed": len(committed_files),
            "durable": durable,
            "warning_count": len(all_warnings),
        })
        # Emit per-family audit events collected from family passes
        for fam_name, fam_result in results.items():
            for evt in fam_result.get("audit_events", []):
                try:
                    audit(auth, evt["event"], evt["detail"])
                except Exception:
                    _logger.warning("Failed to emit audit event for family %s", fam_name, exc_info=True)

    # Cold-store eligible history shards and prune expired cold tombstones
    cold_apply_result: dict[str, Any] | None = None
    retention_prune_result: dict[str, Any] | None = None
    if auth is not None and gm is not None:
        try:
            cold_apply_result = registry_history_cold_apply_service(
                repo_root=repo_root,
                gm=gm,
                auth=auth,
                now=now,
                settings=settings,
                families=families,
                audit=audit or (lambda *_args, **_kwargs: None),
            )
            all_warnings.extend(cold_apply_result.get("warnings", []))
        except Exception:
            _logger.error("Registry cold-apply failed; continuing", exc_info=True)
            all_warnings.append("registry_cold_apply_failed")

        try:
            retention_prune_result = registry_history_retention_prune_service(
                repo_root=repo_root,
                gm=gm,
                now=now,
                retention_days=int(settings.replication_tombstone_retention_days),
                batch_limit=batch_limit,
            )
            all_warnings.extend(retention_prune_result.get("warnings", []))
        except Exception:
            _logger.error("Registry retention-prune failed; continuing", exc_info=True)
            all_warnings.append("registry_retention_prune_failed")

    response["cold_apply"] = cold_apply_result
    response["retention_prune"] = retention_prune_result

    return response


# ===================================================================
# Registry-history cold-store / rehydrate
# ===================================================================


def registry_history_cold_dir_rel(history_dir_rel: str) -> str:
    """Return the cold payload directory for one history namespace."""
    return f"{history_dir_rel}/cold"


def registry_history_cold_storage_rel_path(payload_rel: str) -> str:
    """Map a hot registry-history shard path to its cold gzip payload path."""
    rel = str(payload_rel or "").strip().strip("/")
    for history_dir_rel in _REGISTRY_HISTORY_DIRS_BY_FAMILY.values():
        prefix = f"{history_dir_rel}/"
        if rel.startswith(prefix) and rel.endswith(".json") and "/index/" not in rel and "/cold/" not in rel:
            return f"{registry_history_cold_dir_rel(history_dir_rel)}/{Path(rel).name}.gz"
    raise HTTPException(status_code=400, detail="Invalid registry-history payload path")


def registry_history_payload_rel_from_cold(cold_rel: str) -> str:
    """Derive the hot payload path from a registry-history cold payload path."""
    rel = str(cold_rel or "").strip().strip("/")
    for history_dir_rel in _REGISTRY_HISTORY_DIRS_BY_FAMILY.values():
        cold_prefix = f"{registry_history_cold_dir_rel(history_dir_rel)}/"
        if rel.startswith(cold_prefix) and rel.endswith(".json.gz"):
            stem = Path(rel).name[:-3]  # remove .gz → leaves shard_id.json
            return f"{history_dir_rel}/{stem}"
    raise HTTPException(status_code=400, detail="Invalid registry-history cold payload path")


def _validate_registry_history_payload_rel_path(payload_rel: str) -> tuple[str, str]:
    """Validate one hot registry-history payload path and return family + history dir."""
    rel = str(payload_rel or "").strip().strip("/")
    for family, history_dir_rel in _REGISTRY_HISTORY_DIRS_BY_FAMILY.items():
        prefix = f"{history_dir_rel}/"
        if rel.startswith(prefix) and rel.endswith(".json") and "/index/" not in rel and "/cold/" not in rel:
            return family, history_dir_rel
    raise HTTPException(status_code=400, detail="Invalid registry-history payload path")


def _registry_family_cold_timestamp(summary: dict[str, Any], *, family: str) -> datetime | None:
    """Return the cold-eligibility timestamp from a shard's summary."""
    field = {
        "delivery": "newest_retention_timestamp",
        "peer_trust": "newest_transition_at",
        "replication_state": "newest_event_at",
        "replication_tombstones": "newest_tombstone_at",
    }.get(family)
    if field is None:
        return None
    return _parse_iso(str(summary.get(field) or ""))


def _load_registry_history_shard(repo_root: Path, payload_rel: str) -> dict[str, Any]:
    """Load and validate one hot registry-history shard payload."""
    family, _history_dir_rel = _validate_registry_history_payload_rel_path(payload_rel)
    path = safe_path(repo_root, payload_rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Registry-history shard not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid registry-history shard: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid registry-history shard")
    expected_schema = _REGISTRY_SHARD_SCHEMA_TYPES_BY_FAMILY.get(family)
    if expected_schema and payload.get("schema_type") != expected_schema:
        raise HTTPException(status_code=400, detail="Registry-history shard schema_type mismatch")
    if payload.get("schema_version") != "1.0":
        raise HTTPException(status_code=400, detail="Registry-history shard schema_version mismatch")
    if not isinstance(payload.get("summary"), dict):
        raise HTTPException(status_code=400, detail="Registry-history shard summary must be an object")
    return payload


def _load_registry_history_stub(repo_root: Path, stub_rel: str) -> dict[str, Any]:
    """Load and validate one registry-history stub."""
    rel = str(stub_rel or "").strip().strip("/")
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Registry-history stub not found")
    try:
        stub = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid registry-history stub: {exc}") from exc
    if not isinstance(stub, dict):
        raise HTTPException(status_code=400, detail="Invalid registry-history stub")
    if stub.get("schema_type") != "registry_history_stub" or stub.get("schema_version") != "1.0":
        raise HTTPException(status_code=400, detail="Invalid registry-history stub schema")
    family = str(stub.get("family") or "")
    if family not in _REGISTRY_STUB_DIRS_BY_FAMILY:
        raise HTTPException(status_code=400, detail="Invalid registry-history stub family")
    if stub.get("shard_id") != path.stem:
        raise HTTPException(status_code=400, detail="Registry-history stub shard_id does not match path")
    if not isinstance(stub.get("summary"), dict):
        raise HTTPException(status_code=400, detail="Registry-history stub summary must be an object")
    payload_path = str(stub.get("payload_path") or "")
    if not payload_path:
        raise HTTPException(status_code=400, detail="Registry-history stub payload_path is required")
    return stub


def _restore_failed_registry_cold_store(
    *,
    source_payload_path: Path,
    source_payload_bytes: bytes,
    cold_payload_path: Path,
    stub_path: Path,
    original_stub: dict[str, Any],
) -> list[str]:
    """Restore the original hot payload and stub after failed cold-store commit."""
    errors: list[str] = []
    try:
        write_bytes_file(source_payload_path, source_payload_bytes)
    except Exception as exc:
        errors.append(f"restore payload: {exc}")
    try:
        cold_payload_path.unlink(missing_ok=True)
    except Exception as exc:
        errors.append(f"remove cold: {exc}")
    try:
        _write_json(stub_path, original_stub)
    except Exception as exc:
        errors.append(f"restore stub: {exc}")
    return errors


def registry_history_cold_store_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: Any,
    req: Any,
    audit: Callable[[Any, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Cold-store one registry-history shard into a gzip payload plus hot JSON stub."""
    if hasattr(auth, "require"):
        auth.require("admin:peers")
    source_payload_path = str(req.source_payload_path)
    if hasattr(auth, "require_read_path"):
        auth.require_read_path(source_payload_path)
        auth.require_write_path(source_payload_path)
    family, history_dir_rel = _validate_registry_history_payload_rel_path(source_payload_path)
    cold_storage_path = registry_history_cold_storage_rel_path(source_payload_path)
    cold_payload_path = safe_path(repo_root, cold_storage_path)
    hot_payload_path = safe_path(repo_root, source_payload_path)
    stub_dir_rel = _REGISTRY_STUB_DIRS_BY_FAMILY[family]
    stub_rel = f"{stub_dir_rel}/{hot_payload_path.name}"
    if hasattr(auth, "require_read_path"):
        auth.require_read_path(stub_rel)
        auth.require_write_path(stub_rel)
        auth.require_write_path(cold_storage_path)

    lock_key = f"registry_history_cold:{source_payload_path}"
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock(lock_key, lock_dir=lock_dir)
    except LockInfrastructureError as exc:
        raise HTTPException(status_code=503, detail="Registry-history cold-store lock unavailable; retry") from exc
    try:
        with lock_ctx:
            # Crash recovery: if both hot and cold exist, a previous attempt crashed
            # after writing cold but before deleting hot. Remove the orphan cold file.
            if cold_payload_path.exists() and hot_payload_path.exists():
                _logger.warning(
                    "Registry cold-store crash recovery: removing orphaned cold file %s",
                    cold_storage_path,
                )
                cold_payload_path.unlink(missing_ok=True)

            payload = _load_registry_history_shard(repo_root, source_payload_path)
            stub = _load_registry_history_stub(repo_root, stub_rel)
            if stub.get("payload_path") != source_payload_path:
                raise HTTPException(status_code=409, detail="Registry-history stub does not point at the hot payload")
            source_bytes = hot_payload_path.read_bytes()
            if cold_payload_path.exists():
                raise HTTPException(status_code=409, detail="Registry-history cold payload already exists")

            try:
                gzip_bytes = build_cold_gzip_bytes(source_bytes)
                cold_payload_path.parent.mkdir(parents=True, exist_ok=True)
                write_bytes_file(cold_payload_path, gzip_bytes)
                updated_stub = dict(stub)
                updated_stub["payload_path"] = cold_storage_path
                _write_json(safe_path(repo_root, stub_rel), updated_stub)
                hot_payload_path.unlink()
                committed = bool(gm and try_commit_paths(
                    paths=[cold_payload_path, safe_path(repo_root, stub_rel), hot_payload_path],
                    gm=gm,
                    commit_message=f"registry-history: cold-store {family} {payload.get('shard_id', '')}",
                ))
                if gm is not None and not committed:
                    raise RuntimeError("Registry-history cold-store commit produced no changes")
            except Exception as exc:
                cleanup_errors = _restore_failed_registry_cold_store(
                    source_payload_path=hot_payload_path,
                    source_payload_bytes=source_bytes,
                    cold_payload_path=cold_payload_path,
                    stub_path=safe_path(repo_root, stub_rel),
                    original_stub=stub,
                )
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="registry_history_cold_store",
                        family=family,
                        error_code="registry_history_cold_store_failed",
                        error_detail=str(exc),
                        rollback_errors=cleanup_errors,
                    ),
                ) from exc
    except SegmentHistoryLockTimeout as exc:
        raise make_lock_error("registry_history_cold_store", family, exc, is_timeout=True) from exc

    audit(
        auth,
        "registry_history_cold_store",
        {
            "family": family,
            "source_payload_path": source_payload_path,
            "cold_storage_path": cold_storage_path,
        },
    )
    return {
        "ok": True,
        "family": family,
        "shard_state": "cold",
        "source_payload_path": source_payload_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": stub_rel,
        "committed_files": [cold_storage_path, stub_rel, source_payload_path],
        "durable": True,
        "latest_commit": gm.latest_commit() if gm is not None else None,
        "warnings": [],
        "recovery_warnings": [],
    }


def registry_history_cold_rehydrate_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: Any,
    req: Any,
    audit: Callable[[Any, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Rehydrate one registry-history cold payload back into its hot history path."""
    if hasattr(auth, "require"):
        auth.require("admin:peers")
    if getattr(req, "cold_stub_path", None):
        cold_stub_path = str(req.cold_stub_path)
        stub = _load_registry_history_stub(repo_root, cold_stub_path)
        source_payload_path = registry_history_payload_rel_from_cold(str(stub.get("payload_path") or ""))
        family, _history_dir_rel = _validate_registry_history_payload_rel_path(source_payload_path)
    else:
        source_payload_path = str(req.source_payload_path)
        family, _history_dir_rel = _validate_registry_history_payload_rel_path(source_payload_path)
        stub_dir_rel = _REGISTRY_STUB_DIRS_BY_FAMILY[family]
        cold_stub_path = f"{stub_dir_rel}/{Path(source_payload_path).name}"
        stub = _load_registry_history_stub(repo_root, cold_stub_path)
    cold_storage_path = str(stub.get("payload_path") or "")
    if hasattr(auth, "require_read_path"):
        auth.require_read_path(cold_stub_path)
        auth.require_read_path(cold_storage_path)
        auth.require_write_path(cold_stub_path)
        auth.require_write_path(cold_storage_path)
        auth.require_write_path(source_payload_path)

    stub_path = safe_path(repo_root, cold_stub_path)
    hot_payload_path = safe_path(repo_root, source_payload_path)
    cold_payload_path = safe_path(repo_root, cold_storage_path)

    lock_key = f"registry_history_cold:{source_payload_path}"
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock(lock_key, lock_dir=lock_dir)
    except LockInfrastructureError as exc:
        raise HTTPException(status_code=503, detail="Registry-history cold-rehydrate lock unavailable; retry") from exc
    try:
        with lock_ctx:
            if hot_payload_path.exists():
                raise HTTPException(status_code=409, detail="Registry-history hot payload already exists")
            if not cold_payload_path.exists() or not cold_payload_path.is_file():
                raise HTTPException(status_code=404, detail="Registry-history cold payload not found")
            try:
                cold_payload_bytes = cold_payload_path.read_bytes()
                cold_stub_bytes = stub_path.read_bytes()
                payload_bytes = gzip.decompress(cold_payload_bytes)
                payload = json.loads(payload_bytes.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("payload is not an object")
                if payload.get("shard_id") != Path(source_payload_path).stem:
                    raise ValueError("shard_id mismatch")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Invalid registry-history cold payload: {exc}") from exc

            # Validate decompressed payload schema
            _expected_type = _REGISTRY_SHARD_SCHEMA_TYPES_BY_FAMILY.get(family)
            if payload.get("schema_type") != _expected_type:
                raise HTTPException(
                    status_code=400,
                    detail=make_error_detail(
                        operation="registry_history_cold_rehydrate",
                        family=family,
                        error_code="registry_history_cold_rehydrate_schema_mismatch",
                        error_detail=f"Expected schema_type '{_expected_type}', got '{payload.get('schema_type')}'",
                    ),
                )
            if payload.get("schema_version") != "1.0":
                raise HTTPException(
                    status_code=400,
                    detail=make_error_detail(
                        operation="registry_history_cold_rehydrate",
                        family=family,
                        error_code="registry_history_cold_rehydrate_schema_version_mismatch",
                        error_detail=f"Expected schema_version '1.0', got '{payload.get('schema_version')}'",
                    ),
                )
            if not isinstance(payload.get("summary"), dict):
                raise HTTPException(
                    status_code=400,
                    detail=make_error_detail(
                        operation="registry_history_cold_rehydrate",
                        family=family,
                        error_code="registry_history_cold_rehydrate_invalid_summary",
                        error_detail="Decompressed payload 'summary' must be a dict",
                    ),
                )

            updated_stub = dict(stub)
            updated_stub["payload_path"] = source_payload_path
            try:
                write_bytes_file(hot_payload_path, payload_bytes)
                _write_json(stub_path, updated_stub)
                cold_payload_path.unlink()
                committed = bool(gm and try_commit_paths(
                    paths=[hot_payload_path, stub_path, cold_payload_path],
                    gm=gm,
                    commit_message=f"registry-history: cold-rehydrate {payload.get('schema_type', '')} {payload.get('shard_id', '')}",
                ))
                if gm is not None and not committed:
                    raise RuntimeError("Registry-history cold rehydrate commit produced no changes")
            except Exception as exc:
                rollback_errors: list[str] = []
                try:
                    hot_payload_path.unlink(missing_ok=True)
                except Exception as rollback_exc:
                    rollback_errors.append(f"remove restored payload: {rollback_exc}")
                try:
                    write_bytes_file(cold_payload_path, cold_payload_bytes)
                except Exception as rollback_exc:
                    rollback_errors.append(f"restore cold payload: {rollback_exc}")
                try:
                    write_bytes_file(stub_path, cold_stub_bytes)
                except Exception as rollback_exc:
                    rollback_errors.append(f"restore cold stub: {rollback_exc}")
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="registry_history_cold_rehydrate",
                        error_code="registry_history_cold_rehydrate_failed",
                        error_detail=str(exc),
                        rollback_errors=rollback_errors,
                    ),
                ) from exc
    except SegmentHistoryLockTimeout as exc:
        raise make_lock_error("registry_history_cold_rehydrate", None, exc, is_timeout=True) from exc

    audit(
        auth,
        "registry_history_cold_rehydrate",
        {
            "source_payload_path": source_payload_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "family": payload.get("schema_type", "").replace("_shard", "").replace("_history", ""),
        "shard_state": "hot",
        "source_payload_path": source_payload_path,
        "restored_payload_path": source_payload_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "committed_files": [source_payload_path, cold_storage_path, cold_stub_path],
        "durable": True,
        "latest_commit": gm.latest_commit() if gm is not None else None,
        "warnings": [],
        "recovery_warnings": [],
    }


def registry_history_cold_apply_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: Any,
    now: datetime,
    settings: Any,
    families: list[str] | None,
    audit: Callable[[Any, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply registry-history cold-store eligibility using the configured thresholds."""
    all_cold_families = ["delivery", "peer_trust", "replication_state", "replication_tombstones"]
    requested = [f for f in all_cold_families if (families is None or f in families)]
    cold_after_days_by_family = {
        "delivery": int(settings.delivery_history_cold_after_days),
        "peer_trust": int(settings.peer_trust_history_cold_after_days),
        "replication_state": int(settings.replication_history_cold_after_days),
        "replication_tombstones": int(settings.replication_tombstone_cold_after_days),
    }
    results: dict[str, Any] = {}
    warnings: list[str] = []
    cold_stored = 0

    for family in requested:
        history_dir_rel = _REGISTRY_HISTORY_DIRS_BY_FAMILY[family]
        history_dir = safe_path(repo_root, history_dir_rel)
        eligible_paths: list[str] = []
        if history_dir.exists() and history_dir.is_dir():
            cutoff = now - timedelta(days=cold_after_days_by_family[family])
            for path in sorted(history_dir.glob("*.json")):
                if not path.is_file():
                    continue
                rel = str(path.relative_to(repo_root))
                try:
                    payload = _load_registry_history_shard(repo_root, rel)
                except HTTPException:
                    warnings.append(make_warning(
                        "registry_history_payload_unreadable",
                        f"Cannot read shard: {rel}",
                        path=rel,
                    ))
                    continue
                cold_ts = _registry_family_cold_timestamp(payload.get("summary", {}), family=family)
                if cold_ts is None:
                    warnings.append(f"registry_history_cold_timestamp_missing:{rel}")
                    continue
                stub_dir_rel = _REGISTRY_STUB_DIRS_BY_FAMILY[family]
                stub_rel = f"{stub_dir_rel}/{path.name}"
                try:
                    stub = _load_registry_history_stub(repo_root, stub_rel)
                except HTTPException:
                    warnings.append(f"registry_history_invalid_stub:{stub_rel}")
                    continue
                if stub.get("payload_path") != rel:
                    continue  # Already cold or inconsistent
                if cold_ts <= cutoff:
                    eligible_paths.append(rel)
        family_results: list[dict[str, Any]] = []
        for rel in eligible_paths:
            try:
                family_results.append(
                    registry_history_cold_store_service(
                        repo_root=repo_root,
                        gm=gm,
                        auth=auth,
                        req=type("Req", (), {"source_payload_path": rel})(),
                        audit=audit,
                    )
                )
                cold_stored += 1
            except HTTPException as exc:
                warnings.append(make_warning(
                    "registry_history_cold_store_failed",
                    str(exc.detail),
                    path=rel,
                ))
        results[family] = {"eligible": len(eligible_paths), "cold_stored": len(family_results), "results": family_results}

    return {"ok": True, "cold_stored": cold_stored, "families": results, "warnings": warnings}


def registry_history_retention_prune_service(
    *,
    repo_root: Path,
    gm: Any,
    now: datetime,
    retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Prune cold tombstone shards past the configured retention window.

    Only applies to the replication_tombstones family (the only registry family
    with a retention_days setting).
    """
    cold_dir_rel = registry_history_cold_dir_rel(REPLICATION_TOMBSTONE_HISTORY_DIR_REL)
    cold_dir = safe_path(repo_root, cold_dir_rel)
    stub_dir_rel = REPLICATION_TOMBSTONE_STUB_DIR_REL
    warnings: list[str] = []
    pruned = 0
    pruned_paths: list[str] = []
    cutoff = now - timedelta(days=retention_days)

    if not cold_dir.exists() or not cold_dir.is_dir():
        return {"ok": True, "pruned": 0, "warnings": warnings}

    for cold_path in sorted(cold_dir.glob("*.json.gz")):
        if pruned >= batch_limit:
            break
        if not cold_path.is_file():
            continue
        cold_rel = str(cold_path.relative_to(repo_root))

        # Read and decompress to extract summary timestamp
        try:
            payload_bytes = gzip.decompress(cold_path.read_bytes())
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            warnings.append(f"registry_retention_prune_unreadable:{cold_rel}")
            continue

        if not isinstance(payload, dict):
            warnings.append(f"registry_retention_prune_invalid:{cold_rel}")
            continue

        summary = payload.get("summary", {})
        cold_ts = _parse_iso(str(summary.get("newest_tombstone_at") or ""))
        if cold_ts is None:
            warnings.append(f"registry_retention_prune_no_timestamp:{cold_rel}")
            continue
        if cold_ts > cutoff:
            continue  # Not past retention

        # Derive stub path from the cold shard filename
        shard_filename = cold_path.name[:-3]  # remove .gz → shard_id.json
        stub_rel = f"{stub_dir_rel}/{shard_filename}"

        # Acquire lock to prevent racing with a concurrent rehydrate
        lock_key = f"registry_history_cold:{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/{shard_filename}"
        lock_dir = repo_root / ".locks"
        try:
            lock_ctx = segment_history_source_lock(lock_key, lock_dir=lock_dir)
        except LockInfrastructureError:
            warnings.append(f"registry_retention_prune_lock_unavailable:{cold_rel}")
            continue

        try:
            with lock_ctx:
                # TOCTOU guard: verify stub still points to this cold file
                stub_path = safe_path(repo_root, stub_rel)
                if stub_path.exists():
                    try:
                        stub = json.loads(stub_path.read_text(encoding="utf-8"))
                    except Exception:
                        warnings.append(f"registry_retention_prune_stub_unreadable:{stub_rel}")
                        continue
                    if stub.get("payload_path") != cold_rel:
                        # Stub points elsewhere (e.g., rehydrated) — skip
                        continue
                    stub_path.unlink()
                    pruned_paths.append(stub_rel)

                cold_path.unlink()
                pruned_paths.append(cold_rel)
                pruned += 1
        except SegmentHistoryLockTimeout:
            warnings.append(f"registry_retention_prune_lock_timeout:{cold_rel}")
            continue

    # Git commit all deletions
    durable = True
    if pruned_paths and gm is not None:
        commit_paths = [safe_path(repo_root, rel) for rel in pruned_paths]
        if not try_commit_paths(
            paths=commit_paths,
            gm=gm,
            commit_message=f"registry-history: retention-prune {pruned} tombstone shards",
        ):
            durable = False
            warnings.append("registry_retention_prune_not_durable")

    return {
        "ok": True,
        "pruned": pruned,
        "durable": durable,
        "pruned_paths": pruned_paths,
        "warnings": warnings,
    }
