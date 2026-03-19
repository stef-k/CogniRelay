"""Registry lifecycle: externalize, prune, and stub management for current-state registries.

Implements the namespace-specific execution contract defined in issue #112.
Each registry family has its own maintenance pass logic; the shared substrate
(shard naming, stub creation, rollback) is defined in shared helpers at the
top of this module.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from app.git_safety import try_commit_paths
from app.storage import safe_path, write_bytes_file, write_text_file

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


# ---------------------------------------------------------------------------
# ISO timestamp helpers
# ---------------------------------------------------------------------------

def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO timestamp string into a timezone-aware datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _iso_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Shared shard naming
# ---------------------------------------------------------------------------

def _shard_timestamp_str(cut_at: datetime) -> str:
    """Convert cut_at to the YYYYMMDDTHHMMSSZ format per spec."""
    utc = cut_at.astimezone(timezone.utc).replace(microsecond=0)
    return utc.strftime("%Y%m%dT%H%M%SZ")


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
        "created_at": created_at.isoformat(),
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
            return None, f"delivery_retention_missing:acked record has no parseable accepted ack_at"
        return ts, None

    if effective_status == "dead_letter":
        stored_status = str(record.get("status") or "")
        if stored_status == "pending_ack":
            # Effective dead-letter because ack_deadline passed
            dl = _parse_iso(record.get("ack_deadline"))
            if dl is None:
                return None, f"delivery_retention_missing:effective dead_letter has no parseable ack_deadline"
            return dl, None
        # Stored dead_letter
        ts = _latest_ack_at("rejected")
        if ts is not None:
            return ts, None
        # Fallback: any parseable ack_at
        ts = _latest_ack_at()
        if ts is not None:
            return ts, None
        return None, f"delivery_retention_missing:dead_letter record has no parseable ack timestamps"

    if effective_status == "delivered":
        ts = _parse_iso(record.get("sent_at"))
        if ts is None:
            return None, f"delivery_retention_missing:delivered record has no parseable sent_at"
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
            "warnings": warnings,
        }

    # --- Build shard if there are records to externalize ---
    shard_id = None
    shard_rel = None
    stub_rel = None
    cut_records: dict[str, Any] = {}
    cut_idempotency: dict[str, str] = {}

    if eligible_records:
        shard_dir = safe_path(repo_root, DELIVERY_HISTORY_DIR_REL)
        shard_dir.mkdir(parents=True, exist_ok=True)
        shard_id = _next_shard_id("delivery", now, shard_dir)

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
            "oldest_retention_timestamp": min(retention_timestamps).isoformat() if retention_timestamps else None,
            "newest_retention_timestamp": max(retention_timestamps).isoformat() if retention_timestamps else None,
        }

        shard_payload = {
            "schema_type": "delivery_history_shard",
            "schema_version": "1.0",
            "shard_id": shard_id,
            "source_head_path": DELIVERY_STATE_REL,
            "cut_at": now.isoformat(),
            "records": cut_records,
            "idempotency": cut_idempotency,
            "summary": summary,
        }

        shard_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"
        stub_rel = f"{DELIVERY_STUB_DIR_REL}/{shard_id}.json"

        stub_payload = _create_stub(
            family="delivery",
            shard_id=shard_id,
            payload_path=shard_rel,
            created_at=now,
            source_head_path=DELIVERY_STATE_REL,
            summary=summary,
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
        dm["last_cut_at"] = now.isoformat()
        dm["last_cut_record_count"] = len(cut_records)
        dm["last_cut_effective_status_counts"] = summary["effective_status_counts"]  # type: ignore[possibly-undefined]
    dm["hot_record_count"] = len(records)
    dm["hot_idempotency_count"] = len(idempotency)

    head["records"] = records
    head["idempotency"] = idempotency

    # --- Write files with rollback ---
    paths_to_write: list[tuple[Path, str, dict[str, Any]]] = []
    paths_to_write.append((head_path, DELIVERY_STATE_REL, {k: head[k] for k in ("version", "records", "idempotency", "history_meta") if k in head}))
    if shard_rel:
        shard_path = safe_path(repo_root, shard_rel)
        paths_to_write.append((shard_path, shard_rel, shard_payload))  # type: ignore[possibly-undefined]
    if stub_rel:
        stub_path = safe_path(repo_root, stub_rel)
        paths_to_write.append((stub_path, stub_rel, stub_payload))  # type: ignore[possibly-undefined]

    rollback = _capture_rollback([p for p, _, _ in paths_to_write])
    try:
        for path, _, data in paths_to_write:
            _write_json(path, data)
    except Exception:
        _restore_rollback(rollback)
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
        "written_paths": [rel for _, rel, _ in paths_to_write],
        "warnings": warnings,
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
    nm["last_pruned_at"] = now.isoformat()
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
        shard_dir = safe_path(repo_root, PEER_TRUST_HISTORY_DIR_REL)
        shard_dir.mkdir(parents=True, exist_ok=True)
        shard_id = _next_shard_id("peer_trust", now, shard_dir)

        transition_timestamps = [_parse_iso(t.get("at")) for t in eligible if _parse_iso(t.get("at"))]
        final_trust_after = str(row.get("trust_level") or "untrusted")

        summary = {
            "transition_count": len(eligible),
            "oldest_transition_at": min(transition_timestamps).isoformat() if transition_timestamps else None,
            "newest_transition_at": max(transition_timestamps).isoformat() if transition_timestamps else None,
            "final_hot_trust_level_after_cut": final_trust_after,
        }

        shard_payload = {
            "schema_type": "peer_trust_history_shard",
            "schema_version": "1.0",
            "shard_id": shard_id,
            "source_head_path": PEERS_REGISTRY_REL,
            "peer_id": peer_id,
            "cut_at": now.isoformat(),
            "transitions": eligible,
            "summary": summary,
        }

        shard_rel = f"{PEER_TRUST_HISTORY_DIR_REL}/{shard_id}.json"
        stub_rel = f"{PEER_TRUST_STUB_DIR_REL}/{shard_id}.json"

        stub_summary = dict(summary)
        stub_summary["peer_id"] = peer_id

        stub_payload = _create_stub(
            family="peer_trust",
            shard_id=shard_id,
            payload_path=shard_rel,
            created_at=now,
            source_head_path=PEERS_REGISTRY_REL,
            summary=stub_summary,
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
            "shard_payload": shard_payload,
            "stub_payload": stub_payload,
        })

    if not shard_results:
        return {
            "ok": True,
            "family": "peer_trust",
            "transitions_externalized": 0,
            "shards_created": 0,
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
    pr_meta["last_cut_at"] = now.isoformat()
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
        pm["last_trust_cut_at"] = now.isoformat()
        # Compute total count
        peer_row = peers.get(pid, {})
        hot_count = len(peer_row.get("trust_history", [])) if isinstance(peer_row, dict) else 0
        pm["trust_history_total_count"] = hot_count + pm["trust_history_externalized_count"]

    head["peers"] = peers

    # --- Write all files with rollback ---
    paths_to_write: list[tuple[Path, str, dict[str, Any]]] = [(head_path, PEERS_REGISTRY_REL, head)]
    for sr in shard_results:
        shard_path = safe_path(repo_root, sr["shard_rel"])
        paths_to_write.append((shard_path, sr["shard_rel"], sr["shard_payload"]))
        stub_path = safe_path(repo_root, sr["stub_rel"])
        paths_to_write.append((stub_path, sr["stub_rel"], sr["stub_payload"]))

    rollback = _capture_rollback([p for p, _, _ in paths_to_write])
    try:
        for path, _, data in paths_to_write:
            _write_json(path, data)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "peer_trust",
        "transitions_externalized": total_externalized,
        "shards_created": len(shard_results),
        "shards": [{"peer_id": sr["peer_id"], "shard_id": sr["shard_id"], "transition_count": sr["transition_count"]} for sr in shard_results],
        "written_paths": [rel for _, rel, _ in paths_to_write],
        "warnings": warnings,
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

    shard_dir = safe_path(repo_root, REPLICATION_STATE_HISTORY_DIR_REL)
    shard_dir.mkdir(parents=True, exist_ok=True)
    shard_id = _next_shard_id("replication_state", now, shard_dir)

    summary = {
        "push_event_count": 1,
        "pull_event_count": 0,
        "oldest_event_at": pushed_at.isoformat(),
        "newest_event_at": pushed_at.isoformat(),
    }

    shard_payload = {
        "schema_type": "replication_state_history_shard",
        "schema_version": "1.0",
        "shard_id": shard_id,
        "source_head_path": REPLICATION_STATE_REL,
        "cut_at": now.isoformat(),
        "push_events": [{"superseded_at": now.isoformat(), "row": previous_row}],
        "pull_events": [],
        "summary": summary,
    }

    shard_rel = f"{REPLICATION_STATE_HISTORY_DIR_REL}/{shard_id}.json"
    stub_rel = f"{REPLICATION_STATE_STUB_DIR_REL}/{shard_id}.json"

    stub_payload = _create_stub(
        family="replication_state",
        shard_id=shard_id,
        payload_path=shard_rel,
        created_at=now,
        source_head_path=REPLICATION_STATE_REL,
        summary=summary,
    )

    shard_path = safe_path(repo_root, shard_rel)
    stub_path = safe_path(repo_root, stub_rel)
    _write_json(shard_path, shard_payload)
    _write_json(stub_path, stub_payload)

    return {
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
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

    shard_dir = safe_path(repo_root, REPLICATION_STATE_HISTORY_DIR_REL)
    shard_dir.mkdir(parents=True, exist_ok=True)
    shard_id = _next_shard_id("replication_state", now, shard_dir)

    summary = {
        "push_event_count": 0,
        "pull_event_count": 1,
        "oldest_event_at": pulled_at.isoformat(),
        "newest_event_at": pulled_at.isoformat(),
    }

    shard_payload = {
        "schema_type": "replication_state_history_shard",
        "schema_version": "1.0",
        "shard_id": shard_id,
        "source_head_path": REPLICATION_STATE_REL,
        "cut_at": now.isoformat(),
        "push_events": [],
        "pull_events": [{"source_peer": source_peer, "superseded_at": now.isoformat(), "row": previous_row}],
        "summary": summary,
    }

    shard_rel = f"{REPLICATION_STATE_HISTORY_DIR_REL}/{shard_id}.json"
    stub_rel = f"{REPLICATION_STATE_STUB_DIR_REL}/{shard_id}.json"

    stub_payload = _create_stub(
        family="replication_state",
        shard_id=shard_id,
        payload_path=shard_rel,
        created_at=now,
        source_head_path=REPLICATION_STATE_REL,
        summary=summary,
    )

    shard_path = safe_path(repo_root, shard_rel)
    stub_path = safe_path(repo_root, stub_rel)
    _write_json(shard_path, shard_payload)
    _write_json(stub_path, stub_payload)

    return {
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
    }


def replication_state_prune_idempotency(
    *,
    repo_root: Path,
    now: datetime,
    pull_idempotency_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Prune expired pull_idempotency entries from replication_state.json."""
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
        return {"ok": True, "family": "replication_state", "pruned": 0, "warnings": warnings}

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
            "warnings": warnings,
        }

    # Build shard
    shard_dir = safe_path(repo_root, REPLICATION_TOMBSTONE_HISTORY_DIR_REL)
    shard_dir.mkdir(parents=True, exist_ok=True)
    shard_id = _next_shard_id("replication_tombstone", now, shard_dir)

    cut_entries: dict[str, Any] = {}
    tombstone_timestamps: list[datetime] = []
    for path_key, row, ts in eligible:
        cut_entries[path_key] = row
        tombstone_timestamps.append(ts)

    summary = {
        "entry_count": len(cut_entries),
        "oldest_tombstone_at": min(tombstone_timestamps).isoformat() if tombstone_timestamps else None,
        "newest_tombstone_at": max(tombstone_timestamps).isoformat() if tombstone_timestamps else None,
    }

    shard_payload = {
        "schema_type": "replication_tombstone_shard",
        "schema_version": "1.0",
        "shard_id": shard_id,
        "source_head_path": REPLICATION_TOMBSTONES_REL,
        "cut_at": now.isoformat(),
        "entries": cut_entries,
        "summary": summary,
    }

    shard_rel = f"{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/{shard_id}.json"
    stub_rel = f"{REPLICATION_TOMBSTONE_STUB_DIR_REL}/{shard_id}.json"

    stub_payload = _create_stub(
        family="replication_tombstone",
        shard_id=shard_id,
        payload_path=shard_rel,
        created_at=now,
        source_head_path=REPLICATION_TOMBSTONES_REL,
        summary=summary,
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
    tm["last_cut_at"] = now.isoformat()
    tm["last_cut_entry_count"] = len(cut_entries)
    tm["hot_entry_count"] = len(entries)

    # Compute oldest_hot_tombstone_at
    oldest_hot: datetime | None = None
    for row in entries.values():
        if isinstance(row, dict):
            ts = _parse_iso(row.get("tombstone_at"))
            if ts and (oldest_hot is None or ts < oldest_hot):
                oldest_hot = ts
    tm["oldest_hot_tombstone_at"] = oldest_hot.isoformat() if oldest_hot else None

    head["entries"] = entries

    # Write with rollback
    paths_to_write: list[tuple[Path, str, dict[str, Any]]] = [
        (head_path, REPLICATION_TOMBSTONES_REL, head),
        (safe_path(repo_root, shard_rel), shard_rel, shard_payload),
        (safe_path(repo_root, stub_rel), stub_rel, stub_payload),
    ]

    rollback = _capture_rollback([p for p, _, _ in paths_to_write])
    try:
        for path, _, data in paths_to_write:
            _write_json(path, data)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "replication_tombstones",
        "entries_externalized": len(cut_entries),
        "shard_id": shard_id,
        "shard_path": shard_rel,
        "stub_path": stub_rel,
        "written_paths": [rel for _, rel, _ in paths_to_write],
        "warnings": warnings,
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

        results[family] = result
        all_warnings.extend(result.get("warnings", []))
        written = result.get("written_paths", [])
        all_written.extend(written)

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
    git_warnings: list[str] = []
    if all_written and gm is not None:
        commit_paths = [safe_path(repo_root, rel) for rel in all_written]
        if try_commit_paths(
            paths=commit_paths,
            gm=gm,
            commit_message="registry-lifecycle: maintenance pass",
        ):
            committed_files = list(all_written)
        else:
            git_warnings.append("registry_maintenance_not_durable: data written to disk but not committed to git")

    all_warnings.extend(git_warnings)

    response: dict[str, Any] = {
        "ok": True,
        "families": results,
        "committed_files": committed_files,
        "warnings": all_warnings if all_warnings else [],
    }
    if gm is not None:
        response["latest_commit"] = gm.latest_commit()

    if audit and auth:
        audit(auth, "registry_maintenance", {"families": list(results.keys()), "committed": len(committed_files)})

    return response
