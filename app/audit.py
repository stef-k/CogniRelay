"""Audit log helpers for repository-backed API events."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


class WriteTimeRolloverError(Exception):
    """Raised when write-time rollover fails and the append must not proceed.

    Carries a stable ``code`` for structured error responses.
    """

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _check_write_time_rollover(
    path: Path, rollover_bytes: int, repo_root: Path, gm: Any
) -> None:
    """Cheap stat-based check; triggers rollover only when threshold exceeded.

    Called before appending to the audit log. The check is a single stat()
    call — no lock acquisition unless rollover actually fires.

    Raises :class:`WriteTimeRolloverError` on failure so the append is
    blocked (per spec, the triggering append must fail on rollover failure).
    """
    try:
        size = path.stat().st_size
    except OSError:
        return
    if size < rollover_bytes:
        return

    # Trigger rollover — import lazily to avoid circular imports and keep
    # the common (no-rollover) path as fast as possible.
    from app.segment_history.families import FAMILIES
    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )
    from app.segment_history.manifest import read_manifest as _read_manifest
    from app.segment_history.service import (
        _derive_stream_key,
        _next_segment_id,
        _roll_jsonl_source,
    )

    family = "api_audit"
    config = FAMILIES[family]
    now = datetime.now(timezone.utc)
    rel = str(path.relative_to(repo_root))
    stream_key = _derive_stream_key(family, rel)
    lock_key = f"segment_history:{family}:{stream_key}"
    lock_dir = repo_root / ".locks" / "segment_history"

    try:
        with segment_history_source_lock(lock_key, lock_dir=lock_dir):
            # Check for pending batch residue under lock
            try:
                mf = _read_manifest(repo_root, family)
            except ValueError:
                mf = None
            if mf is not None and rel in mf.get("source_paths", []):
                raise WriteTimeRolloverError(
                    "segment_history_pending_batch_residue",
                    f"A pending batch operation lists this source: {rel}",
                )

            # Re-check under lock
            try:
                if path.stat().st_size < rollover_bytes:
                    return
            except OSError:
                return

            history_dir = repo_root / config.history_dir
            stub_dir = repo_root / config.stub_dir
            segment_id = _next_segment_id(family, stream_key, now, history_dir)
            payload_path = history_dir / f"{segment_id}.jsonl"

            content = path.read_text(encoding="utf-8", errors="replace")
            summary = config.build_summary(content)

            result = _roll_jsonl_source(
                source_path=path,
                payload_path=payload_path,
                family=family,
                segment_id=segment_id,
                stream_key=stream_key,
                rolled_at=now,
                stub_dir=stub_dir,
                summary=summary,
                repo_root=repo_root,
            )
            if result is None:
                _log.warning("Write-time rollover skipped (partial line only) for %s", path)
                return
            _stub, created = result

            # Commit with git serialization lock
            if gm is not None:
                from app.git_locking import repository_mutation_lock

                try:
                    with repository_mutation_lock(repo_root):
                        commit_paths = created + [path]
                        gm.commit_paths(
                            commit_paths,
                            f"segment-history: roll {family} {stream_key}",
                        )
                except Exception:
                    _log.warning("Write-time rollover commit failed for %s", segment_id)
                    # Local writes succeeded; durable=false but append continues
    except SegmentHistoryLockTimeout as exc:
        raise WriteTimeRolloverError(
            "segment_history_source_lock_timeout",
            str(exc),
        ) from exc


def append_audit(
    repo_root: Path, event: str, peer_id: str, detail: dict[str, Any],
    *, rollover_bytes: int = 0, gm: Any = None,
) -> None:
    """Append one structured API audit event to the repository log.

    When *rollover_bytes* > 0 and a *gm* (git manager) is provided,
    a cheap size check triggers write-time rollover before appending.

    Raises :class:`WriteTimeRolloverError` if write-time rollover fails
    (lock timeout, pending batch residue, or rollover local write failure).
    """
    path = repo_root / "logs" / "api_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)

    if rollover_bytes > 0 and gm is not None:
        _check_write_time_rollover(path, rollover_bytes, repo_root, gm)

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "peer_id": peer_id,
        "detail": detail,
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
