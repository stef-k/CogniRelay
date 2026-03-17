"""SQLite sidecar index for bounded coordination query services.

Provides O(log N) query performance for handoff, shared, and reconciliation
artifact discovery, replacing the O(N) full-directory-scan pattern.  The SQLite
database lives alongside the filesystem artifacts and is rebuilt from disk on
startup.  Incremental updates keep it current between restarts.

Thread safety: uses WAL journal mode so concurrent reads proceed without
blocking during writes.  Mutation endpoints are already serialised by
``artifact_lock``, so write contention is minimal.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timestamp helper — mirrors the sort-key logic in the service modules
# ---------------------------------------------------------------------------


def _parse_ts(value: str | None) -> float:
    """Parse an ISO-8601 timestamp string into a POSIX float for sorting.

    Returns ``0.0`` for *None* or malformed values so that unparseable
    timestamps sort last (matching the existing degraded-sort behaviour).
    """
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS handoff_index (
    handoff_id       TEXT PRIMARY KEY,
    sender_peer      TEXT NOT NULL,
    recipient_peer   TEXT NOT NULL,
    recipient_status TEXT NOT NULL DEFAULT 'pending',
    created_at       TEXT NOT NULL,
    created_at_ts    REAL NOT NULL,
    task_id          TEXT,
    thread_id        TEXT
);
CREATE INDEX IF NOT EXISTS idx_handoff_sender    ON handoff_index(sender_peer);
CREATE INDEX IF NOT EXISTS idx_handoff_recipient ON handoff_index(recipient_peer);
CREATE INDEX IF NOT EXISTS idx_handoff_status    ON handoff_index(recipient_status);

CREATE TABLE IF NOT EXISTS shared_index (
    shared_id    TEXT PRIMARY KEY,
    owner_peer   TEXT NOT NULL,
    task_id      TEXT,
    thread_id    TEXT,
    updated_at   TEXT NOT NULL,
    updated_at_ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shared_owner ON shared_index(owner_peer);
CREATE INDEX IF NOT EXISTS idx_shared_task  ON shared_index(task_id);

-- Junction table: one row per participant peer per shared artifact.
CREATE TABLE IF NOT EXISTS shared_participants (
    shared_id        TEXT NOT NULL,
    participant_peer TEXT NOT NULL,
    PRIMARY KEY (shared_id, participant_peer),
    FOREIGN KEY (shared_id) REFERENCES shared_index(shared_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_shared_participant ON shared_participants(participant_peer);

CREATE TABLE IF NOT EXISTS reconciliation_index (
    reconciliation_id TEXT PRIMARY KEY,
    owner_peer        TEXT NOT NULL,
    status            TEXT NOT NULL,
    classification    TEXT NOT NULL,
    task_id           TEXT,
    thread_id         TEXT,
    updated_at        TEXT NOT NULL,
    updated_at_ts     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recon_owner  ON reconciliation_index(owner_peer);
CREATE INDEX IF NOT EXISTS idx_recon_status ON reconciliation_index(status);

-- Junction table: one row per claimant peer per reconciliation artifact.
CREATE TABLE IF NOT EXISTS reconciliation_claimants (
    reconciliation_id TEXT NOT NULL,
    claimant_peer     TEXT NOT NULL,
    PRIMARY KEY (reconciliation_id, claimant_peer),
    FOREIGN KEY (reconciliation_id) REFERENCES reconciliation_index(reconciliation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_recon_claimant ON reconciliation_claimants(claimant_peer);
"""


# ---------------------------------------------------------------------------
# CoordinationQueryIndex
# ---------------------------------------------------------------------------


class CoordinationQueryIndex:
    """SQLite-backed query index for coordination artifacts.

    The index stores only the fields required for filtering, sorting, and
    pagination.  Full artifact payloads are loaded from disk only for the
    page window returned by the query.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Created if it does not exist.
        Parent directories must already exist.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        try:
            self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_SCHEMA_SQL)
            self._conn.commit()
        except Exception:
            _log.exception("Failed to initialise coordination query index at %s", db_path)
            self._conn = None

    # -- availability -------------------------------------------------------

    @property
    def is_available(self) -> bool:
        """Return whether the index database is open and usable."""
        return self._conn is not None

    # -- rebuild (startup) --------------------------------------------------

    def rebuild_handoffs(self, directory: Path) -> int:
        """Rebuild the handoff index table from filesystem JSON files.

        Clears existing rows, scans ``directory`` for ``.json`` files, and
        inserts lightweight index entries using ``json.loads`` only (no
        Pydantic validation) for speed.  Runs in a single transaction.

        Returns the number of indexed entries.
        """
        if self._conn is None:
            return 0
        count = 0
        cur = self._conn.cursor()
        try:
            cur.execute("DELETE FROM handoff_index")
            if directory.exists() and directory.is_dir():
                for path in directory.iterdir():
                    if path.is_dir() or path.suffix.lower() != ".json":
                        continue
                    try:
                        data = json.loads(path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                        _log.warning("Index rebuild: skipping invalid file %s", path.name)
                        continue
                    if not isinstance(data, dict) or "handoff_id" not in data:
                        continue
                    created_at = str(data.get("created_at") or "")
                    cur.execute(
                        "INSERT OR REPLACE INTO handoff_index"
                        " (handoff_id, sender_peer, recipient_peer, recipient_status,"
                        "  created_at, created_at_ts, task_id, thread_id)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            str(data["handoff_id"]),
                            str(data.get("sender_peer") or ""),
                            str(data.get("recipient_peer") or ""),
                            str(data.get("recipient_status") or "pending"),
                            created_at,
                            _parse_ts(created_at),
                            data.get("task_id"),
                            data.get("thread_id"),
                        ),
                    )
                    count += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            _log.exception("Index rebuild failed for handoffs")
            raise
        _log.info("Handoff index rebuilt: %d entries", count)
        return count

    def rebuild_shared(self, directory: Path) -> int:
        """Rebuild the shared-artifact index tables from filesystem JSON files.

        Populates both ``shared_index`` and the ``shared_participants``
        junction table.  Returns the number of indexed artifacts.
        """
        if self._conn is None:
            return 0
        count = 0
        cur = self._conn.cursor()
        try:
            cur.execute("DELETE FROM shared_participants")
            cur.execute("DELETE FROM shared_index")
            if directory.exists() and directory.is_dir():
                for path in directory.iterdir():
                    if path.is_dir() or path.suffix.lower() != ".json":
                        continue
                    try:
                        data = json.loads(path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                        _log.warning("Index rebuild: skipping invalid file %s", path.name)
                        continue
                    if not isinstance(data, dict) or "shared_id" not in data:
                        continue
                    updated_at = str(data.get("updated_at") or "")
                    shared_id = str(data["shared_id"])
                    cur.execute(
                        "INSERT OR REPLACE INTO shared_index"
                        " (shared_id, owner_peer, task_id, thread_id, updated_at, updated_at_ts)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            shared_id,
                            str(data.get("owner_peer") or ""),
                            data.get("task_id"),
                            data.get("thread_id"),
                            updated_at,
                            _parse_ts(updated_at),
                        ),
                    )
                    # Junction rows for participant membership queries.
                    participants = data.get("participant_peers")
                    if isinstance(participants, list):
                        for peer in participants:
                            if isinstance(peer, str) and peer:
                                cur.execute(
                                    "INSERT OR IGNORE INTO shared_participants"
                                    " (shared_id, participant_peer) VALUES (?, ?)",
                                    (shared_id, peer),
                                )
                    count += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            _log.exception("Index rebuild failed for shared artifacts")
            raise
        _log.info("Shared index rebuilt: %d entries", count)
        return count

    def rebuild_reconciliations(self, directory: Path) -> int:
        """Rebuild the reconciliation index tables from filesystem JSON files.

        Populates both ``reconciliation_index`` and the
        ``reconciliation_claimants`` junction table.  Returns the number of
        indexed artifacts.
        """
        if self._conn is None:
            return 0
        count = 0
        cur = self._conn.cursor()
        try:
            cur.execute("DELETE FROM reconciliation_claimants")
            cur.execute("DELETE FROM reconciliation_index")
            if directory.exists() and directory.is_dir():
                for path in directory.iterdir():
                    if path.is_dir() or path.suffix.lower() != ".json":
                        continue
                    try:
                        data = json.loads(path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                        _log.warning("Index rebuild: skipping invalid file %s", path.name)
                        continue
                    if not isinstance(data, dict) or "reconciliation_id" not in data:
                        continue
                    updated_at = str(data.get("updated_at") or "")
                    recon_id = str(data["reconciliation_id"])
                    cur.execute(
                        "INSERT OR REPLACE INTO reconciliation_index"
                        " (reconciliation_id, owner_peer, status, classification,"
                        "  task_id, thread_id, updated_at, updated_at_ts)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            recon_id,
                            str(data.get("owner_peer") or ""),
                            str(data.get("status") or "open"),
                            str(data.get("classification") or ""),
                            data.get("task_id"),
                            data.get("thread_id"),
                            updated_at,
                            _parse_ts(updated_at),
                        ),
                    )
                    # Junction rows for claimant membership queries.
                    claims = data.get("claims")
                    if isinstance(claims, list):
                        seen: set[str] = set()
                        for claim in claims:
                            if isinstance(claim, dict):
                                cp = claim.get("claimant_peer")
                                if isinstance(cp, str) and cp and cp not in seen:
                                    seen.add(cp)
                                    cur.execute(
                                        "INSERT OR IGNORE INTO reconciliation_claimants"
                                        " (reconciliation_id, claimant_peer) VALUES (?, ?)",
                                        (recon_id, cp),
                                    )
                    count += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            _log.exception("Index rebuild failed for reconciliation artifacts")
            raise
        _log.info("Reconciliation index rebuilt: %d entries", count)
        return count

    # -- upsert (incremental, called after mutations) -----------------------

    def upsert_handoff(self, artifact: dict[str, Any]) -> None:
        """Insert or update one handoff index entry from a full artifact dict.

        Called after a successful filesystem persist (create or consume).
        Failures are logged but never raised — the mutation must not break
        because the index update failed.
        """
        if self._conn is None:
            return
        try:
            created_at = str(artifact.get("created_at") or "")
            self._conn.execute(
                "INSERT OR REPLACE INTO handoff_index"
                " (handoff_id, sender_peer, recipient_peer, recipient_status,"
                "  created_at, created_at_ts, task_id, thread_id)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(artifact["handoff_id"]),
                    str(artifact.get("sender_peer") or ""),
                    str(artifact.get("recipient_peer") or ""),
                    str(artifact.get("recipient_status") or "pending"),
                    created_at,
                    _parse_ts(created_at),
                    artifact.get("task_id"),
                    artifact.get("thread_id"),
                ),
            )
            self._conn.commit()
        except Exception:
            _log.exception("Index upsert failed for handoff %s", artifact.get("handoff_id"))

    def upsert_shared(self, artifact: dict[str, Any]) -> None:
        """Insert or update one shared-artifact index entry and its participants.

        Called after a successful filesystem persist (create or update).
        """
        if self._conn is None:
            return
        try:
            updated_at = str(artifact.get("updated_at") or "")
            shared_id = str(artifact["shared_id"])
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO shared_index"
                " (shared_id, owner_peer, task_id, thread_id, updated_at, updated_at_ts)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    shared_id,
                    str(artifact.get("owner_peer") or ""),
                    artifact.get("task_id"),
                    artifact.get("thread_id"),
                    updated_at,
                    _parse_ts(updated_at),
                ),
            )
            # Replace participant rows — delete then re-insert.
            cur.execute("DELETE FROM shared_participants WHERE shared_id = ?", (shared_id,))
            participants = artifact.get("participant_peers")
            if isinstance(participants, list):
                for peer in participants:
                    if isinstance(peer, str) and peer:
                        cur.execute(
                            "INSERT OR IGNORE INTO shared_participants"
                            " (shared_id, participant_peer) VALUES (?, ?)",
                            (shared_id, peer),
                        )
            self._conn.commit()
        except Exception:
            _log.exception("Index upsert failed for shared %s", artifact.get("shared_id"))

    def upsert_reconciliation(self, artifact: dict[str, Any]) -> None:
        """Insert or update one reconciliation index entry and its claimants.

        Called after a successful filesystem persist (open or resolve).
        """
        if self._conn is None:
            return
        try:
            updated_at = str(artifact.get("updated_at") or "")
            recon_id = str(artifact["reconciliation_id"])
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO reconciliation_index"
                " (reconciliation_id, owner_peer, status, classification,"
                "  task_id, thread_id, updated_at, updated_at_ts)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    recon_id,
                    str(artifact.get("owner_peer") or ""),
                    str(artifact.get("status") or "open"),
                    str(artifact.get("classification") or ""),
                    artifact.get("task_id"),
                    artifact.get("thread_id"),
                    updated_at,
                    _parse_ts(updated_at),
                ),
            )
            # Replace claimant rows — delete then re-insert.
            cur.execute(
                "DELETE FROM reconciliation_claimants WHERE reconciliation_id = ?",
                (recon_id,),
            )
            claims = artifact.get("claims")
            if isinstance(claims, list):
                seen: set[str] = set()
                for claim in claims:
                    if isinstance(claim, dict):
                        cp = claim.get("claimant_peer")
                        if isinstance(cp, str) and cp and cp not in seen:
                            seen.add(cp)
                            cur.execute(
                                "INSERT OR IGNORE INTO reconciliation_claimants"
                                " (reconciliation_id, claimant_peer) VALUES (?, ?)",
                                (recon_id, cp),
                            )
            self._conn.commit()
        except Exception:
            _log.exception(
                "Index upsert failed for reconciliation %s",
                artifact.get("reconciliation_id"),
            )

    # -- query --------------------------------------------------------------

    def query_handoffs(
        self,
        *,
        sender_peer: str | None = None,
        recipient_peer: str | None = None,
        status: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> tuple[list[str], int]:
        """Query the handoff index and return a page of IDs with total count.

        Filters are conjunctive (AND).  Sort order: ``created_at`` descending,
        ``handoff_id`` ascending (matching the existing in-memory sort key).

        Parameters
        ----------
        sender_peer:
            Exact match on ``sender_peer``.
        recipient_peer:
            Exact match on ``recipient_peer``.
        status:
            Exact match on ``recipient_status``.
        offset:
            Number of rows to skip for pagination.
        limit:
            Maximum rows to return.

        Returns
        -------
        tuple:
            ``(list_of_handoff_ids, total_matches)``
        """
        if self._conn is None:
            return [], 0

        conditions: list[str] = []
        params: list[Any] = []
        if sender_peer is not None:
            conditions.append("sender_peer = ?")
            params.append(sender_peer)
        if recipient_peer is not None:
            conditions.append("recipient_peer = ?")
            params.append(recipient_peer)
        if status is not None:
            conditions.append("recipient_status = ?")
            params.append(status)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        # Total count for pagination metadata.
        count_row = self._conn.execute(
            f"SELECT COUNT(*) FROM handoff_index{where}", params,
        ).fetchone()
        total = count_row[0] if count_row else 0

        # Page of IDs, sorted to match existing query behaviour.
        rows = self._conn.execute(
            f"SELECT handoff_id FROM handoff_index{where}"
            " ORDER BY created_at_ts DESC, handoff_id ASC"
            " LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()

        return [r[0] for r in rows], total

    def query_shared(
        self,
        *,
        owner_peer: str | None = None,
        participant_peer: str | None = None,
        task_id: str | None = None,
        thread_id: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> tuple[list[str], int]:
        """Query the shared-artifact index and return a page of IDs with total count.

        The ``participant_peer`` filter uses an EXISTS sub-query against the
        ``shared_participants`` junction table.  Sort order: ``updated_at``
        descending, ``shared_id`` ascending.

        Returns
        -------
        tuple:
            ``(list_of_shared_ids, total_matches)``
        """
        if self._conn is None:
            return [], 0

        conditions: list[str] = []
        params: list[Any] = []
        if owner_peer is not None:
            conditions.append("s.owner_peer = ?")
            params.append(owner_peer)
        if participant_peer is not None:
            # Sub-query: the shared artifact has this peer as a participant.
            conditions.append(
                "EXISTS (SELECT 1 FROM shared_participants sp"
                " WHERE sp.shared_id = s.shared_id AND sp.participant_peer = ?)"
            )
            params.append(participant_peer)
        if task_id is not None:
            conditions.append("s.task_id = ?")
            params.append(task_id)
        if thread_id is not None:
            conditions.append("s.thread_id = ?")
            params.append(thread_id)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        count_row = self._conn.execute(
            f"SELECT COUNT(*) FROM shared_index s{where}", params,
        ).fetchone()
        total = count_row[0] if count_row else 0

        rows = self._conn.execute(
            f"SELECT s.shared_id FROM shared_index s{where}"
            " ORDER BY s.updated_at_ts DESC, s.shared_id ASC"
            " LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()

        return [r[0] for r in rows], total

    def query_reconciliations(
        self,
        *,
        owner_peer: str | None = None,
        claimant_peer: str | None = None,
        status: str | None = None,
        classification: str | None = None,
        task_id: str | None = None,
        thread_id: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> tuple[list[str], int]:
        """Query the reconciliation index and return a page of IDs with total count.

        The ``claimant_peer`` filter uses an EXISTS sub-query against the
        ``reconciliation_claimants`` junction table.  Sort order:
        ``updated_at`` descending, ``reconciliation_id`` ascending.

        Returns
        -------
        tuple:
            ``(list_of_reconciliation_ids, total_matches)``
        """
        if self._conn is None:
            return [], 0

        conditions: list[str] = []
        params: list[Any] = []
        if owner_peer is not None:
            conditions.append("r.owner_peer = ?")
            params.append(owner_peer)
        if claimant_peer is not None:
            # Sub-query: the reconciliation has this peer as a claimant.
            conditions.append(
                "EXISTS (SELECT 1 FROM reconciliation_claimants rc"
                " WHERE rc.reconciliation_id = r.reconciliation_id AND rc.claimant_peer = ?)"
            )
            params.append(claimant_peer)
        if status is not None:
            conditions.append("r.status = ?")
            params.append(status)
        if classification is not None:
            conditions.append("r.classification = ?")
            params.append(classification)
        if task_id is not None:
            conditions.append("r.task_id = ?")
            params.append(task_id)
        if thread_id is not None:
            conditions.append("r.thread_id = ?")
            params.append(thread_id)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        count_row = self._conn.execute(
            f"SELECT COUNT(*) FROM reconciliation_index r{where}", params,
        ).fetchone()
        total = count_row[0] if count_row else 0

        rows = self._conn.execute(
            f"SELECT r.reconciliation_id FROM reconciliation_index r{where}"
            " ORDER BY r.updated_at_ts DESC, r.reconciliation_id ASC"
            " LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()

        return [r[0] for r in rows], total


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_coordination_index: CoordinationQueryIndex | None = None


def set_coordination_index(idx: CoordinationQueryIndex) -> None:
    """Set the module-level coordination query index (called during lifespan startup)."""
    global _coordination_index
    _coordination_index = idx


def get_coordination_index() -> CoordinationQueryIndex | None:
    """Return the current coordination query index, or ``None`` if unavailable."""
    return _coordination_index
