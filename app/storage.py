from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ALLOWED_TOP_LEVEL = {
    "journal",
    "essays",
    "projects",
    "memory",
    "messages",
    "peers",
    "snapshots",
    "tasks",
    "patches",
    "runs",
    "index",
    "archive",
    "config",
    "logs",
    "backups",
}


class StorageError(ValueError):
    pass


def safe_path(repo_root: Path, relative_path: str) -> Path:
    if not relative_path or relative_path.startswith("/"):
        raise StorageError("Path must be a non-empty relative path")

    rel = Path(relative_path)
    if rel.parts and rel.parts[0] not in ALLOWED_TOP_LEVEL:
        raise StorageError(f"Top-level path not allowed: {rel.parts[0]}")

    resolved = (repo_root / rel).resolve()
    if repo_root not in resolved.parents and resolved != repo_root:
        raise StorageError("Path escapes repository root")
    return resolved


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def append_jsonl(path: Path, record: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
