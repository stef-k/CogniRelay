from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


TEXT_SUFFIXES = {".md", ".txt", ".json", ".jsonl"}
TAG_REGEX = re.compile(r"#([a-zA-Z0-9_\-]+)")
WORD_REGEX = re.compile(r"[A-Za-z0-9_\-]{2,}")
FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def _snippet(text: str, limit: int = 240) -> str:
    t = " ".join(text.split())
    return t[:limit] + ("..." if len(t) > limit else "")


def _extract_tags(text: str) -> List[str]:
    return sorted(set(TAG_REGEX.findall(text)))


def _extract_words(text: str) -> List[str]:
    return [w.lower() for w in WORD_REGEX.findall(text)]


def _parse_frontmatter(text: str) -> dict[str, Any]:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}
    out: dict[str, Any] = {}
    for line in m.group(1).splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def _iter_text_files(repo_root: Path):
    for path in repo_root.rglob('*'):
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(repo_root)
        except ValueError:
            continue
        if '.git' in path.parts:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        # Skip derived index artifacts to avoid self-referential indexing.
        if rel.parts and rel.parts[0] == 'index':
            continue
        yield path


def _record_for_file(repo_root: Path, path: Path) -> dict[str, Any] | None:
    rel = str(path.relative_to(repo_root))
    try:
        content = path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return None

    fm = _parse_frontmatter(content) if path.suffix.lower() == '.md' else {}
    file_type = str(fm.get('type') or (Path(rel).parts[0] if Path(rel).parts else 'unknown'))
    importance = None
    try:
        if 'importance' in fm:
            importance = float(str(fm['importance']).strip())
    except Exception:
        importance = None

    stat = path.stat()
    return {
        'path': rel,
        'type': file_type,
        'size': stat.st_size,
        'modified_at': datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        'mtime_ns': getattr(stat, 'st_mtime_ns', int(stat.st_mtime * 1e9)),
        'snippet': _snippet(content),
        'tags': _extract_tags(content),
        'importance': importance,
    }


def _index_dir(repo_root: Path) -> Path:
    d = repo_root / 'index'
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sqlite_path(repo_root: Path) -> Path:
    return _index_dir(repo_root) / 'search.db'


def _ensure_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute('PRAGMA journal_mode=DELETE;')
    conn.execute('CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, type TEXT, modified_at TEXT, mtime_ns INTEGER, size INTEGER, importance REAL, snippet TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS tags (tag TEXT, path TEXT, PRIMARY KEY(tag, path))')
    conn.execute('CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(path, type, snippet, content="")')


def _upsert_sqlite(repo_root: Path, records: list[dict[str, Any]], removed_paths: list[str] | None = None) -> None:
    dbp = _sqlite_path(repo_root)
    conn = sqlite3.connect(dbp)
    try:
        _ensure_sqlite(conn)
        if removed_paths:
            conn.executemany('DELETE FROM files WHERE path = ?', [(p,) for p in removed_paths])
            conn.executemany('DELETE FROM tags WHERE path = ?', [(p,) for p in removed_paths])
            conn.executemany('DELETE FROM files_fts WHERE path = ?', [(p,) for p in removed_paths])
        for r in records:
            conn.execute(
                'INSERT INTO files(path,type,modified_at,mtime_ns,size,importance,snippet) VALUES(?,?,?,?,?,?,?) '
                'ON CONFLICT(path) DO UPDATE SET type=excluded.type, modified_at=excluded.modified_at, mtime_ns=excluded.mtime_ns, size=excluded.size, importance=excluded.importance, snippet=excluded.snippet',
                (r['path'], r['type'], r['modified_at'], r['mtime_ns'], r['size'], r.get('importance'), r['snippet'])
            )
            conn.execute('DELETE FROM tags WHERE path = ?', (r['path'],))
            conn.executemany('INSERT OR IGNORE INTO tags(tag,path) VALUES(?,?)', [(t, r['path']) for t in r.get('tags', [])])
            conn.execute('DELETE FROM files_fts WHERE path = ?', (r['path'],))
            conn.execute('INSERT INTO files_fts(path,type,snippet) VALUES(?,?,?)', (r['path'], r['type'], r['snippet']))
        conn.commit()
    finally:
        conn.close()


def _write_json_indexes(repo_root: Path, files: list[dict[str, Any]]) -> dict[str, Any]:
    tags_map: Dict[str, List[str]] = {}
    words_map: Dict[str, List[str]] = {}
    type_map: Dict[str, List[str]] = {}
    mtime_map: Dict[str, int] = {}
    for r in files:
        type_map.setdefault(r['type'], []).append(r['path'])
        for tag in r.get('tags', []):
            tags_map.setdefault(tag, []).append(r['path'])
        # lightweight word index for fallback/simple lookups
        for w in set(_extract_words((r.get('path','') + ' ' + r.get('snippet','')))):
            if len(words_map.setdefault(w, [])) < 200:
                words_map[w].append(r['path'])
        mtime_map[r['path']] = int(r.get('mtime_ns') or 0)

    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'file_count': len(files),
        'files': sorted([{k:v for k,v in r.items() if k != 'mtime_ns'} for r in files], key=lambda x: x['path']),
    }
    idx = _index_dir(repo_root)
    (idx / 'files_index.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    (idx / 'tags_index.json').write_text(json.dumps(tags_map, ensure_ascii=False, indent=2), encoding='utf-8')
    (idx / 'words_index.json').write_text(json.dumps(words_map, ensure_ascii=False, indent=2), encoding='utf-8')
    (idx / 'types_index.json').write_text(json.dumps(type_map, ensure_ascii=False, indent=2), encoding='utf-8')
    (idx / 'index_state.json').write_text(json.dumps({'generated_at': payload['generated_at'], 'mtime_ns_by_path': mtime_map}, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def rebuild_index(repo_root: Path) -> Dict[str, Any]:
    records = []
    for p in _iter_text_files(repo_root):
        r = _record_for_file(repo_root, p)
        if r:
            records.append(r)
    _upsert_sqlite(repo_root, records, removed_paths=None)
    return _write_json_indexes(repo_root, records)


def load_files_index(repo_root: Path) -> Dict[str, Any]:
    p = repo_root / 'index' / 'files_index.json'
    if not p.exists():
        return {'generated_at': None, 'file_count': 0, 'files': []}
    return json.loads(p.read_text(encoding='utf-8'))


def _parse_modified_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def filter_results_by_time_window(
    results: List[Dict[str, Any]],
    time_window_days: int | None = None,
    time_window_hours: int | None = None,
) -> List[Dict[str, Any]]:
    cutoff: datetime | None = None
    now = datetime.now(timezone.utc)
    if time_window_hours is not None:
        cutoff = now - timedelta(hours=time_window_hours)
    elif time_window_days is not None:
        cutoff = now - timedelta(days=time_window_days)
    if cutoff is None:
        return list(results)
    out: List[Dict[str, Any]] = []
    for row in results:
        modified_at = _parse_modified_at(row.get('modified_at'))
        if modified_at is None or modified_at < cutoff:
            continue
        out.append(row)
    return out


def sort_results_by_recent(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        results,
        key=lambda x: (
            -((_parse_modified_at(x.get('modified_at')) or datetime.fromtimestamp(0, tz=timezone.utc)).timestamp()),
            str(x.get('path', '')),
        ),
    )


def list_recent_files(
    repo_root: Path,
    limit: int = 10,
    include_types: list[str] | None = None,
    time_window_days: int | None = None,
    time_window_hours: int | None = None,
) -> List[Dict[str, Any]]:
    include_set = {t.lower() for t in (include_types or []) if t}

    out: List[Dict[str, Any]] = []
    for row in load_files_index(repo_root).get('files', []):
        if include_set and str(row.get('type') or '').lower() not in include_set:
            continue
        out.append({**row, 'score': row.get('importance')})
    out = filter_results_by_time_window(out, time_window_days=time_window_days, time_window_hours=time_window_hours)
    out = sort_results_by_recent(out)
    return out[: max(1, min(limit, 100))]


def _load_index_state(repo_root: Path) -> dict[str, Any]:
    p = repo_root / 'index' / 'index_state.json'
    if not p.exists():
        return {'mtime_ns_by_path': {}}
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {'mtime_ns_by_path': {}}


def incremental_rebuild_index(repo_root: Path) -> dict[str, Any]:
    prev = _load_index_state(repo_root).get('mtime_ns_by_path', {}) or {}
    current_records: list[dict[str, Any]] = []
    changed_records: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for p in _iter_text_files(repo_root):
        r = _record_for_file(repo_root, p)
        if not r:
            continue
        current_records.append(r)
        seen_paths.add(r['path'])
        if int(r.get('mtime_ns') or 0) != int(prev.get(r['path']) or 0):
            changed_records.append(r)
    removed = [path for path in prev.keys() if path not in seen_paths]
    _upsert_sqlite(repo_root, changed_records, removed_paths=removed)
    payload = _write_json_indexes(repo_root, current_records)
    payload['incremental'] = {
        'changed_count': len(changed_records),
        'removed_count': len(removed),
        'unchanged_count': max(0, len(current_records) - len(changed_records)),
    }
    return payload


def search_index(
    repo_root: Path,
    query: str,
    limit: int = 10,
    include_types: list[str] | None = None,
    sort_by: str = "relevance",
    time_window_days: int | None = None,
    time_window_hours: int | None = None,
) -> List[Dict[str, Any]]:
    include_set = {t.lower() for t in (include_types or []) if t}
    # Try SQLite FTS first
    dbp = _sqlite_path(repo_root)
    if dbp.exists():
        try:
            conn = sqlite3.connect(dbp)
            conn.row_factory = sqlite3.Row
            try:
                q_terms = [t.lower() for t in WORD_REGEX.findall(query)]
                if q_terms:
                    fts_query = ' OR '.join(q_terms)
                    cutoff = None
                    params: list[Any] = [fts_query]
                    where = 'WHERE files_fts MATCH ?'
                    if time_window_hours is not None:
                        cutoff = (datetime.now(timezone.utc) - timedelta(hours=time_window_hours)).isoformat()
                    elif time_window_days is not None:
                        cutoff = (datetime.now(timezone.utc) - timedelta(days=time_window_days)).isoformat()
                    if cutoff is not None:
                        where += ' AND f.modified_at >= ?'
                        params.append(cutoff)
                    order = 'ORDER BY f.modified_at DESC, f.path ASC' if sort_by == "recent" else 'ORDER BY rank ASC, f.path ASC'
                    params.append(max(1, min(limit, 100)))
                    rows = conn.execute(
                        'SELECT f.path, f.type, f.size, f.modified_at, f.snippet, f.importance, bm25(files_fts) as rank '
                        f'FROM files_fts JOIN files f ON f.path = files_fts.path {where} {order} LIMIT ?',
                        tuple(params)
                    ).fetchall()
                else:
                    rows = []
                out = []
                for row in rows:
                    if include_set and str(row['type'] or '').lower() not in include_set:
                        continue
                    score = round(float(-(row['rank'] or 0.0)), 6)
                    if row['importance'] is not None:
                        try:
                            score += float(row['importance'])
                        except Exception:
                            pass
                    out.append({
                        'path': row['path'], 'type': row['type'], 'size': row['size'], 'modified_at': row['modified_at'],
                        'snippet': row['snippet'], 'importance': row['importance'], 'score': score,
                    })
                if sort_by == "recent":
                    out = sort_results_by_recent(out)
                else:
                    out.sort(key=lambda x: (-float(x.get('score',0)), x['path']))
                if out:
                    return out[: max(1, min(limit, 100))]
            finally:
                conn.close()
        except Exception:
            pass

    # Fallback JSON index scoring
    query_terms = [t.lower() for t in WORD_REGEX.findall(query)]
    files_index = load_files_index(repo_root)
    results: List[Dict[str, Any]] = []
    for f in files_index.get('files', []):
        if include_set and str(f.get('type', '')).lower() not in include_set:
            continue
        hay = (f.get('path', '') + ' ' + f.get('snippet', '')).lower()
        score = 0.0
        score += sum(2 for t in query_terms if t in f.get('path', '').lower())
        score += sum(1 for t in query_terms if t in hay)
        if f.get('importance') is not None:
            try:
                score += float(f['importance'])
            except Exception:
                pass
        if score > 0:
            results.append({**f, 'score': round(score, 3)})
    results = filter_results_by_time_window(results, time_window_days=time_window_days, time_window_hours=time_window_hours)
    if sort_by == "recent":
        results = sort_results_by_recent(results)
    else:
        results.sort(key=lambda x: (-x['score'], x['path']))
    return results[: max(1, min(limit, 100))]
