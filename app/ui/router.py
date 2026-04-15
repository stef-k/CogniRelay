"""Server-rendered read-only operator UI routes."""

from __future__ import annotations

import html
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse

from app.auth import AuthContext
from app.config import Settings, get_settings
from app.continuity import continuity_read_service
from app.continuity.listing import (
    _scan_active_summaries,
    _scan_archive_summaries,
    _scan_cold_summaries,
    _scan_fallback_summaries,
)
from app.discovery import capabilities_payload, health_payload
from app.git_manager import GitManager
from app.models import ContinuityReadRequest

from .render import render_template

UI_SUBJECT_KINDS: tuple[str, ...] = ("user", "peer", "thread", "task")
UI_ARTIFACT_STATES: tuple[str, ...] = ("active", "fallback", "archived", "cold")
UI_HEALTH_STATUSES: tuple[str, ...] = ("healthy", "degraded", "conflicted")
UI_CONTINUITY_DISPLAY_LIMIT = 200
_STATIC_DIR = Path(__file__).resolve().parent / "static"


def build_ui_router(*, app_version: str) -> APIRouter:
    """Build the optional server-rendered operator UI router."""
    router = APIRouter(prefix="/ui")

    @router.get("/static/{asset_name}")
    def ui_static(request: Request, asset_name: str) -> FileResponse:
        """Serve one local operator UI asset."""
        settings = get_settings()
        _enforce_ui_access(request, settings)
        asset_path = (_STATIC_DIR / asset_name).resolve()
        if asset_path.parent != _STATIC_DIR or not asset_path.is_file():
            raise HTTPException(status_code=404, detail="UI asset not found")
        return FileResponse(asset_path)

    @router.get("/", response_class=HTMLResponse)
    def ui_overview(request: Request) -> HTMLResponse:
        """Render the operator overview page."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        gm = _ui_git_manager(settings)
        auth = _ui_auth(client_ip)
        now = datetime.now(timezone.utc)
        health = health_payload(
            app_version=app_version,
            contract_version=settings.contract_version,
            repo_root=str(settings.repo_root),
            git_initialized=gm.is_repo(),
            latest_commit=gm.latest_commit(),
            signed_ingress_required=bool(settings.require_signed_ingress),
        )
        capabilities = capabilities_payload()
        continuity_counts = _continuity_counts(repo_root=settings.repo_root, auth=auth, now=now)
        security_rows = [
            ("UI enabled", "true"),
            ("Require localhost", _bool_label(settings.ui_require_localhost)),
            ("Configured read only", _bool_label(settings.ui_read_only)),
            ("Effective mode", "read-only"),
            ("Resolved client IP", client_ip or "unknown"),
        ]
        body = render_template(
            "overview.html",
            health_rows=_definition_rows(
                [
                    ("Service", str(health.get("service", ""))),
                    ("Version", str(health.get("version", ""))),
                    ("Contract version", str(health.get("contract_version", ""))),
                    ("Git initialized", _bool_label(bool(health.get("git_initialized")))),
                    ("Latest commit", str(health.get("latest_commit") or "none")),
                    ("Signed ingress required", _bool_label(bool(health.get("signed_ingress_required")))),
                    ("Reported at", str(health.get("time", ""))),
                ]
            ),
            ui_rows=_definition_rows(security_rows),
            capability_list=_html_list([str(item) for item in capabilities.get("features", [])]),
            continuity_rows=_definition_rows(
                [
                    ("Active capsules", str(continuity_counts["active"])),
                    ("Fallback snapshots", str(continuity_counts["fallback"])),
                    ("Archived envelopes", str(continuity_counts["archived"])),
                    ("Cold stubs", str(continuity_counts["cold"])),
                    ("User capsules", str(continuity_counts["by_subject_kind"].get("user", 0))),
                    ("Peer capsules", str(continuity_counts["by_subject_kind"].get("peer", 0))),
                    ("Thread capsules", str(continuity_counts["by_subject_kind"].get("thread", 0))),
                    ("Task capsules", str(continuity_counts["by_subject_kind"].get("task", 0))),
                ]
            ),
        )
        return _page(
            title="Operator Overview",
            current_path="/ui/",
            content=body,
        )

    @router.get("/continuity", response_class=HTMLResponse)
    def ui_continuity(
        request: Request,
        q: str | None = Query(default=None),
        subject_kind: Literal["user", "peer", "thread", "task"] | None = Query(default=None),
        artifact_state: Literal["active", "fallback", "archived", "cold"] | None = Query(default=None),
        health_status: Literal["healthy", "degraded", "conflicted"] | None = Query(default=None),
    ) -> HTMLResponse:
        """Render the continuity list view."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        _gm = _ui_git_manager(settings)
        auth = _ui_auth(client_ip)
        now = datetime.now(timezone.utc)
        all_rows = _ui_continuity_rows(
            repo_root=settings.repo_root,
            auth=auth,
            subject_kind=subject_kind,
            artifact_state=None,
            now=now,
            retention_archive_days=settings.continuity_retention_archive_days,
        )
        scoped_rows = _filter_rows_by_query_and_health(all_rows, q=q, health_status=health_status)
        lifecycle_counts = _artifact_state_counts(scoped_rows)
        filtered_rows = _filter_rows_by_artifact_state(scoped_rows, artifact_state)
        display_rows = filtered_rows[:UI_CONTINUITY_DISPLAY_LIMIT]
        rows = []
        for capsule in display_rows:
            rows.append(
                [
                    html.escape(str(capsule.get("subject_kind", ""))),
                    _subject_link(str(capsule.get("subject_kind", "")), str(capsule.get("subject_id", ""))),
                    _lifecycle_badges(capsule),
                    _artifact_location_cell(capsule),
                    html.escape(_display_recorded_at(capsule)),
                    html.escape(str(capsule.get("health_status", ""))),
                    _signal_counts_cell(capsule),
                ]
            )
        filter_options = "".join(
            _option_row(value=kind, selected=(kind == subject_kind))
            for kind in UI_SUBJECT_KINDS
        )
        artifact_options = "".join(
            _option_row(value=state, selected=(state == artifact_state))
            for state in UI_ARTIFACT_STATES
        )
        health_options = "".join(
            _option_row(value=status, selected=(status == health_status))
            for status in UI_HEALTH_STATUSES
        )
        body = render_template(
            "continuity_list.html",
            query_value=html.escape(q or ""),
            selected_kind=html.escape(subject_kind or "all kinds"),
            selected_artifact_state=html.escape(artifact_state or "all lifecycle states"),
            selected_health_status=html.escape(health_status or "all health states"),
            filter_options=filter_options,
            artifact_options=artifact_options,
            health_options=health_options,
            displayed_count=str(len(display_rows)),
            matched_count=str(len(filtered_rows)),
            result_truncated=_bool_label(len(filtered_rows) > UI_CONTINUITY_DISPLAY_LIMIT),
            active_count=str(lifecycle_counts["active"]),
            fallback_count=str(lifecycle_counts["fallback"]),
            archived_count=str(lifecycle_counts["archived"]),
            cold_count=str(lifecycle_counts["cold"]),
            continuity_table=_html_table(
                headers=[
                    "Kind",
                    "Subject",
                    "Lifecycle",
                    "Artifact",
                    "Recorded",
                    "Health",
                    "Signals",
                ],
                rows=rows,
                empty_message="No continuity capsules matched the current filter.",
            ),
        )
        return _page(title="Continuity Capsules", current_path="/ui/continuity", content=body)

    @router.get("/continuity/{subject_kind}/{subject_id}", response_class=HTMLResponse)
    def ui_continuity_detail(
        request: Request,
        subject_kind: Literal["user", "peer", "thread", "task"],
        subject_id: str,
    ) -> HTMLResponse:
        """Render one continuity detail page with graceful degradation."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        _gm = _ui_git_manager(settings)
        auth = _ui_auth(client_ip)
        detail = continuity_read_service(
            repo_root=settings.repo_root,
            auth=auth,
            req=ContinuityReadRequest(
                subject_kind=subject_kind,
                subject_id=subject_id,
                allow_fallback=True,
                view="startup",
            ),
            now=datetime.now(timezone.utc),
            audit=_noop_audit,
        )
        capsule = detail.get("capsule") or {}
        continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
        startup_summary = detail.get("startup_summary")
        trust_signals = detail.get("trust_signals")
        related_rows = _related_artifact_rows(
            repo_root=settings.repo_root,
            auth=auth,
            subject_kind=subject_kind,
            subject_id=subject_id,
            now=datetime.now(timezone.utc),
            retention_archive_days=settings.continuity_retention_archive_days,
        )
        related_summary = _related_artifact_summary_rows(subject_kind=subject_kind, rows=related_rows)
        detail_sections = {
            "top_priorities": _html_list(_coerce_str_list(continuity.get("top_priorities"))),
            "active_concerns": _html_list(_coerce_str_list(continuity.get("active_concerns"))),
            "active_constraints": _html_list(_coerce_str_list(continuity.get("active_constraints"))),
            "open_loops": _html_list(_coerce_str_list(continuity.get("open_loops"))),
            "session_trajectory": _html_list(_coerce_str_list(continuity.get("session_trajectory"))),
            "stance_summary": _paragraph(continuity.get("stance_summary")),
        }
        body = render_template(
            "continuity_detail.html",
            subject_kind=html.escape(subject_kind),
            subject_id=html.escape(subject_id),
            source_state=html.escape(str(detail.get("source_state", "unknown"))),
            recovery_warnings=_html_list([str(item) for item in detail.get("recovery_warnings", [])]),
            capsule_meta_rows=_definition_rows(
                [
                    ("Path", str(detail.get("path", ""))),
                    ("Source state", str(detail.get("source_state", "unknown"))),
                    ("Archived", _bool_label(bool(detail.get("archived")))),
                    ("Fallback snapshot present", _bool_label(any(row["artifact_state"] == "fallback" for row in related_rows))),
                    ("Archived artifacts present", _bool_label(any(row["artifact_state"] == "archived" for row in related_rows))),
                    ("Cold artifacts present", _bool_label(any(row["artifact_state"] == "cold" for row in related_rows))),
                    ("Updated at", str(capsule.get("updated_at") or "n/a")),
                    ("Verified at", str(capsule.get("verified_at") or "n/a")),
                    ("Verification kind", str(capsule.get("verification_kind") or "n/a")),
                ]
            ),
            related_artifact_rows=_html_table(
                headers=["Lifecycle", "Present", "Count", "Latest artifact", "Recorded", "Browse"],
                rows=related_summary,
                empty_message="No related lifecycle artifacts were found for this subject.",
            ),
            startup_summary_html=_render_object(startup_summary, empty_message="Startup summary unavailable."),
            trust_signals_html=_render_object(trust_signals, empty_message="Trust signals unavailable."),
            top_priorities_html=detail_sections["top_priorities"],
            active_concerns_html=detail_sections["active_concerns"],
            active_constraints_html=detail_sections["active_constraints"],
            open_loops_html=detail_sections["open_loops"],
            session_trajectory_html=detail_sections["session_trajectory"],
            stance_summary_html=detail_sections["stance_summary"],
            stable_preferences_html=_structured_table(
                rows=list(capsule.get("stable_preferences") or []),
                columns=["tag", "content", "created_at", "updated_at", "last_confirmed_at"],
                empty_message="No stable preferences recorded.",
            ),
            negative_decisions_html=_structured_table(
                rows=list(continuity.get("negative_decisions") or []),
                columns=["decision", "rationale", "created_at", "updated_at", "last_confirmed_at"],
                empty_message="No negative decisions recorded.",
            ),
            rationale_entries_html=_structured_table(
                rows=list(continuity.get("rationale_entries") or []),
                columns=["tag", "kind", "status", "summary", "reasoning", "updated_at"],
                empty_message="No rationale entries recorded.",
            ),
        )
        return _page(
            title=f"Continuity Detail: {subject_kind}/{subject_id}",
            current_path="/ui/continuity",
            content=body,
        )

    return router


def _ui_git_manager(settings: Settings) -> GitManager:
    """Build a git manager for read-only metadata access without repo initialization."""
    return GitManager(
        repo_root=settings.repo_root,
        author_name=settings.git_author_name,
        author_email=settings.git_author_email,
    )


def _ui_auth(client_ip: str | None) -> AuthContext:
    """Build a read-only internal auth context for UI read paths."""
    return AuthContext(
        token="ui-operator",
        peer_id="ui-operator",
        scopes={"read:files"},
        read_namespaces={"*"},
        write_namespaces=set(),
        client_ip=client_ip,
    )


def _enforce_ui_access(request: Request, settings: Settings) -> str | None:
    """Enforce the optional local-only UI policy."""
    client_ip = _ui_transport_client_ip(request)
    if settings.ui_require_localhost and not _is_local_client_ip(client_ip):
        raise HTTPException(status_code=403, detail="Operator UI is local-only")
    return client_ip


def _ui_transport_client_ip(request: Request) -> str | None:
    """Return the transport peer host for strict UI localhost enforcement."""
    if request.client is None or request.client.host is None:
        return None
    value = str(request.client.host).strip()
    if not value:
        return None
    return value


def _is_local_client_ip(client_ip: str | None) -> bool:
    """Return whether the resolved client IP belongs to localhost."""
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


def _page(*, title: str, current_path: str, content: str) -> HTMLResponse:
    """Render one full HTML document."""
    html_doc = render_template(
        "layout.html",
        title=html.escape(title),
        overview_nav_class=_nav_class(current_path == "/ui/"),
        continuity_nav_class=_nav_class(current_path.startswith("/ui/continuity")),
        content=content,
    )
    return HTMLResponse(html_doc)


def _nav_class(active: bool) -> str:
    """Return the nav link class for the current page."""
    return "nav-link active" if active else "nav-link"


def _bool_label(value: bool) -> str:
    """Return a lowercase boolean label for human-readable tables."""
    return "true" if value else "false"


def _artifact_state_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Count continuity rows by lifecycle state for list-view summaries."""
    counts = {state: 0 for state in UI_ARTIFACT_STATES}
    for row in rows:
        state = str(row.get("artifact_state") or "")
        if state in counts:
            counts[state] += 1
    return counts


def _ui_continuity_rows(
    *,
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    artifact_state: str | None,
    now: datetime,
    retention_archive_days: int,
) -> list[dict[str, Any]]:
    """Collect lifecycle rows for the UI without pre-filter truncation."""
    states = [artifact_state] if artifact_state in UI_ARTIFACT_STATES else list(UI_ARTIFACT_STATES)
    rows: list[dict[str, Any]] = []
    for state in states:
        if state == "active":
            rows.extend(_scan_active_summaries(repo_root, auth, subject_kind, now))
        elif state == "fallback":
            rows.extend(_scan_fallback_summaries(repo_root, auth, subject_kind, now))
        elif state == "archived":
            rows.extend(_scan_archive_summaries(repo_root, auth, subject_kind, now, retention_archive_days))
        elif state == "cold":
            rows.extend(_scan_cold_summaries(repo_root, auth, subject_kind))
    artifact_order = {state: idx for idx, state in enumerate(UI_ARTIFACT_STATES)}
    rows.sort(
        key=lambda row: (
            str(row.get("subject_kind")),
            str(row.get("subject_id")),
            artifact_order.get(str(row.get("artifact_state")), 99),
            str(_primary_artifact_path(row)),
        )
    )
    return rows


def _filter_rows_by_artifact_state(
    rows: list[dict[str, Any]],
    artifact_state: str | None,
) -> list[dict[str, Any]]:
    """Apply the optional lifecycle filter used by the continuity list page."""
    if artifact_state is None:
        return rows
    return [row for row in rows if row.get("artifact_state") == artifact_state]


def _filter_rows_by_query_and_health(
    rows: list[dict[str, Any]],
    *,
    q: str | None,
    health_status: str | None,
) -> list[dict[str, Any]]:
    """Apply explicit server-side query and health filters.

    Query matching is deterministic and bounded:
    - trim and lowercase the query
    - split on whitespace into tokens
    - each token must appear as a substring in at least one fixed searchable field
    """
    tokens = _search_tokens(q)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if health_status is not None and row.get("health_status") != health_status:
            continue
        if tokens and not _row_matches_query(row, tokens):
            continue
        filtered.append(row)
    return filtered


def _search_tokens(value: str | None) -> list[str]:
    """Normalize one search query into bounded lowercase tokens."""
    if value is None:
        return []
    return [token for token in value.strip().lower().split() if token]


def _row_matches_query(row: dict[str, Any], tokens: list[str]) -> bool:
    """Return whether every token matches one of the row's searchable fields."""
    fields = _row_search_fields(row)
    return all(any(token in field for field in fields) for token in tokens)


def _row_search_fields(row: dict[str, Any]) -> list[str]:
    """Return the explicit fields searchable from continuity list rows."""
    values = [
        row.get("subject_kind"),
        row.get("subject_id"),
        row.get("artifact_state"),
        row.get("health_status"),
        row.get("phase"),
        row.get("verification_status"),
        row.get("freshness_class"),
        row.get("verification_kind"),
        row.get("retention_class"),
        row.get("path"),
        row.get("archive_path"),
        row.get("cold_stub_path"),
        row.get("source_archive_path"),
        row.get("cold_storage_path"),
    ]
    fields: list[str] = []
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip().lower()
        if normalized:
            fields.append(normalized)
    return fields


def _continuity_counts(*, repo_root: Path, auth: AuthContext, now: datetime) -> dict[str, Any]:
    """Collect cheap high-level continuity counts for the overview page."""
    active = _scan_active_summaries(repo_root, auth, None, now)
    fallback = _scan_fallback_summaries(repo_root, auth, None, now)
    archived = _scan_archive_summaries(repo_root, auth, None, now, get_settings().continuity_retention_archive_days)
    cold = _scan_cold_summaries(repo_root, auth, None)
    kind_counts = Counter(str(item.get("subject_kind", "")) for item in active)
    return {
        "active": len(active),
        "fallback": len(fallback),
        "archived": len(archived),
        "cold": len(cold),
        "by_subject_kind": dict(kind_counts),
    }


def _noop_audit(_auth: AuthContext, _event: str, _detail: dict[str, Any]) -> None:
    """Skip audit writes for read-only UI rendering paths."""
    return None


def _definition_rows(rows: list[tuple[str, str]]) -> str:
    """Render key/value rows as a definition list."""
    parts = ['<dl class="kv-list">']
    for label, value in rows:
        parts.append(f"<dt>{html.escape(label)}</dt><dd>{html.escape(value)}</dd>")
    parts.append("</dl>")
    return "".join(parts)


def _html_list(items: list[str]) -> str:
    """Render a list of strings as a bullet list."""
    if not items:
        return '<p class="muted">None</p>'
    return "<ul>" + "".join(f"<li>{html.escape(item)}</li>" for item in items) + "</ul>"


def _html_table(*, headers: list[str], rows: list[list[str]], empty_message: str) -> str:
    """Render a deterministic HTML table."""
    if not rows:
        return f'<p class="muted">{html.escape(empty_message)}</p>'
    head = "".join(f"<th>{html.escape(label)}</th>" for label in headers)
    body_rows = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body_rows}</tbody></table>"


def _subject_link(subject_kind: str, subject_id: str) -> str:
    """Render the continuity detail link for one subject."""
    href = f"/ui/continuity/{quote(subject_kind, safe='')}/{quote(subject_id, safe='')}"
    return f'<a href="{href}">{html.escape(subject_id)}</a>'


def _option_row(*, value: str, selected: bool) -> str:
    """Render one filter option."""
    selected_attr = ' selected="selected"' if selected else ""
    return f'<option value="{html.escape(value)}"{selected_attr}>{html.escape(value)}</option>'


def _badge(*, label: str, tone: str = "default") -> str:
    """Render a compact badge label."""
    safe_label = html.escape(label)
    safe_tone = html.escape(tone)
    return f'<span class="badge badge-{safe_tone}">{safe_label}</span>'


def _lifecycle_badges(row: dict[str, Any]) -> str:
    """Render lifecycle-related badges for one continuity row."""
    state = str(row.get("artifact_state") or "unknown")
    parts = [_badge(label=state, tone=state)]
    retention_class = str(row.get("retention_class") or "")
    if retention_class and retention_class not in {"active", "fallback", "cold"}:
        parts.append(_badge(label=retention_class.replace("_", " "), tone="retention"))
    return '<div class="badge-row">' + "".join(parts) + "</div>"


def _artifact_location_cell(row: dict[str, Any]) -> str:
    """Render the most useful artifact path metadata for one continuity row."""
    parts = []
    state = str(row.get("artifact_state") or "")
    primary_path = _primary_artifact_path(row)
    if primary_path:
        parts.append(f"<div>{html.escape(primary_path)}</div>")
    if state == "cold" and row.get("source_archive_path"):
        parts.append(f'<div class="muted">archive: {html.escape(str(row.get("source_archive_path")))}</div>')
    elif state == "archived" and row.get("path"):
        parts.append(f'<div class="muted">active path: {html.escape(str(row.get("path")))}</div>')
    return "".join(parts) or '<span class="muted">n/a</span>'


def _primary_artifact_path(row: dict[str, Any]) -> str:
    """Return the most relevant artifact path for one continuity row."""
    state = str(row.get("artifact_state") or "")
    if state == "archived":
        return str(row.get("archive_path") or row.get("path") or "")
    if state == "cold":
        return str(row.get("cold_stub_path") or row.get("path") or "")
    return str(row.get("path") or "")


def _display_recorded_at(row: dict[str, Any]) -> str:
    """Return the best lifecycle timestamp to show for one row."""
    state = str(row.get("artifact_state") or "")
    if state == "cold":
        return str(row.get("cold_stored_at") or row.get("archived_at") or "n/a")
    if state == "archived":
        return str(row.get("archived_at") or row.get("updated_at") or "n/a")
    return str(row.get("updated_at") or "n/a")


def _signal_counts_cell(row: dict[str, Any]) -> str:
    """Render stable-preference and rationale-entry counts for one row."""
    stable = row.get("stable_preference_count")
    rationale = row.get("rationale_entry_count")
    stable_label = "n/a" if stable is None else str(stable)
    rationale_label = "n/a" if rationale is None else str(rationale)
    return html.escape(f"{stable_label} prefs / {rationale_label} rationale")


def _related_artifact_rows(
    *,
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str,
    subject_id: str,
    now: datetime,
    retention_archive_days: int,
) -> list[dict[str, Any]]:
    """Collect all lifecycle rows for one subject using existing continuity scanners."""
    rows = _ui_continuity_rows(
        repo_root=repo_root,
        auth=auth,
        subject_kind=subject_kind,
        artifact_state=None,
        now=now,
        retention_archive_days=retention_archive_days,
    )
    filtered = [row for row in rows if str(row.get("subject_id")) == subject_id]
    return filtered


def _related_artifact_summary_rows(
    *,
    subject_kind: str,
    rows: list[dict[str, Any]],
) -> list[list[str]]:
    """Build a bounded per-lifecycle summary table for the detail page."""
    summary_rows: list[list[str]] = []
    for state in UI_ARTIFACT_STATES:
        matching = [row for row in rows if row.get("artifact_state") == state]
        latest = matching[-1] if matching else None
        browse_href = f"/ui/continuity?subject_kind={quote(subject_kind, safe='')}&artifact_state={quote(state, safe='')}"
        latest_html = '<span class="muted">None</span>'
        recorded = "n/a"
        if latest is not None:
            latest_html = _artifact_location_cell(latest)
            recorded = _display_recorded_at(latest)
        summary_rows.append(
            [
                _lifecycle_badges({"artifact_state": state, "retention_class": latest.get("retention_class") if latest else ""}),
                html.escape(_bool_label(bool(matching))),
                html.escape(str(len(matching))),
                latest_html,
                html.escape(recorded),
                f'<a href="{browse_href}">Open {html.escape(subject_kind)} {html.escape(state)} list</a>',
            ]
        )
    return summary_rows


def _render_object(value: Any, *, empty_message: str) -> str:
    """Render nested JSON-like data as human-readable HTML."""
    if value is None:
        return f'<p class="muted">{html.escape(empty_message)}</p>'
    return _render_value(value)


def _render_value(value: Any) -> str:
    """Render one JSON-like value recursively."""
    if value is None:
        return '<span class="muted">null</span>'
    if isinstance(value, bool):
        return html.escape(_bool_label(value))
    if isinstance(value, (int, float)):
        return html.escape(str(value))
    if isinstance(value, str):
        return html.escape(value)
    if isinstance(value, dict):
        parts = ['<dl class="kv-list">']
        for key, item in value.items():
            parts.append(f"<dt>{html.escape(str(key))}</dt><dd>{_render_value(item)}</dd>")
        parts.append("</dl>")
        return "".join(parts)
    if isinstance(value, list):
        if not value:
            return '<p class="muted">None</p>'
        return "<ul>" + "".join(f"<li>{_render_value(item)}</li>" for item in value) + "</ul>"
    return html.escape(str(value))


def _structured_table(*, rows: list[Any], columns: list[str], empty_message: str) -> str:
    """Render a table for a list of structured dictionaries."""
    normalized: list[list[str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append([html.escape(str(row.get(column, ""))) for column in columns])
    return _html_table(headers=[column.replace("_", " ") for column in columns], rows=normalized, empty_message=empty_message)


def _coerce_str_list(value: Any) -> list[str]:
    """Normalize a list-like field into a string list for rendering."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _paragraph(value: Any) -> str:
    """Render a short paragraph or a muted placeholder."""
    if value is None or value == "":
        return '<p class="muted">None</p>'
    return f"<p>{html.escape(str(value))}</p>"
