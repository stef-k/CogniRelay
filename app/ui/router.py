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
from app.continuity import continuity_list_service, continuity_read_service
from app.continuity.listing import (
    _scan_active_summaries,
    _scan_archive_summaries,
    _scan_cold_summaries,
    _scan_fallback_summaries,
)
from app.discovery import capabilities_payload, health_payload
from app.git_manager import GitManager
from app.models import ContinuityListRequest, ContinuityReadRequest

from .render import render_template

UI_SUBJECT_KINDS: tuple[str, ...] = ("user", "peer", "thread", "task")
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
        subject_kind: Literal["user", "peer", "thread", "task"] | None = Query(default=None),
    ) -> HTMLResponse:
        """Render the continuity list view."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        _gm = _ui_git_manager(settings)
        auth = _ui_auth(client_ip)
        now = datetime.now(timezone.utc)
        listing = continuity_list_service(
            repo_root=settings.repo_root,
            auth=auth,
            req=ContinuityListRequest(
                subject_kind=subject_kind,
                limit=200,
                include_fallback=True,
                include_archived=False,
                include_cold=False,
            ),
            now=now,
            retention_archive_days=settings.continuity_retention_archive_days,
            audit=_noop_audit,
        )
        rows = []
        for capsule in listing["capsules"]:
            rows.append(
                [
                    html.escape(str(capsule.get("subject_kind", ""))),
                    _subject_link(str(capsule.get("subject_kind", "")), str(capsule.get("subject_id", ""))),
                    html.escape(str(capsule.get("artifact_state", ""))),
                    html.escape(str(capsule.get("health_status", ""))),
                    html.escape(str(capsule.get("updated_at", ""))),
                    html.escape(str(capsule.get("stable_preference_count", 0))),
                    html.escape(str(capsule.get("rationale_entry_count", 0))),
                ]
            )
        filter_options = "".join(
            _option_row(value=kind, selected=(kind == subject_kind))
            for kind in UI_SUBJECT_KINDS
        )
        body = render_template(
            "continuity_list.html",
            selected_kind=html.escape(subject_kind or "all"),
            filter_options=filter_options,
            result_count=str(listing["count"]),
            continuity_table=_html_table(
                headers=[
                    "Kind",
                    "Subject",
                    "Source",
                    "Health",
                    "Updated",
                    "Stable prefs",
                    "Rationale entries",
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
                    ("Updated at", str(capsule.get("updated_at") or "n/a")),
                    ("Verified at", str(capsule.get("verified_at") or "n/a")),
                    ("Verification kind", str(capsule.get("verification_kind") or "n/a")),
                ]
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
