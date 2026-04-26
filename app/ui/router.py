"""Server-rendered read-only operator UI routes."""

from __future__ import annotations

import asyncio
import html
import json
import re
import stat
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, urlencode

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from app.auth import AuthContext
from app.config import Settings, get_settings
from app.continuity import continuity_read_service
from app.continuity.listing import (
    _scan_active_summaries,
    _scan_archive_summaries,
    _scan_cold_summaries,
    _scan_fallback_summaries,
)
from app.continuity.paths import continuity_fallback_rel_path
from app.context import context_retrieve_service
from app.context.graph import derive_internal_graph_slice1
from app.discovery import capabilities_payload, health_payload
from app.git_manager import GitManager
from app.models import ContextRetrieveRequest, ContinuityReadRequest
from app.schedule import schedule_list_service
from app.ui.docs import UI_DOCS_BY_ID, UiDoc, doc_statuses, read_doc_source, render_doc_markdown

from .render import render_template

UI_SUBJECT_KINDS: tuple[str, ...] = ("user", "peer", "thread", "task")
UI_ARTIFACT_STATES: tuple[str, ...] = ("active", "fallback", "archived", "cold")
UI_HEALTH_STATUSES: tuple[str, ...] = ("healthy", "degraded", "conflicted")
UI_TASK_STATUSES: tuple[str, ...] = ("open", "in_progress", "blocked", "done")
UI_SCHEDULE_STATUSES: tuple[str, ...] = ("pending", "acknowledged", "done", "retired")
UI_SCHEDULE_DERIVED_STATES: tuple[str, ...] = ("scheduled", "due", "terminal")
UI_CONTINUITY_DISPLAY_LIMIT = 200
UI_TASK_DISPLAY_LIMIT = 200
UI_SCHEDULE_DISPLAY_LIMIT = 200
UI_SSE_RETRY_MS = 5000
_STATIC_DIR = Path(__file__).resolve().parent / "static"
_SCHEDULE_QUERY_SPLIT_RE = re.compile(r"[ \t\n\r\f\v]+")


@dataclass(frozen=True)
class _TaskArtifact:
    """One readable task artifact normalized for read-only UI rendering."""

    task_id: str
    status: str
    inferred_status: str
    root_rel: str
    path_rel: str
    root_rank: int
    data: dict[str, Any]
    warnings: tuple[str, ...] = ()


@dataclass
class _TaskSourceState:
    """Task artifact scan output with degraded-read warning codes."""

    artifacts: list[_TaskArtifact] = field(default_factory=list)
    root_warnings: list[str] = field(default_factory=list)
    artifact_warnings: list[str] = field(default_factory=list)
    duplicate_warnings: dict[str, str] = field(default_factory=dict)
    canonical_by_id: dict[str, _TaskArtifact] = field(default_factory=dict)


@dataclass
class _RelatedDocumentResult:
    """Related-document rows and warnings for one task."""

    rows: list[dict[str, str]]
    warnings: list[str]


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
            live_latest_commit=html.escape(str(health.get("latest_commit") or "none")),
            live_reported_at=html.escape(str(health.get("time", ""))),
            live_service_version=html.escape(str(health.get("version", ""))),
            live_git_initialized=html.escape(_bool_label(bool(health.get("git_initialized")))),
            live_active_count=html.escape(str(continuity_counts["active"])),
            live_fallback_count=html.escape(str(continuity_counts["fallback"])),
            live_archived_count=html.escape(str(continuity_counts["archived"])),
            live_cold_count=html.escape(str(continuity_counts["cold"])),
            live_user_count=html.escape(str(continuity_counts["by_subject_kind"].get("user", 0))),
            live_peer_count=html.escape(str(continuity_counts["by_subject_kind"].get("peer", 0))),
            live_thread_count=html.escape(str(continuity_counts["by_subject_kind"].get("thread", 0))),
            live_task_count=html.escape(str(continuity_counts["by_subject_kind"].get("task", 0))),
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
        subject_kind: str | None = Query(default=None),
        artifact_state: str | None = Query(default=None),
        health_status: str | None = Query(default=None),
    ) -> HTMLResponse:
        """Render the continuity list view."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        _gm = _ui_git_manager(settings)
        auth = _ui_auth(client_ip)
        now = datetime.now(timezone.utc)
        subject_kind = _normalize_ui_filter(subject_kind, UI_SUBJECT_KINDS)
        artifact_state = _normalize_ui_filter(artifact_state, UI_ARTIFACT_STATES)
        health_status = _normalize_ui_filter(health_status, UI_HEALTH_STATUSES)
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
            query_value=html.escape(_normalized_query_display(q)),
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
            live_displayed_count=html.escape(str(len(display_rows))),
            live_matched_count=html.escape(str(len(filtered_rows))),
            live_result_truncated=html.escape(_bool_label(len(filtered_rows) > UI_CONTINUITY_DISPLAY_LIMIT)),
            live_latest_recorded_at=html.escape(_continuity_live_latest_recorded_at(filtered_rows)),
            live_recent_change=html.escape(_continuity_live_recent_change_label(filtered_rows)),
            live_stream_path=html.escape(
                _ui_events_href(
                    q=q,
                    subject_kind=subject_kind,
                    artifact_state=artifact_state,
                    health_status=health_status,
                )
            ),
            continuity_table=_continuity_table_html(display_rows),
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
        related_rows = _related_artifact_rows(
            repo_root=settings.repo_root,
            auth=auth,
            subject_kind=subject_kind,
            subject_id=subject_id,
            now=datetime.now(timezone.utc),
            retention_archive_days=settings.continuity_retention_archive_days,
        )
        related_summary = _related_artifact_summary_rows(subject_kind=subject_kind, rows=related_rows)
        related_counts = _artifact_state_counts(related_rows)
        rendered_sections = _ui_detail_render_sections(
            detail=detail,
            capsule=capsule,
            continuity=continuity,
            subject_kind=subject_kind,
            startup_summary=_startup_summary_for_ui(detail.get("startup_summary")),
            trust_signals=detail.get("trust_signals"),
            related_summary=related_summary,
        )
        body = render_template(
            "continuity_detail.html",
            subject_kind=html.escape(subject_kind),
            subject_id=html.escape(subject_id),
            source_state=html.escape(str(detail.get("source_state", "unknown"))),
            live_stream_path=html.escape(
                _ui_events_href(
                    detail_subject_kind=subject_kind,
                    detail_subject_id=subject_id,
                )
            ),
            live_detail_updated_at=html.escape(str(capsule.get("updated_at") or "n/a")),
            live_detail_verified_at=html.escape(str(capsule.get("verified_at") or "n/a")),
            live_detail_warning_count=html.escape(str(len(list(detail.get("recovery_warnings") or [])))),
            live_detail_latest_recorded_at=html.escape(_continuity_live_latest_recorded_at(related_rows)),
            live_detail_source_state=html.escape(str(detail.get("source_state", "unknown"))),
            live_detail_active_count=html.escape(str(related_counts["active"])),
            live_detail_fallback_count=html.escape(str(related_counts["fallback"])),
            live_detail_archived_count=html.escape(str(related_counts["archived"])),
            live_detail_cold_count=html.escape(str(related_counts["cold"])),
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
            related_artifact_rows=rendered_sections["related_artifact_rows"],
            graph_link_section=_continuity_graph_link_section(subject_kind, subject_id),
            startup_summary_html=rendered_sections["startup_summary_html"],
            trust_signals_html=rendered_sections["trust_signals_html"],
            top_priorities_html=rendered_sections["top_priorities_html"],
            active_concerns_html=rendered_sections["active_concerns_html"],
            active_constraints_html=rendered_sections["active_constraints_html"],
            open_loops_html=rendered_sections["open_loops_html"],
            session_trajectory_html=rendered_sections["session_trajectory_html"],
            stance_summary_html=rendered_sections["stance_summary_html"],
            related_documents_html=rendered_sections["related_documents_html"],
            thread_descriptor_section=rendered_sections["thread_descriptor_section"],
            stable_preferences_html=rendered_sections["stable_preferences_html"],
            negative_decisions_html=rendered_sections["negative_decisions_html"],
            rationale_entries_html=rendered_sections["rationale_entries_html"],
            recovery_warnings_html=rendered_sections["recovery_warnings_html"],
        )
        return _page(
            title=f"Continuity Detail: {subject_kind}/{subject_id}",
            current_path="/ui/continuity",
            content=body,
        )

    @router.get("/tasks", response_class=HTMLResponse)
    def ui_tasks(
        request: Request,
        task_id: str | None = Query(default=None),
        status: str | None = Query(default=None),
        q: str | None = Query(default=None),
    ) -> HTMLResponse:
        """Render the read-only task list or query-addressed task detail."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        auth = _ui_auth(client_ip)
        task_id = _coerce_optional_query_value(task_id)
        status = _coerce_optional_query_value(status)
        q = _coerce_optional_query_value(q)
        if "task_id" in request.query_params:
            return _task_detail_page(settings=settings, auth=auth, task_id=task_id)
        return _task_list_page(settings=settings, auth=auth, status=status, q=q)

    @router.get("/tasks/{task_id}", response_class=HTMLResponse)
    def ui_task_detail(request: Request, task_id: str) -> HTMLResponse:
        """Render one read-only task detail page with graceful degradation."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        auth = _ui_auth(client_ip)
        return _task_detail_page(settings=settings, auth=auth, task_id=task_id)

    @router.get("/schedule", response_class=HTMLResponse)
    def ui_schedule(request: Request) -> HTMLResponse:
        """Render the read-only schedule/reminder inspection page."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        auth = _ui_auth(client_ip)
        return _schedule_list_page(settings=settings, auth=auth, request=request)

    @router.get("/context", response_class=HTMLResponse)
    def ui_context(request: Request) -> HTMLResponse:
        """Render the read-only context retrieval inspector."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        return _context_retrieval_page(settings=settings, client_ip=client_ip, request=request)

    @router.get("/graph", response_class=HTMLResponse)
    def ui_graph(
        request: Request,
        subject_kind: str | None = Query(default=None),
        subject_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        """Render the read-only derived graph selector or query-addressed graph."""
        settings = get_settings()
        _enforce_ui_access(request, settings)
        has_subject_kind = "subject_kind" in request.query_params
        has_subject_id = "subject_id" in request.query_params
        if not has_subject_kind and not has_subject_id:
            return _graph_page(
                current_path="/ui/graph",
                subject_kind=None,
                subject_id=None,
                graph=None,
                live_stream_path=None,
            )
        return _render_graph_response(
            settings=settings,
            subject_kind=subject_kind if has_subject_kind else None,
            subject_id=subject_id if has_subject_id else None,
        )

    @router.get("/graph/{subject_kind}/{subject_id}", response_class=HTMLResponse)
    def ui_graph_detail(
        request: Request,
        subject_kind: str,
        subject_id: str,
    ) -> HTMLResponse:
        """Render one read-only derived graph detail page."""
        settings = get_settings()
        _enforce_ui_access(request, settings)
        return _render_graph_response(settings=settings, subject_kind=subject_kind, subject_id=subject_id)

    @router.get("/docs", response_class=HTMLResponse)
    def ui_docs(request: Request) -> HTMLResponse:
        """Render the fixed read-only documentation index."""
        settings = get_settings()
        _enforce_ui_access(request, settings)
        return _docs_index_page(settings.docs_source_root)

    @router.get("/docs/{doc_id}", response_class=HTMLResponse)
    def ui_doc_detail(request: Request, doc_id: str) -> HTMLResponse:
        """Render one allowlisted Markdown document."""
        settings = get_settings()
        _enforce_ui_access(request, settings)
        doc = UI_DOCS_BY_ID.get(doc_id)
        if doc is None:
            return _docs_not_found_page()
        return _docs_detail_page(settings.docs_source_root, doc)

    @router.get("/events")
    async def ui_events(
        request: Request,
        q: str | None = Query(default=None),
        subject_kind: str | None = Query(default=None),
        artifact_state: str | None = Query(default=None),
        health_status: str | None = Query(default=None),
        detail_subject_kind: str | None = Query(default=None),
        detail_subject_id: str | None = Query(default=None),
        graph_subject_kind: str | None = Query(default=None),
        graph_subject_id: str | None = Query(default=None),
    ) -> StreamingResponse:
        """Stream bounded read-only UI snapshots for progressive enhancement."""
        settings = get_settings()
        client_ip = _enforce_ui_access(request, settings)
        auth = _ui_auth(client_ip)
        subject_kind = _normalize_ui_filter(subject_kind, UI_SUBJECT_KINDS)
        artifact_state = _normalize_ui_filter(artifact_state, UI_ARTIFACT_STATES)
        health_status = _normalize_ui_filter(health_status, UI_HEALTH_STATUSES)
        detail_subject_kind = _normalize_ui_filter(detail_subject_kind, UI_SUBJECT_KINDS)
        graph_subject_kind = _coerce_optional_query_value(graph_subject_kind)
        graph_subject_id = _coerce_optional_query_value(graph_subject_id)

        async def event_stream() -> Any:
            event_id = 0
            yield f"retry: {UI_SSE_RETRY_MS}\n\n"
            while True:
                snapshot = _ui_live_snapshot(
                    app_version=app_version,
                    settings=settings,
                    auth=auth,
                    now=datetime.now(timezone.utc),
                    q=q,
                    subject_kind=subject_kind,
                    artifact_state=artifact_state,
                    health_status=health_status,
                    detail_subject_kind=detail_subject_kind,
                    detail_subject_id=detail_subject_id,
                    graph_subject_kind=graph_subject_kind,
                    graph_subject_id=graph_subject_id,
                )
                event_id += 1
                yield _sse_event("ui-snapshot", snapshot, event_id)
                if await request.is_disconnected():
                    break
                await asyncio.sleep(settings.ui_sse_poll_interval_seconds)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
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


def _ui_context_auth(client_ip: str | None) -> AuthContext:
    """Build the read-only auth context required for UI retrieval inspection."""
    return AuthContext(
        token="ui-operator",
        peer_id="ui-operator",
        scopes={"read:files", "search"},
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
        tasks_nav_class=_nav_class(current_path.startswith("/ui/tasks")),
        schedule_nav_class=_nav_class(current_path.startswith("/ui/schedule")),
        retrieval_nav_class=_nav_class(current_path.startswith("/ui/context")),
        graph_nav_class=_nav_class(current_path.startswith("/ui/graph")),
        docs_nav_class=_nav_class(current_path.startswith("/ui/docs")),
        content=content,
    )
    return HTMLResponse(html_doc)


def _nav_class(active: bool) -> str:
    """Return the nav link class for the current page."""
    return "nav-link active" if active else "nav-link"


def _docs_index_page(repo_root: Path) -> HTMLResponse:
    """Render the read-only docs index with fixed allowlist ordering."""
    statuses = doc_statuses(repo_root)
    rows: list[list[str]] = []
    warnings: list[str] = []
    for status in statuses:
        doc = status.doc
        status_label = "Available" if status.available else "Unavailable"
        title = html.escape(doc.title)
        if status.available:
            title = f'<a href="/ui/docs/{html.escape(doc.doc_id, quote=True)}">{title}</a>'
        if status.warning:
            warnings.append(status.warning)
        rows.append(
            [
                title,
                html.escape(doc.description),
                html.escape(doc.path),
                html.escape(status_label),
            ]
        )
    body = render_template(
        "docs_index.html",
        docs_table=_html_table(
            headers=["Title", "Description", "Source path", "Status"],
            rows=rows,
            empty_message="No documentation entries configured.",
        ),
        warnings_panel=_docs_warnings_panel(warnings),
        runtime_help_panel=_runtime_help_panel(),
    )
    return _page(title="Documentation", current_path="/ui/docs", content=body)


def _docs_detail_page(repo_root: Path, doc: UiDoc) -> HTMLResponse:
    """Render one allowlisted document, degrading if the file is unavailable."""
    source, warning = read_doc_source(repo_root, doc)
    status = "Available" if source is not None else "Unavailable"
    if source is None:
        content_html = '<p class="muted">Document content is unavailable.</p>'
        toc_html = '<p class="muted">No section headings available.</p>'
    else:
        rendered = render_doc_markdown(source=source, doc=doc)
        content_html = rendered.content_html
        toc_html = rendered.toc_html
    body = render_template(
        "docs_detail.html",
        document_rows=_definition_rows(
            [
                ("Title", doc.title),
                ("Source path", doc.path),
                ("Status", status),
            ]
        ),
        document_warning=_docs_warning_line(warning),
        toc_html=toc_html,
        runtime_help_panel=_runtime_help_panel(),
        content_html=content_html,
    )
    return _page(title=doc.title, current_path="/ui/docs", content=body)


def _docs_not_found_page() -> HTMLResponse:
    """Render deterministic UI 404 for unknown docs IDs."""
    body = (
        '<section class="panel">'
        "<h2>Documentation Not Found</h2>"
        '<p class="warning">The requested documentation page is not available in the UI docs allowlist.</p>'
        '<p><a href="/ui/docs">Back to documentation index</a></p>'
        "</section>"
    )
    response = _page(title="Documentation Not Found", current_path="/ui/docs", content=body)
    response.status_code = 404
    return response


def _docs_warnings_panel(warnings: list[str]) -> str:
    """Render deterministic docs warning codes when present."""
    if not warnings:
        return ""
    return '<section class="panel"><h2>Warnings</h2>' + _html_list(warnings) + "</section>"


def _docs_warning_line(warning: str | None) -> str:
    """Render the detail warning line for degraded docs."""
    if warning is None:
        return ""
    return f'<p class="warning">{html.escape(warning)}</p>'


def _runtime_help_panel() -> str:
    """Render plain links to existing runtime help surfaces."""
    links = [
        '<a href="/v1/help">Runtime help index</a>',
        '<a href="/v1/help/onboarding">Runtime onboarding index</a>',
        '<a href="/v1/help/limits">Validation limits index</a>',
    ]
    return "<section class=\"panel\"><h2>Runtime Help</h2><ul>" + "".join(f"<li>{link}</li>" for link in links) + "</ul></section>"


def _normalize_ui_filter(value: str | None, allowed: tuple[str, ...]) -> str | None:
    """Normalize optional UI filters so empty or unsupported values degrade safely."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in allowed:
        return normalized
    return None


def _coerce_optional_query_value(value: Any) -> str | None:
    """Coerce direct endpoint-call defaults to the same shape as FastAPI query values."""
    if value is None or isinstance(value, str):
        return value
    return None


def _task_list_page(*, settings: Settings, auth: AuthContext, status: str | None, q: str | None) -> HTMLResponse:
    """Render the #249 read-only task list page."""
    source = _scan_task_sources(settings.repo_root)
    status_filter = _normalize_ui_filter(status, UI_TASK_STATUSES)
    query_tokens = _task_query_tokens(q)
    canonical_rows = list(source.canonical_by_id.values())
    filtered_rows = [
        artifact for artifact in canonical_rows
        if _task_matches_status(artifact, status_filter) and _task_matches_query(artifact, query_tokens)
    ]
    filtered_rows.sort(key=_task_sort_key)
    display_rows = filtered_rows[:UI_TASK_DISPLAY_LIMIT]
    related_by_task: dict[str, _RelatedDocumentResult] = {}
    related_warnings: list[str] = []
    for artifact in display_rows:
        related = _task_related_documents(settings=settings, auth=auth, artifact=artifact)
        related_by_task[artifact.task_id] = related
        related_warnings.extend(related.warnings)
    matched_artifact_warnings = [warning for artifact in filtered_rows for warning in artifact.warnings]
    warnings = _dedupe_preserve_order(
        [
            *source.root_warnings,
            *source.artifact_warnings,
            *matched_artifact_warnings,
            *source.duplicate_warnings.values(),
            *related_warnings,
        ]
    )
    status_options = "".join(_option_row(value=value, selected=(value == status_filter)) for value in UI_TASK_STATUSES)
    body = render_template(
        "task_list.html",
        query_value=html.escape(_normalized_query_display(q)),
        status_options=status_options,
        selected_status=html.escape(status_filter or "all"),
        normalized_query=html.escape(" ".join(query_tokens) if query_tokens else "none"),
        displayed_count=str(len(display_rows)),
        matched_count=str(len(filtered_rows)),
        warning_count=str(len(warnings)),
        result_truncated=_bool_label(len(filtered_rows) > UI_TASK_DISPLAY_LIMIT),
        warnings_html=_task_warnings_panel(warnings),
        task_table=_task_table_html(display_rows, related_by_task),
    )
    return _page(title="Tasks", current_path="/ui/tasks", content=body)


def _task_detail_page(*, settings: Settings, auth: AuthContext, task_id: str | None) -> HTMLResponse:
    """Render the #249 read-only task detail page."""
    source = _scan_task_sources(settings.repo_root)
    requested = task_id if isinstance(task_id, str) else None
    decoded = requested if requested is not None else ""
    normalized = decoded.strip()
    not_found_warning = "task_not_found" if not normalized else f"task_not_found:{decoded}"
    if not normalized:
        body = _task_detail_body(
            task_id=decoded,
            artifact=None,
            warnings=[not_found_warning, *source.root_warnings],
            related=_RelatedDocumentResult(rows=[], warnings=[]),
        )
        return _page(title="Task Detail", current_path="/ui/tasks", content=body)
    artifact = source.canonical_by_id.get(decoded)
    if artifact is None:
        body = _task_detail_body(
            task_id=decoded,
            artifact=None,
            warnings=[not_found_warning, *source.root_warnings],
            related=_RelatedDocumentResult(rows=[], warnings=[]),
        )
        return _page(title=f"Task Detail: {decoded}", current_path="/ui/tasks", content=body)
    related = _task_related_documents(settings=settings, auth=auth, artifact=artifact)
    warnings = [*artifact.warnings]
    duplicate_warning = source.duplicate_warnings.get(artifact.task_id)
    if duplicate_warning:
        warnings.append(duplicate_warning)
    warnings.extend(related.warnings)
    body = _task_detail_body(task_id=decoded, artifact=artifact, warnings=_dedupe_preserve_order(warnings), related=related)
    return _page(title=f"Task Detail: {artifact.task_id}", current_path="/ui/tasks", content=body)


def _schedule_list_page(*, settings: Settings, auth: AuthContext, request: Request) -> HTMLResponse:
    """Render the #260 read-only schedule list using the shipped schedule service."""
    filters, ui_warnings = _schedule_ui_filters(request)
    service_query: dict[str, Any] = {
        "limit": UI_SCHEDULE_DISPLAY_LIMIT,
        "offset": 0,
        "include_retired": filters["include_retired"],
    }
    if filters["status"] is not None:
        service_query["status"] = filters["status"]
    try:
        result = schedule_list_service(repo_root=settings.repo_root, auth=auth, query=service_query)
    except Exception as exc:
        result = {
            "ok": False,
            "count": 0,
            "total": 0,
            "limit": UI_SCHEDULE_DISPLAY_LIMIT,
            "offset": 0,
            "items": [],
            "warnings": [f"schedule_ui_service_exception:{exc.__class__.__name__}"],
        }
    items = [item for item in list(result.get("items") or []) if isinstance(item, dict)]
    filtered_items = _schedule_apply_ui_filters(items, derived_state=filters["derived_state"], q=filters["q"])
    service_count = _schedule_int(result.get("count"))
    service_total = _schedule_int(result.get("total"))
    service_limit = _schedule_int(result.get("limit"), default=UI_SCHEDULE_DISPLAY_LIMIT)
    warnings = _dedupe_preserve_order([*_coerce_str_list(result.get("warnings")), *ui_warnings])
    truncated = service_total > service_count
    status_options = "".join(_option_row(value=value, selected=(value == filters["status"])) for value in UI_SCHEDULE_STATUSES)
    derived_state_options = "".join(_option_row(value=value, selected=(value == filters["derived_state"])) for value in UI_SCHEDULE_DERIVED_STATES)
    body = render_template(
        "schedule_list.html",
        status_options=status_options,
        derived_state_options=derived_state_options,
        selected_status=html.escape(filters["status"] or "all"),
        selected_derived_state=html.escape(filters["derived_state"] or "all"),
        include_retired_false_selected=' selected="selected"' if not filters["include_retired"] else "",
        include_retired_true_selected=' selected="selected"' if filters["include_retired"] else "",
        query_value=html.escape(filters["q"]),
        count=str(len(filtered_items)),
        service_count=str(service_count),
        service_total=str(service_total),
        limit=str(service_limit),
        truncated=_bool_label(truncated),
        truncation_notice=_schedule_truncation_notice(truncated, service_limit),
        warnings_html=_schedule_warnings_panel(warnings),
        schedule_table=_schedule_table_html(filtered_items),
    )
    return _page(title="Schedule", current_path="/ui/schedule", content=body)


def _schedule_ui_filters(request: Request) -> tuple[dict[str, Any], list[str]]:
    """Normalize #260 UI-only filters and return validation warning codes."""
    params = request.query_params
    warnings: list[str] = []
    status = _schedule_normalize_all_filter(params.get("status"), UI_SCHEDULE_STATUSES)
    if status == "__invalid__":
        warnings.append("invalid_schedule_ui_filter:status")
        status = None
    derived_state = _schedule_normalize_all_filter(params.get("derived_state"), UI_SCHEDULE_DERIVED_STATES)
    if derived_state == "__invalid__":
        warnings.append("invalid_schedule_ui_filter:derived_state")
        derived_state = None
    include_retired = False
    raw_include_retired = params.get("include_retired")
    if raw_include_retired in (None, "", "false"):
        include_retired = False
    elif raw_include_retired == "true":
        include_retired = True
    else:
        warnings.append("invalid_schedule_ui_filter:include_retired")
    q_values = params.getlist("q")
    raw_q = q_values[0] if q_values else ""
    return {
        "status": status,
        "derived_state": derived_state,
        "include_retired": include_retired,
        "q": raw_q,
    }, warnings


def _schedule_normalize_all_filter(value: str | None, allowed: tuple[str, ...]) -> str | None:
    """Normalize missing, empty, and all to no filter; flag unsupported values."""
    if value is None:
        return None
    normalized = str(value)
    if normalized == "" or normalized == "all":
        return None
    if normalized in allowed:
        return normalized
    return "__invalid__"


def _schedule_apply_ui_filters(items: list[dict[str, Any]], *, derived_state: str | None, q: str) -> list[dict[str, Any]]:
    """Apply derived_state and q filters without changing service order."""
    out = items
    if derived_state is not None:
        out = [item for item in out if item.get("derived_state") == derived_state]
    tokens = _schedule_query_tokens(q)
    if tokens:
        out = [item for item in out if _schedule_matches_query(item, tokens)]
    return out


def _schedule_query_tokens(value: str) -> list[str]:
    """Return casefolded tokens using the #260 ASCII whitespace algorithm."""
    return [token.casefold() for token in _SCHEDULE_QUERY_SPLIT_RE.split(value) if token]


def _schedule_matches_query(item: dict[str, Any], tokens: list[str]) -> bool:
    """Return whether all query tokens match the permitted rendered fields."""
    fields = [
        _schedule_search_value(item.get(field))
        for field in (
            "schedule_id",
            "kind",
            "status",
            "derived_state",
            "title",
            "due_at",
            "task_id",
            "thread_id",
            "subject_kind",
            "subject_id",
            "updated_at",
        )
    ]
    return all(any(token in field for field in fields) for token in tokens)


def _schedule_search_value(value: Any) -> str:
    """Return a casefolded searchable string for scalar schedule fields."""
    if isinstance(value, str):
        return str(value).casefold()
    return ""


def _schedule_table_html(items: list[dict[str, Any]]) -> str:
    """Render the schedule table with exactly the #260 columns."""
    return _html_table(
        headers=[
            "schedule_id",
            "kind",
            "status",
            "derived_state",
            "title",
            "due_at",
            "task_id",
            "thread_id",
            "subject_kind",
            "subject_id",
            "updated_at",
        ],
        rows=[_schedule_row_cells(item) for item in items],
        empty_message="No schedule items matched the current filters.",
    )


def _schedule_row_cells(item: dict[str, Any]) -> list[str]:
    """Render one schedule item row, degrading malformed cells in place."""
    return [
        _schedule_scalar_cell(item.get("schedule_id")),
        _schedule_scalar_cell(item.get("kind")),
        _schedule_scalar_cell(item.get("status")),
        _schedule_scalar_cell(item.get("derived_state")),
        _schedule_scalar_cell(item.get("title")),
        _schedule_scalar_cell(item.get("due_at")),
        _schedule_task_cell(item.get("task_id")),
        _schedule_thread_cell(item.get("thread_id")),
        _schedule_scalar_cell(item.get("subject_kind")),
        _schedule_subject_id_cell(item),
        _schedule_scalar_cell(item.get("updated_at")),
    ]


def _schedule_scalar_cell(value: Any) -> str:
    """Render schedule scalar values or the required muted n/a."""
    if value is None or value == "":
        return '<span class="muted">n/a</span>'
    if isinstance(value, str | int | float | bool):
        return html.escape(str(value))
    return '<span class="muted">n/a</span>'


def _schedule_task_cell(value: Any) -> str:
    """Render task_id with task detail first and retrieval appended."""
    if not isinstance(value, str) or value == "":
        return '<span class="muted">n/a</span>'
    task_link = _task_detail_link(value)
    retrieval = _schedule_context_task_link(value)
    return _schedule_inline_parts([task_link, retrieval])


def _schedule_thread_cell(value: Any) -> str:
    """Render thread_id value plus supported continuity and graph links."""
    if not isinstance(value, str) or value == "":
        return '<span class="muted">n/a</span>'
    parts = [html.escape(value)]
    if "/" not in value:
        parts.append(_continuity_subject_link("thread", value, "Continuity"))
    parts.append(_graph_query_link("thread", value, "Graph"))
    return _schedule_inline_parts(parts)


def _schedule_subject_id_cell(item: dict[str, Any]) -> str:
    """Render subject_id value plus supported continuity, graph, and retrieval links."""
    subject_id = item.get("subject_id")
    if not isinstance(subject_id, str) or subject_id == "":
        return '<span class="muted">n/a</span>'
    subject_kind = item.get("subject_kind")
    parts = [html.escape(subject_id)]
    if isinstance(subject_kind, str) and subject_kind in UI_SUBJECT_KINDS:
        if "/" not in subject_id:
            parts.append(_continuity_subject_link(subject_kind, subject_id, "Continuity"))
        if subject_kind in {"thread", "task"}:
            parts.append(_graph_query_link(subject_kind, subject_id, "Graph"))
        if not isinstance(item.get("task_id"), str) or item.get("task_id") == "":
            parts.append(_schedule_context_subject_link(subject_kind, subject_id))
    return _schedule_inline_parts(parts)


def _schedule_context_task_link(task_id: str) -> str:
    """Render the context retrieval link for a task-backed schedule row."""
    href = f"/ui/context?{urlencode({'task': task_id})}"
    return f'<a href="{html.escape(href)}">Retrieval</a>'


def _schedule_context_subject_link(subject_kind: str, subject_id: str) -> str:
    """Render the context retrieval link for a subject-backed schedule row."""
    href = f"/ui/context?{urlencode({'subject_kind': subject_kind, 'subject_id': subject_id})}"
    return f'<a href="{html.escape(href)}">Retrieval</a>'


def _schedule_inline_parts(parts: list[str]) -> str:
    """Join already-escaped inline cell fragments with the required separator."""
    return " · ".join(parts)


def _schedule_warnings_panel(warnings: list[str]) -> str:
    """Render visible schedule warning codes using the existing panel pattern."""
    if not warnings:
        return ""
    return f'<section class="panel"><h2>Warnings</h2>{_html_list(warnings)}</section>'


def _schedule_truncation_notice(truncated: bool, limit: int) -> str:
    """Render the fixed-limit truncation notice when the service result is capped."""
    if not truncated:
        return ""
    return f'<p class="warning">Only the first {html.escape(str(limit))} service-ordered rows are available in this UI slice.</p>'


def _schedule_int(value: Any, *, default: int = 0) -> int:
    """Coerce service count fields for summary rendering."""
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _scan_task_sources(repo_root: Path) -> _TaskSourceState:
    """Read task artifacts from the exact #249 roots with deterministic degradation."""
    state = _TaskSourceState()
    for root_rank, (root_rel, inferred_status) in enumerate((("tasks/open", "open"), ("tasks/done", "done"))):
        root = repo_root / root_rel
        if not root.exists():
            state.root_warnings.append(f"task_root_missing:{root_rel}")
            continue
        if not root.is_dir():
            state.root_warnings.append(f"task_root_invalid:{root_rel}")
            continue
        for path in sorted(root.iterdir(), key=lambda item: item.name):
            if path.is_symlink() or not path.is_file() or path.suffix != ".json":
                continue
            path_rel = _repo_relative_path(repo_root, path)
            if not _path_has_any_read_bit(path):
                state.artifact_warnings.append(f"task_artifact_skipped:{path_rel}")
                continue
            try:
                decoded = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                state.artifact_warnings.append(f"task_artifact_skipped:{path_rel}")
                continue
            if not isinstance(decoded, dict):
                state.artifact_warnings.append(f"task_artifact_skipped:{path_rel}")
                continue
            task_id = _non_empty_str(decoded.get("task_id"))
            warnings: list[str] = []
            if task_id is None:
                task_id = path.stem
                warnings.append(f"task_id_inferred:{path_rel}")
            status_value = _non_empty_str(decoded.get("status")) or inferred_status
            state.artifacts.append(
                _TaskArtifact(
                    task_id=task_id,
                    status=status_value,
                    inferred_status=inferred_status,
                    root_rel=root_rel,
                    path_rel=path_rel,
                    root_rank=root_rank,
                    data=decoded,
                    warnings=tuple(warnings),
                )
            )
    by_id: dict[str, list[_TaskArtifact]] = {}
    for artifact in state.artifacts:
        by_id.setdefault(artifact.task_id, []).append(artifact)
    for task_id, artifacts in by_id.items():
        ordered = sorted(artifacts, key=lambda item: (item.root_rank, item.path_rel))
        state.canonical_by_id[task_id] = ordered[0]
        if len(ordered) > 1:
            state.duplicate_warnings[task_id] = f"duplicate_task_artifacts:{task_id}"
    return state


def _repo_relative_path(repo_root: Path, path: Path) -> str:
    """Return a stable POSIX repository-relative path."""
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _path_has_any_read_bit(path: Path) -> bool:
    """Return whether the artifact mode grants read access to anyone."""
    try:
        mode = path.stat().st_mode
    except OSError:
        return False
    return bool(mode & (stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH))


def _non_empty_str(value: Any) -> str | None:
    """Return a stripped non-empty string, ignoring all non-string values."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _task_query_tokens(value: str | None) -> list[str]:
    """Normalize the #249 whitespace query tokens."""
    if not isinstance(value, str):
        return []
    return [token for token in value.strip().lower().split() if token]


def _task_matches_status(artifact: _TaskArtifact, status_filter: str | None) -> bool:
    """Return whether one task matches the normalized status filter."""
    return status_filter is None or artifact.status == status_filter


def _task_matches_query(artifact: _TaskArtifact, tokens: list[str]) -> bool:
    """Return whether every query token matches a permitted string field."""
    if not tokens:
        return True
    fields = [
        artifact.task_id,
        artifact.status,
        artifact.path_rel,
        *[
            value
            for value in (
                artifact.data.get("title"),
                artifact.data.get("description"),
                artifact.data.get("owner_peer"),
                artifact.data.get("thread_id"),
            )
            if isinstance(value, str) and value.strip()
        ],
        *_string_list(artifact.data.get("collaborators")),
    ]
    searchable = [value.lower() for value in fields if value]
    return all(any(token in field for field in searchable) for token in tokens)


def _task_sort_key(artifact: _TaskArtifact) -> tuple[int, str, str, str]:
    """Sort by updated_at descending, then task_id and artifact path ascending."""
    updated_at = _display_str(artifact.data, "updated_at")
    missing = 1 if updated_at == "" else 0
    return (missing, _descending_text_key(updated_at), artifact.task_id, artifact.path_rel)


def _descending_text_key(value: str) -> str:
    """Invert code points for deterministic descending string sort."""
    return "".join(chr(0x10FFFF - ord(char)) for char in value)


def _display_str(data: dict[str, Any], key: str) -> str:
    """Return a display/search scalar only when it is a non-empty string."""
    return _non_empty_str(data.get(key)) or ""


def _string_list(value: Any) -> list[str]:
    """Return only non-empty string entries from a list field."""
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _task_related_documents(*, settings: Settings, auth: AuthContext, artifact: _TaskArtifact) -> _RelatedDocumentResult:
    """Resolve related documents from task artifact, task metadata, and task continuity."""
    rows: list[dict[str, str]] = []
    warnings: list[str] = []
    seen: set[tuple[str, str]] = set()
    _extend_related_document_rows(
        rows=rows,
        warnings=warnings,
        seen=seen,
        source="task_artifact",
        value=artifact.data.get("related_documents"),
    )
    metadata = artifact.data.get("metadata") if isinstance(artifact.data.get("metadata"), dict) else {}
    _extend_related_document_rows(
        rows=rows,
        warnings=warnings,
        seen=seen,
        source="task_metadata",
        value=metadata.get("related_documents"),
    )
    try:
        detail = continuity_read_service(
            repo_root=settings.repo_root,
            auth=auth,
            req=ContinuityReadRequest(subject_kind="task", subject_id=artifact.task_id, allow_fallback=True, view="startup"),
            now=datetime.now(timezone.utc),
            audit=_noop_audit,
        )
    except Exception:
        warnings.append(f"task_continuity_unavailable:{artifact.task_id}")
        return _RelatedDocumentResult(rows=rows, warnings=_dedupe_preserve_order(warnings))
    capsule = detail.get("capsule") if isinstance(detail, dict) else None
    recovery_warnings = _task_continuity_recovery_warnings(detail=detail, capsule=capsule)
    if isinstance(capsule, dict):
        continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
        continuity_related_documents = _task_continuity_related_documents_value(
            settings=settings,
            detail=detail,
            task_id=artifact.task_id,
            sanitized_value=continuity.get("related_documents"),
        )
        _extend_related_document_rows(
            rows=rows,
            warnings=warnings,
            seen=seen,
            source="task_continuity",
            value=continuity_related_documents,
        )
    warnings.extend(f"task_continuity:{warning}" for warning in recovery_warnings)
    return _RelatedDocumentResult(rows=rows, warnings=_dedupe_preserve_order(warnings))


def _task_continuity_recovery_warnings(*, detail: Any, capsule: Any) -> list[str]:
    """Return task-continuity recovery warnings, excluding pure missing continuity."""
    if not isinstance(detail, dict):
        return []
    recovery_warnings = [str(warning) for warning in list(detail.get("recovery_warnings") or [])]
    if isinstance(capsule, dict):
        return recovery_warnings
    source_state = str(detail.get("source_state") or "")
    if source_state == "missing" and set(recovery_warnings).issubset({"continuity_active_missing", "continuity_fallback_missing"}):
        return []
    return recovery_warnings


def _task_continuity_related_documents_value(
    *,
    settings: Settings,
    detail: dict[str, Any],
    task_id: str,
    sanitized_value: Any,
) -> Any:
    """Return task-continuity related_documents, preserving raw pathless skips when available."""
    if isinstance(sanitized_value, list):
        return sanitized_value
    source_state = str(detail.get("source_state") or "")
    if source_state == "active":
        rel = str(detail.get("path") or "")
        raw = _read_json_object(settings.repo_root / rel)
        continuity = raw.get("continuity") if isinstance(raw.get("continuity"), dict) else {}
        return continuity.get("related_documents")
    if source_state == "fallback":
        rel = continuity_fallback_rel_path("task", task_id)
        raw = _read_json_object(settings.repo_root / rel)
        capsule = raw.get("capsule") if isinstance(raw.get("capsule"), dict) else {}
        continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
        return continuity.get("related_documents")
    return sanitized_value


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read a local JSON object for UI-only inspection; degrade to empty."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _extend_related_document_rows(
    *,
    rows: list[dict[str, str]],
    warnings: list[str],
    seen: set[tuple[str, str]],
    source: str,
    value: Any,
) -> None:
    """Append valid related document entries for one source, coalescing path skips."""
    if not isinstance(value, list):
        return
    skipped = False
    for entry in value:
        if not isinstance(entry, dict):
            continue
        path = _non_empty_str(entry.get("path"))
        if path is None:
            skipped = True
            continue
        key = (path, source)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "path": path,
                "kind": _non_empty_str(entry.get("kind")) or "",
                "label": _non_empty_str(entry.get("label")) or "",
                "relevance": _non_empty_str(entry.get("relevance")) or "",
                "source": source,
            }
        )
    if skipped:
        warnings.append(f"related_document_skipped:{source}")


def _task_table_html(rows: list[_TaskArtifact], related_by_task: dict[str, _RelatedDocumentResult]) -> str:
    """Render the task list table with the exact #249 columns."""
    table_rows: list[list[str]] = []
    for artifact in rows:
        collaborators = _string_list(artifact.data.get("collaborators"))
        table_rows.append(
            [
                html.escape(artifact.status),
                _task_detail_link(artifact.task_id),
                _task_title_cell(artifact),
                _muted_or_text(_display_str(artifact.data, "owner_peer"), "n/a"),
                html.escape(", ".join(collaborators)) if collaborators else "None",
                _thread_continuity_cell(_display_str(artifact.data, "thread_id")),
                html.escape(str(len(_string_list(artifact.data.get("blocked_by"))))),
                html.escape(str(len(related_by_task.get(artifact.task_id, _RelatedDocumentResult([], [])).rows))),
                html.escape(_display_str(artifact.data, "updated_at") or "n/a"),
                html.escape(artifact.path_rel),
            ]
        )
    return _html_table(
        headers=["Status", "Task", "Title", "Owner", "Collaborators", "Thread", "Blocked By", "Related Documents", "Updated", "Artifact"],
        rows=table_rows,
        empty_message="No tasks matched the current filter.",
    )


def _task_detail_body(
    *,
    task_id: str,
    artifact: _TaskArtifact | None,
    warnings: list[str],
    related: _RelatedDocumentResult,
) -> str:
    """Render task detail sections in the exact #249 order."""
    if artifact is None:
        resolved_id = task_id.strip()
        task_section = _definition_rows(
            [
                ("Task ID", resolved_id or "n/a"),
                ("Title", "Untitled task"),
                ("Description", "No description recorded."),
                ("Status", "n/a"),
                ("Owner", "n/a"),
                ("Collaborators", "None"),
                ("Due at", "n/a"),
                ("Created at", "n/a"),
                ("Updated at", "n/a"),
            ]
        )
        relationships = '<p class="muted">n/a</p>'
        artifact_html = _definition_rows([("Path", "n/a"), ("Root", "n/a"), ("Status inferred from root", "n/a")])
    else:
        data = artifact.data
        task_section = _definition_rows(
            [
                ("Task ID", artifact.task_id),
                ("Title", _display_str(data, "title") or "Untitled task"),
                ("Description", _display_str(data, "description") or "No description recorded."),
                ("Status", artifact.status),
                ("Owner", _display_str(data, "owner_peer") or "n/a"),
                ("Collaborators", ", ".join(_string_list(data.get("collaborators"))) or "None"),
                ("Due at", _display_str(data, "due_at") or "n/a"),
                ("Created at", _display_str(data, "created_at") or "n/a"),
                ("Updated at", _display_str(data, "updated_at") or "n/a"),
            ]
        )
        relationships = _task_relationships_html(artifact)
        artifact_html = _definition_rows(
            [
                ("Path", artifact.path_rel),
                ("Root", artifact.root_rel),
                ("Status inferred from root", artifact.inferred_status),
            ]
        )
    return (
        '<section class="panel"><h2>Task</h2>'
        f"{task_section}</section>"
        '<section class="panel"><h2>Warnings</h2>'
        f"{_html_list(warnings)}</section>"
        '<section class="panel"><h2>Relationships</h2>'
        f"{relationships}</section>"
        '<section class="panel"><h2>Related Documents</h2>'
        f"{_task_related_documents_table(related.rows)}</section>"
        '<section class="panel"><h2>Metadata</h2>'
        '<p class="muted">No metadata recorded.</p></section>'
        '<section class="panel"><h2>Artifact</h2>'
        f"{artifact_html}</section>"
    )


def _task_relationships_html(artifact: _TaskArtifact) -> str:
    """Render task relationship rows and blockers with safe links."""
    thread_id = _display_str(artifact.data, "thread_id")
    thread_cell = "n/a"
    if thread_id:
        thread_graph = _graph_query_link("thread", thread_id, "Graph")
        if "/" in thread_id:
            thread_cell = f"Continuity link unavailable for slash-containing ID. {thread_graph}"
        else:
            thread_cell = f'{_continuity_subject_link("thread", thread_id, "Continuity")} {thread_graph}'
    continuity_cell = (
        "Continuity link unavailable for slash-containing ID."
        if "/" in artifact.task_id
        else _continuity_subject_link("task", artifact.task_id, "Continuity")
    )
    task_graph_cell = _graph_query_link("task", artifact.task_id, "Graph")
    blockers = _string_list(artifact.data.get("blocked_by"))
    blocker_rows = [
        [html.escape(blocker), _task_detail_link(blocker), _graph_query_link("task", blocker, "Graph")]
        for blocker in blockers
    ]
    return (
        f"{_definition_rows_html([('Thread', thread_cell), ('Task continuity', continuity_cell), ('Task graph', task_graph_cell)])}"
        "<h3>Blocked by</h3>"
        f"{_html_table(headers=['Task ID', 'Task', 'Graph'], rows=blocker_rows, empty_message='No blocking tasks recorded.')}"
    )


def _task_related_documents_table(rows: list[dict[str, str]]) -> str:
    """Render task related documents with the exact #249 columns."""
    return _html_table(
        headers=["Path", "Kind", "Label", "Relevance", "Source"],
        rows=[
            [
                html.escape(row["path"]),
                html.escape(row["kind"]),
                html.escape(row["label"]),
                html.escape(row["relevance"]),
                html.escape(row["source"]),
            ]
            for row in rows
        ],
        empty_message="No related documents recorded.",
    )


def _task_warnings_panel(warnings: list[str]) -> str:
    """Render the warning panel only when warning codes exist."""
    if not warnings:
        return ""
    return f'<section class="panel"><h2>Warnings</h2>{_html_list(warnings)}</section>'


def _task_title_cell(artifact: _TaskArtifact) -> str:
    """Render title or the required muted empty state."""
    title = _display_str(artifact.data, "title")
    if title:
        return html.escape(title)
    return '<span class="muted">Untitled task</span>'


def _muted_or_text(value: str, empty: str) -> str:
    """Render a display value or muted empty-state text."""
    if value:
        return html.escape(value)
    return f'<span class="muted">{html.escape(empty)}</span>'


def _thread_continuity_cell(thread_id: str) -> str:
    """Render the task-list thread cell."""
    if not thread_id:
        return '<span class="muted">n/a</span>'
    if "/" in thread_id:
        return "Continuity link unavailable for slash-containing ID."
    return _continuity_subject_link("thread", thread_id, thread_id)


def _task_detail_link(task_id: str) -> str:
    """Render the canonical task detail link for safe and slash-containing IDs."""
    href = _task_detail_href(task_id)
    return f'<a href="{html.escape(href)}">{html.escape(task_id)}</a>'


def _task_detail_href(task_id: str) -> str:
    """Return the canonical task detail route for one task ID."""
    if "/" in task_id:
        return f"/ui/tasks?{urlencode({'task_id': task_id})}"
    return f"/ui/tasks/{quote(task_id, safe='')}"


def _continuity_subject_link(subject_kind: str, subject_id: str, label: str) -> str:
    """Render a continuity path link for one safe path-segment ID."""
    href = f"/ui/continuity/{quote(subject_kind, safe='')}/{quote(subject_id, safe='')}"
    return f'<a href="{html.escape(href)}">{html.escape(label)}</a>'


def _graph_query_link(subject_kind: str, subject_id: str, label: str) -> str:
    """Render a graph query link backed by existing graph UI behavior."""
    href = f"/ui/graph?{urlencode({'subject_kind': subject_kind, 'subject_id': subject_id})}"
    return f'<a href="{html.escape(href)}">{html.escape(label)}</a>'


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    """Deduplicate warning codes without changing first-observed order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _context_retrieval_page(*, settings: Settings, client_ip: str | None, request: Request) -> HTMLResponse:
    """Render the #251 read-only context retrieval inspector."""
    params = request.query_params
    has_any_param = any(name in params for name in ("task", "subject_kind", "subject_id"))
    raw_task = params.get("task") if "task" in params else None
    raw_subject_kind = params.get("subject_kind") if "subject_kind" in params else None
    raw_subject_id = params.get("subject_id") if "subject_id" in params else None
    trimmed_task = (raw_task or "").strip()
    trimmed_subject_kind = (raw_subject_kind or "").strip()
    trimmed_subject_id = (raw_subject_id or "").strip()

    validation_warnings = _context_validation_warnings(
        has_any_param=has_any_param,
        trimmed_task=trimmed_task,
        trimmed_subject_kind=trimmed_subject_kind,
        trimmed_subject_id=trimmed_subject_id,
    )
    if not has_any_param:
        state = _context_ui_state(
            raw_task=raw_task,
            raw_subject_kind=raw_subject_kind,
            raw_subject_id=raw_subject_id,
            request_run=False,
            request_params=None,
            bundle=None,
            ui_warnings=[],
            empty_message="No retrieval request has been run.",
        )
        return _context_response(state)
    if validation_warnings:
        state = _context_ui_state(
            raw_task=raw_task,
            raw_subject_kind=raw_subject_kind,
            raw_subject_id=raw_subject_id,
            request_run=False,
            request_params=None,
            bundle=None,
            ui_warnings=validation_warnings,
            empty_message=None,
        )
        return _context_response(state)

    req = ContextRetrieveRequest(
        task=raw_task or "",
        subject_kind=trimmed_subject_kind or None,
        subject_id=raw_subject_id if trimmed_subject_kind else None,
        continuity_mode="auto",
        continuity_verification_policy="allow_degraded",
        continuity_resilience_policy="allow_fallback",
        continuity_selectors=[],
        continuity_max_capsules=1,
        max_tokens_estimate=12000,
        include_types=[],
        time_window_days=30,
        limit=10,
    )
    request_params = _context_request_param_rows(req)
    try:
        result = context_retrieve_service(
            repo_root=settings.repo_root,
            auth=_ui_context_auth(client_ip),
            req=req,
            now=datetime.now(timezone.utc),
            audit=_noop_audit,
        )
        bundle = result.get("bundle") if isinstance(result, dict) and isinstance(result.get("bundle"), dict) else None
        ui_warnings: list[str] = []
    except Exception:
        bundle = None
        ui_warnings = ["ui_context_retrieve_failed"]
    state = _context_ui_state(
        raw_task=raw_task,
        raw_subject_kind=raw_subject_kind,
        raw_subject_id=raw_subject_id,
        request_run=True,
        request_params=request_params,
        bundle=bundle,
        ui_warnings=ui_warnings,
        empty_message=None,
    )
    return _context_response(state)


def _context_validation_warnings(
    *,
    has_any_param: bool,
    trimmed_task: str,
    trimmed_subject_kind: str,
    trimmed_subject_id: str,
) -> list[str]:
    """Return #251 validation warnings in the required deterministic order."""
    if not has_any_param:
        return []
    warnings: list[str] = []
    valid_subject_kind = trimmed_subject_kind in UI_SUBJECT_KINDS
    if trimmed_subject_kind and not valid_subject_kind:
        warnings.append("ui_context_invalid_subject_kind")
    if valid_subject_kind and not trimmed_subject_id:
        warnings.append("ui_context_subject_id_required")
    if trimmed_subject_id and not trimmed_subject_kind:
        warnings.append("ui_context_subject_kind_required")
    if not trimmed_task and not warnings:
        warnings.append("ui_context_task_required")
    return warnings


def _context_request_param_rows(req: ContextRetrieveRequest) -> list[tuple[str, str]]:
    """Return effective retrieval request params in the #251 display order."""
    return [
        ("task", req.task),
        ("subject_kind", req.subject_kind or "n/a"),
        ("subject_id", req.subject_id or "n/a"),
        ("continuity_mode", req.continuity_mode),
        ("continuity_verification_policy", req.continuity_verification_policy),
        ("continuity_resilience_policy", req.continuity_resilience_policy),
        ("continuity_selectors", _context_json(req.continuity_selectors)),
        ("continuity_max_capsules", str(req.continuity_max_capsules)),
        ("max_tokens_estimate", str(req.max_tokens_estimate)),
        ("include_types", _context_json(req.include_types)),
        ("time_window_days", str(req.time_window_days)),
        ("limit", str(req.limit)),
    ]


def _context_ui_state(
    *,
    raw_task: str | None,
    raw_subject_kind: str | None,
    raw_subject_id: str | None,
    request_run: bool,
    request_params: list[tuple[str, str]] | None,
    bundle: dict[str, Any] | None,
    ui_warnings: list[str],
    empty_message: str | None,
) -> dict[str, Any]:
    """Collect already-normalized context UI render state."""
    continuity_state = bundle.get("continuity_state") if isinstance(bundle, dict) and isinstance(bundle.get("continuity_state"), dict) else {}
    service_warnings = _coerce_str_list(continuity_state.get("warnings"))
    return {
        "raw_task": raw_task or "",
        "raw_subject_kind": raw_subject_kind or "",
        "raw_subject_id": raw_subject_id or "",
        "request_run": request_run,
        "request_params": request_params,
        "bundle": bundle,
        "continuity_state": continuity_state,
        "warnings": service_warnings + ui_warnings,
        "recovery_warnings": _coerce_str_list(continuity_state.get("recovery_warnings")),
        "empty_message": empty_message,
    }


def _context_response(state: dict[str, Any]) -> HTMLResponse:
    """Render the full context inspector page."""
    bundle = state["bundle"]
    continuity_state = state["continuity_state"]
    status_rows = [
        ("Retrieval run", _bool_label(bool(state["request_run"]))),
        ("Bundle available", _bool_label(isinstance(bundle, dict))),
        ("Generated at", str(bundle.get("generated_at") or "n/a") if isinstance(bundle, dict) else "n/a"),
        ("Read only", "true"),
    ]
    body = (
        '<section class="panel"><h2>Selector</h2>'
        f"{_context_selector_form(state)}</section>"
        '<section class="panel"><h2>Request Parameters</h2>'
        f"{_context_request_params_html(state['request_params'])}</section>"
        '<section class="panel"><h2>Retrieval Status</h2>'
        f"{_definition_rows(status_rows)}{_context_empty_message(state)}</section>"
        '<section class="panel"><h2>Warnings</h2>'
        f"{_html_list(state['warnings'])}</section>"
        '<section class="panel"><h2>Recovery Warnings</h2>'
        f"{_html_list(state['recovery_warnings'])}</section>"
        '<section class="panel"><h2>Token Budget</h2>'
        f"{_context_token_budget_html(bundle, continuity_state)}</section>"
        '<section class="panel"><h2>Continuity State</h2>'
        f"{_context_continuity_state_html(continuity_state)}</section>"
        '<section class="panel"><h2>Recent Relevant</h2>'
        f"{_context_recent_relevant_html(bundle)}</section>"
        '<section class="panel"><h2>Open Questions</h2>'
        f"{_html_list(_coerce_str_list(bundle.get('open_questions') if isinstance(bundle, dict) else None))}</section>"
        '<section class="panel"><h2>Notes</h2>'
        f"{_html_list(_coerce_str_list(bundle.get('notes') if isinstance(bundle, dict) else None))}</section>"
    )
    return _page(title="Context Retrieval", current_path="/ui/context", content=body)


def _context_selector_form(state: dict[str, Any]) -> str:
    """Render the read-only GET selector form, preserving raw values."""
    return (
        '<form method="get" action="/ui/context" class="filter-form">'
        '<label>Task'
        f'<input type="text" name="task" value="{html.escape(state["raw_task"])}">'
        "</label>"
        '<label>Subject Kind'
        f'<input type="text" name="subject_kind" value="{html.escape(state["raw_subject_kind"])}">'
        "</label>"
        '<label>Subject ID'
        f'<input type="text" name="subject_id" value="{html.escape(state["raw_subject_id"])}">'
        "</label>"
        '<button type="submit">Run Retrieval</button>'
        "</form>"
    )


def _context_empty_message(state: dict[str, Any]) -> str:
    """Render the default empty-state message when no request has run."""
    message = state.get("empty_message")
    if not message:
        return ""
    return f'<p class="muted">{html.escape(str(message))}</p>'


def _context_request_params_html(rows: list[tuple[str, str]] | None) -> str:
    """Render attempted/effective context request params or n/a for no run."""
    if rows is None:
        return '<p class="muted">n/a</p>'
    return _definition_rows(rows)


def _context_token_budget_html(bundle: dict[str, Any] | None, continuity_state: dict[str, Any]) -> str:
    """Render top-level and continuity budget posture."""
    budget = continuity_state.get("budget") if isinstance(continuity_state.get("budget"), dict) else {}
    rows = [
        ("token_budget_hint", str(bundle.get("token_budget_hint") or "n/a") if isinstance(bundle, dict) else "n/a"),
        ("continuity_state.budget", _context_json(budget) if budget else "n/a"),
    ]
    if isinstance(budget, dict):
        for key in sorted(budget):
            rows.append((f"budget.{key}", _context_render_value(budget.get(key))))
    return _definition_rows(rows)


def _context_continuity_state_html(continuity_state: dict[str, Any]) -> str:
    """Render continuity state aggregate fields and capsule rows."""
    capsules = continuity_state.get("capsules") if isinstance(continuity_state.get("capsules"), list) else []
    rows = [
        ("present", _context_render_value(continuity_state.get("present"))),
        ("fallback_used", _context_render_value(continuity_state.get("fallback_used"))),
        ("requested_selector_count", _context_render_value(continuity_state.get("requested_selector_count"))),
        ("omitted_selector_count", _context_render_value(continuity_state.get("omitted_selector_count"))),
        ("capsule_count", str(len(capsules))),
        ("trust_signals", _context_render_value(continuity_state.get("trust_signals"))),
        ("salience_metadata", _context_render_value(continuity_state.get("salience_metadata"))),
    ]
    return (
        f"{_definition_rows(rows)}"
        f"{_html_table(
            headers=['Subject Kind', 'Subject ID', 'Source State', 'Path', 'Health / Status / Trust / Degraded', 'Recovery Warnings', 'Warnings'],
            rows=_context_capsule_rows(capsules),
            empty_message='No continuity capsules returned.',
        )}"
    )


def _context_capsule_rows(capsules: list[Any]) -> list[list[str]]:
    """Render capsule rows with graceful per-row degradation."""
    rows: list[list[str]] = []
    for capsule in capsules:
        if not isinstance(capsule, dict):
            rows.append(["n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
            continue
        signal_fields = {
            key: capsule.get(key)
            for key in ("health_status", "status", "verification_status", "trust_signals", "degraded", "degraded_reason")
            if key in capsule
        }
        rows.append(
            [
                html.escape(str(capsule.get("subject_kind") or "n/a")),
                html.escape(str(capsule.get("subject_id") or "n/a")),
                html.escape(str(capsule.get("source_state") or "n/a")),
                html.escape(str(capsule.get("path") or "n/a")),
                html.escape(_context_render_value(signal_fields) if signal_fields else "n/a"),
                html.escape(_context_count_or_na(capsule.get("recovery_warnings"))),
                html.escape(_context_count_or_na(capsule.get("warnings"))),
            ]
        )
    return rows


def _context_recent_relevant_html(bundle: dict[str, Any] | None) -> str:
    """Render recent_relevant in service order with #251 field/link behavior."""
    recent = bundle.get("recent_relevant") if isinstance(bundle, dict) else None
    if not isinstance(recent, list) or not recent:
        return '<p class="muted">No recent relevant items returned.</p>'
    rows = [_context_recent_item_row(item) for item in recent]
    return _html_table(
        headers=["Path", "Type", "Score", "Modified", "Importance", "Warning", "Snippet", "Links"],
        rows=rows,
        empty_message="No recent relevant items returned.",
    )


def _context_recent_item_row(item: Any) -> list[str]:
    """Render one recent item, degrading malformed shapes in-row."""
    if not isinstance(item, dict):
        return ["n/a", "n/a", "n/a", "n/a", "n/a", "None", "n/a", ""]
    return [
        html.escape(item["path"]) if isinstance(item.get("path"), str) else "n/a",
        html.escape(item["type"]) if isinstance(item.get("type"), str) else "n/a",
        html.escape(str(item["score"])) if _context_is_scalar(item.get("score")) else "n/a",
        html.escape(item["modified_at"]) if isinstance(item.get("modified_at"), str) else "n/a",
        html.escape(str(item["importance"])) if _context_is_scalar(item.get("importance")) else "n/a",
        html.escape(item["warning"]) if isinstance(item.get("warning"), str) else "None",
        html.escape(item["snippet"]) if isinstance(item.get("snippet"), str) else "n/a",
        _context_recent_links(item),
    ]


def _context_recent_links(item: dict[str, Any]) -> str:
    """Render deterministic links from explicit returned identity fields only."""
    links: list[str] = []
    subject_kind = item.get("subject_kind")
    subject_id = item.get("subject_id")
    if isinstance(subject_kind, str) and isinstance(subject_id, str) and subject_kind in UI_SUBJECT_KINDS:
        if "/" not in subject_id:
            links.append(_continuity_subject_link(subject_kind, subject_id, "Continuity"))
        if subject_kind in {"thread", "task"}:
            links.append(_graph_query_link(subject_kind, subject_id, "Graph"))
    task_id = item.get("task_id")
    if isinstance(task_id, str):
        links.append(_task_detail_link(task_id))
    return " ".join(links)


def _context_is_scalar(value: Any) -> bool:
    """Return whether a recent field may be rendered as a scalar."""
    return isinstance(value, str | int | float | bool)


def _context_render_value(value: Any) -> str:
    """Return a deterministic string for scalar or structured context values."""
    if value is None:
        return "n/a"
    if _context_is_scalar(value):
        return str(value)
    return _context_json(value)


def _context_json(value: Any) -> str:
    """Return deterministic JSON for structured context values."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _context_count_or_na(value: Any) -> str:
    """Render warning counts when present."""
    if isinstance(value, list):
        return str(len(value))
    if value is None:
        return "n/a"
    return _context_render_value(value)


def _definition_rows_html(rows: list[tuple[str, str]]) -> str:
    """Render key/value rows where values are already escaped HTML fragments."""
    parts = ['<dl class="kv-list">']
    for label, value in rows:
        parts.append(f"<dt>{html.escape(label)}</dt><dd>{value}</dd>")
    parts.append("</dl>")
    return "".join(parts)


def _bool_label(value: bool) -> str:
    """Return a lowercase boolean label for human-readable tables."""
    return "true" if value else "false"


def _sse_event(event: str, data: dict[str, Any], event_id: int) -> str:
    """Encode one deterministic SSE event frame."""
    payload = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return f"id: {event_id}\nevent: {event}\ndata: {payload}\n\n"


def _render_graph_response(*, settings: Settings, subject_kind: str | None, subject_id: str | None) -> HTMLResponse:
    """Call the graph helper and render the graph inspector page."""
    try:
        graph = _ui_live_graph_summary(settings=settings, subject_kind=subject_kind, subject_id=subject_id)
    except Exception:
        graph = _empty_ui_graph_summary(
            subject_kind=subject_kind,
            subject_id=subject_id,
            warning="graph_derivation_failed",
        )
    live_stream_path = None
    if subject_kind is not None and subject_id is not None:
        live_stream_path = _ui_events_href(graph_subject_kind=subject_kind, graph_subject_id=subject_id)
    return _graph_page(
        current_path="/ui/graph",
        subject_kind=subject_kind,
        subject_id=subject_id,
        graph=graph,
        live_stream_path=live_stream_path,
    )


def _graph_page(
    *,
    current_path: str,
    subject_kind: str | None,
    subject_id: str | None,
    graph: dict[str, Any] | None,
    live_stream_path: str | None,
) -> HTMLResponse:
    """Render the graph inspector around optional helper-backed sections."""
    graph_sections = ""
    if graph is None:
        empty_state = '<section class="panel"><p class="muted">No graph anchor selected.</p></section>'
        graph_sections = empty_state
    else:
        sections = graph.get("sections") if isinstance(graph.get("sections"), dict) else _graph_sections(graph)
        live_panel = ""
        if live_stream_path is not None:
            live_panel = (
                f'<section class="panel" data-live-page="graph" data-live-stream="{html.escape(live_stream_path)}">'
                "<h2>Live Updates</h2>"
                '<p class="muted" data-live-connection>Live updates waiting for connection.</p>'
                '<p class="muted">Data refreshed: <span data-live-generated-at>Not connected</span></p>'
                "<noscript><p class=\"muted\">JavaScript disabled; live updates are unavailable.</p></noscript>"
                "</section>"
            )
        graph_sections = (
            '<section class="panel"><h2>Anchor</h2><div data-live-graph-anchor>'
            f'{sections["anchor_html"]}</div></section>'
            '<section class="panel"><h2>Source / Status</h2><div data-live-graph-source-status>'
            f'{sections["source_status_html"]}</div></section>'
            '<section class="panel"><h2>Warnings</h2><div data-live-graph-warnings>'
            f'{sections["warnings_html"]}</div></section>'
            '<section class="panel"><h2>Nodes</h2><div data-live-graph-nodes>'
            f'{sections["nodes_html"]}</div></section>'
            '<section class="panel"><h2>Edges</h2><div data-live-graph-edges>'
            f'{sections["edges_html"]}</div></section>'
            f"{live_panel}"
        )
    body = render_template(
        "graph.html",
        subject_kind_value=html.escape(subject_kind or ""),
        subject_id_value=html.escape(subject_id or ""),
        graph_sections=graph_sections,
    )
    return _page(title="Derived Graph", current_path=current_path, content=body)


def _ui_live_snapshot(
    *,
    app_version: str,
    settings: Settings,
    auth: AuthContext,
    now: datetime,
    q: str | None,
    subject_kind: str | None,
    artifact_state: str | None,
    health_status: str | None,
    detail_subject_kind: str | None,
    detail_subject_id: str | None,
    graph_subject_kind: str | None,
    graph_subject_id: str | None,
) -> dict[str, Any]:
    """Build one bounded live-update snapshot for the operator UI."""
    warnings: list[str] = []
    overview = _empty_ui_overview_summary(warning=None)
    continuity = _empty_ui_continuity_summary(
        q=q,
        subject_kind=subject_kind,
        artifact_state=artifact_state,
        health_status=health_status,
        warning=None,
    )
    detail: dict[str, Any] | None = None
    graph: dict[str, Any] | None = None

    try:
        overview = _ui_live_overview_summary(
            app_version=app_version,
            settings=settings,
            auth=auth,
            now=now,
        )
    except Exception as exc:
        warnings.append(f"ui_overview_snapshot_failed:{exc.__class__.__name__}")
        overview = _empty_ui_overview_summary(warning="ui_overview_snapshot_failed")

    try:
        continuity = _ui_live_continuity_summary(
            settings=settings,
            auth=auth,
            now=now,
            q=q,
            subject_kind=subject_kind,
            artifact_state=artifact_state,
            health_status=health_status,
        )
    except Exception as exc:
        warnings.append(f"ui_continuity_snapshot_failed:{exc.__class__.__name__}")
        continuity = _empty_ui_continuity_summary(
            q=q,
            subject_kind=subject_kind,
            artifact_state=artifact_state,
            health_status=health_status,
            warning="ui_continuity_snapshot_failed",
        )

    if detail_subject_kind and detail_subject_id:
        try:
            detail = _ui_live_detail_summary(
                settings=settings,
                auth=auth,
                now=now,
                subject_kind=detail_subject_kind,
                subject_id=detail_subject_id,
            )
        except Exception as exc:
            warnings.append(f"ui_detail_snapshot_failed:{exc.__class__.__name__}")
            detail = _empty_ui_detail_summary(
                subject_kind=detail_subject_kind,
                subject_id=detail_subject_id,
                warning="ui_detail_snapshot_failed",
            )

    if (
        graph_subject_kind is not None
        and graph_subject_id is not None
        and graph_subject_kind != ""
        and graph_subject_id != ""
    ):
        try:
            graph = _ui_live_graph_summary(
                settings=settings,
                subject_kind=graph_subject_kind,
                subject_id=graph_subject_id,
            )
        except Exception as exc:
            warnings.append(f"ui_graph_snapshot_failed:{exc.__class__.__name__}")
            graph = _empty_ui_graph_summary(
                subject_kind=graph_subject_kind,
                subject_id=graph_subject_id,
                warning="ui_graph_snapshot_failed",
            )

    return {
        "schema_version": "1.0",
        "ok": not warnings,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "warnings": warnings,
        "overview": overview,
        "continuity": continuity,
        "detail": detail,
        "graph": graph,
    }


def _ui_live_overview_summary(
    *,
    app_version: str,
    settings: Settings,
    auth: AuthContext,
    now: datetime,
) -> dict[str, Any]:
    """Build the bounded overview summary used by the SSE stream."""
    gm = _ui_git_manager(settings)
    health = health_payload(
        app_version=app_version,
        contract_version=settings.contract_version,
        repo_root=str(settings.repo_root),
        git_initialized=gm.is_repo(),
        latest_commit=gm.latest_commit(),
        signed_ingress_required=bool(settings.require_signed_ingress),
    )
    continuity_counts = _continuity_counts(repo_root=settings.repo_root, auth=auth, now=now)
    return {
        "available": True,
        "warning": None,
        "service": str(health.get("service", "")),
        "version": str(health.get("version", "")),
        "contract_version": str(health.get("contract_version", "")),
        "git_initialized": bool(health.get("git_initialized")),
        "latest_commit": str(health.get("latest_commit") or "none"),
        "reported_at": str(health.get("time", "")),
        "continuity_counts": continuity_counts,
    }


def _ui_live_continuity_summary(
    *,
    settings: Settings,
    auth: AuthContext,
    now: datetime,
    q: str | None,
    subject_kind: str | None,
    artifact_state: str | None,
    health_status: str | None,
) -> dict[str, Any]:
    """Build the bounded continuity summary used by the SSE stream."""
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
    recent_change = _latest_continuity_change(filtered_rows)
    latest_recorded_at = recent_change["recorded_at"] if recent_change is not None else "n/a"
    display_rows = filtered_rows[:UI_CONTINUITY_DISPLAY_LIMIT]
    return {
        "available": True,
        "warning": None,
        "scope": {
            "q": _normalized_query_display(q),
            "subject_kind": subject_kind or "",
            "artifact_state": artifact_state or "",
            "health_status": health_status or "",
        },
        "matched_count": len(filtered_rows),
        "displayed_count": len(display_rows),
        "result_truncated": len(filtered_rows) > UI_CONTINUITY_DISPLAY_LIMIT,
        "artifact_counts": lifecycle_counts,
        "latest_recorded_at": latest_recorded_at,
        "recent_change": recent_change,
        "table_html": _continuity_table_html(display_rows),
    }


def _ui_live_detail_summary(
    *,
    settings: Settings,
    auth: AuthContext,
    now: datetime,
    subject_kind: str,
    subject_id: str,
) -> dict[str, Any]:
    """Build the bounded detail-header summary used by the SSE stream."""
    detail = continuity_read_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=ContinuityReadRequest(
            subject_kind=subject_kind,
            subject_id=subject_id,
            allow_fallback=True,
            view="startup",
        ),
        now=now,
        audit=_noop_audit,
    )
    capsule = detail.get("capsule") or {}
    related_rows = _related_artifact_rows(
        repo_root=settings.repo_root,
        auth=auth,
        subject_kind=subject_kind,
        subject_id=subject_id,
        now=now,
        retention_archive_days=settings.continuity_retention_archive_days,
    )
    related_counts = _artifact_state_counts(related_rows)
    latest_recorded_at = _continuity_live_latest_recorded_at(related_rows)
    continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
    rendered_sections = _ui_detail_render_sections(
        detail=detail,
        capsule=capsule,
        continuity=continuity,
        subject_kind=subject_kind,
        startup_summary=_startup_summary_for_ui(detail.get("startup_summary")),
        trust_signals=detail.get("trust_signals"),
        related_summary=_related_artifact_summary_rows(subject_kind=subject_kind, rows=related_rows),
    )
    return {
        "available": True,
        "warning": None,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "source_state": str(detail.get("source_state", "unknown")),
        "updated_at": str(capsule.get("updated_at") or "n/a"),
        "verified_at": str(capsule.get("verified_at") or "n/a"),
        "recovery_warning_count": len(list(detail.get("recovery_warnings") or [])),
        "artifact_counts": related_counts,
        "latest_recorded_at": latest_recorded_at,
        "sections": {
            "related_artifact_rows": rendered_sections["related_artifact_rows"],
            "recovery_warnings_html": rendered_sections["recovery_warnings_html"],
            "startup_summary_html": rendered_sections["startup_summary_html"],
            "trust_signals_html": rendered_sections["trust_signals_html"],
            "top_priorities_html": rendered_sections["top_priorities_html"],
            "active_concerns_html": rendered_sections["active_concerns_html"],
            "active_constraints_html": rendered_sections["active_constraints_html"],
            "open_loops_html": rendered_sections["open_loops_html"],
            "session_trajectory_html": rendered_sections["session_trajectory_html"],
            "stance_summary_html": rendered_sections["stance_summary_html"],
            "related_documents_html": rendered_sections["related_documents_html"],
            "thread_descriptor_section": rendered_sections["thread_descriptor_section"],
            "stable_preferences_html": rendered_sections["stable_preferences_html"],
            "negative_decisions_html": rendered_sections["negative_decisions_html"],
            "rationale_entries_html": rendered_sections["rationale_entries_html"],
        },
    }


def _ui_live_graph_summary(*, settings: Settings, subject_kind: str | None, subject_id: str | None) -> dict[str, Any]:
    """Build the bounded graph summary used by the page and SSE stream."""
    result = derive_internal_graph_slice1(
        repo_root=settings.repo_root,
        subject_kind=subject_kind,
        subject_id=subject_id,
    )
    anchor = result.get("anchor") if isinstance(result.get("anchor"), dict) else None
    nodes = [node for node in list(result.get("nodes") or []) if isinstance(node, dict)]
    edges = [edge for edge in list(result.get("edges") or []) if isinstance(edge, dict)]
    warnings = [str(item) for item in list(result.get("warnings") or [])]
    graph = {
        "available": True,
        "warning": None,
        "subject_kind": _graph_display_value(subject_kind),
        "subject_id": _graph_display_value(subject_id),
        "source": "derived_on_demand",
        "helper": "derive_internal_graph_slice1",
        "read_only": True,
        "public_api_expanded": False,
        "anchor": anchor,
        "nodes": nodes,
        "edges": edges,
        "warnings": warnings,
        "summary": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "warning_count": len(warnings),
        },
    }
    graph["sections"] = _graph_sections(graph)
    return graph


def _empty_ui_graph_summary(*, subject_kind: str | None, subject_id: str | None, warning: str | None) -> dict[str, Any]:
    """Return a deterministic degraded graph summary."""
    graph = {
        "available": False,
        "warning": warning,
        "subject_kind": _graph_display_value(subject_kind),
        "subject_id": _graph_display_value(subject_id),
        "source": "derived_on_demand",
        "helper": "derive_internal_graph_slice1",
        "read_only": True,
        "public_api_expanded": False,
        "anchor": None,
        "nodes": [],
        "edges": [],
        "warnings": ["graph_derivation_failed"],
        "summary": {"node_count": 0, "edge_count": 0, "warning_count": 1},
    }
    graph["sections"] = _graph_sections(graph)
    return graph


def _graph_display_value(value: str | None) -> str:
    """Return the graph input display value required by #247."""
    if value is None:
        return "n/a"
    return str(value)


def _graph_sections(graph: dict[str, Any]) -> dict[str, str]:
    """Render escaped graph HTML fragments for page and SSE reuse."""
    anchor = graph.get("anchor") if isinstance(graph.get("anchor"), dict) else None
    nodes = [node for node in list(graph.get("nodes") or []) if isinstance(node, dict)]
    edges = [edge for edge in list(graph.get("edges") or []) if isinstance(edge, dict)]
    warnings = [str(item) for item in list(graph.get("warnings") or [])]
    summary = graph.get("summary") if isinstance(graph.get("summary"), dict) else {}
    if anchor is None:
        anchor_html = '<p class="muted">No graph anchor resolved.</p>'
    else:
        anchor_html = _definition_rows(
            [
                ("ID", str(anchor.get("id", ""))),
                ("Family", str(anchor.get("family", ""))),
            ]
        )
    return {
        "anchor_html": anchor_html,
        "source_status_html": _definition_rows(
            [
                ("Subject kind", str(graph.get("subject_kind") or "n/a")),
                ("Subject ID", str(graph.get("subject_id") or "n/a")),
                ("Source", "derived_on_demand"),
                ("Helper", "derive_internal_graph_slice1"),
                ("Read only", "true"),
                ("Public API expanded", "false"),
                ("Node count", str(summary.get("node_count", len(nodes)))),
                ("Edge count", str(summary.get("edge_count", len(edges)))),
                ("Warning count", str(summary.get("warning_count", len(warnings)))),
            ]
        ),
        "warnings_html": _html_list(warnings),
        "nodes_html": _html_table(
            headers=["ID", "Family"],
            rows=[
                [
                    html.escape(str(node.get("id", ""))),
                    html.escape(str(node.get("family", ""))),
                ]
                for node in nodes
            ],
            empty_message="No graph neighbor nodes were derived for this anchor.",
        ),
        "edges_html": _html_table(
            headers=["Family", "Source", "Target"],
            rows=[
                [
                    html.escape(str(edge.get("family", ""))),
                    html.escape(str(edge.get("source_id", ""))),
                    html.escape(str(edge.get("target_id", ""))),
                ]
                for edge in edges
            ],
            empty_message="No graph edges were derived for this anchor.",
        ),
    }


def _empty_ui_overview_summary(*, warning: str | None) -> dict[str, Any]:
    """Return a deterministic degraded overview summary."""
    return {
        "available": False,
        "warning": warning,
        "service": "",
        "version": "",
        "contract_version": "",
        "git_initialized": False,
        "latest_commit": "unavailable",
        "reported_at": "unavailable",
        "continuity_counts": {
            "active": 0,
            "fallback": 0,
            "archived": 0,
            "cold": 0,
            "by_subject_kind": {kind: 0 for kind in UI_SUBJECT_KINDS},
        },
    }


def _empty_ui_continuity_summary(
    *,
    q: str | None,
    subject_kind: str | None,
    artifact_state: str | None,
    health_status: str | None,
    warning: str | None,
) -> dict[str, Any]:
    """Return a deterministic degraded continuity summary."""
    return {
        "available": False,
        "warning": warning,
        "scope": {
            "q": _normalized_query_display(q),
            "subject_kind": subject_kind or "",
            "artifact_state": artifact_state or "",
            "health_status": health_status or "",
        },
        "matched_count": 0,
        "displayed_count": 0,
        "result_truncated": False,
        "artifact_counts": {state: 0 for state in UI_ARTIFACT_STATES},
        "latest_recorded_at": "unavailable",
        "recent_change": None,
        "table_html": '<p class="muted">No continuity capsules matched the current filter.</p>',
    }


def _empty_ui_detail_summary(*, subject_kind: str, subject_id: str, warning: str | None) -> dict[str, Any]:
    """Return a deterministic degraded detail summary."""
    return {
        "available": False,
        "warning": warning,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "source_state": "unavailable",
        "updated_at": "unavailable",
        "verified_at": "unavailable",
        "recovery_warning_count": 0,
        "artifact_counts": {state: 0 for state in UI_ARTIFACT_STATES},
        "latest_recorded_at": "unavailable",
        "sections": {
            "related_artifact_rows": '<p class="muted">No related lifecycle artifacts were found for this subject.</p>',
            "recovery_warnings_html": '<p class="muted">None</p>',
            "startup_summary_html": '<p class="muted">Startup summary unavailable.</p>',
            "trust_signals_html": '<p class="muted">Trust signals unavailable.</p>',
            "top_priorities_html": '<p class="muted">None</p>',
            "active_concerns_html": '<p class="muted">None</p>',
            "active_constraints_html": '<p class="muted">None</p>',
            "open_loops_html": '<p class="muted">None</p>',
            "session_trajectory_html": '<p class="muted">None</p>',
            "stance_summary_html": '<p class="muted">None</p>',
            "related_documents_html": '<p class="muted">No related documents recorded.</p>',
            "thread_descriptor_section": "",
            "stable_preferences_html": _stable_preferences_html(capsule={}, subject_kind=subject_kind),
            "negative_decisions_html": '<p class="muted">No negative decisions recorded.</p>',
            "rationale_entries_html": '<p class="muted">No rationale entries recorded.</p>',
        },
    }


def _ui_events_href(
    *,
    q: str | None = None,
    subject_kind: str | None = None,
    artifact_state: str | None = None,
    health_status: str | None = None,
    detail_subject_kind: str | None = None,
    detail_subject_id: str | None = None,
    graph_subject_kind: str | None = None,
    graph_subject_id: str | None = None,
) -> str:
    """Build a deterministic SSE URL for one page scope."""
    pairs: list[tuple[str, str]] = []
    if q is not None and _normalized_query_display(q):
        pairs.append(("q", _normalized_query_display(q)))
    if subject_kind is not None:
        pairs.append(("subject_kind", subject_kind))
    if artifact_state is not None:
        pairs.append(("artifact_state", artifact_state))
    if health_status is not None:
        pairs.append(("health_status", health_status))
    if detail_subject_kind is not None:
        pairs.append(("detail_subject_kind", detail_subject_kind))
    if detail_subject_id is not None:
        pairs.append(("detail_subject_id", detail_subject_id))
    if graph_subject_kind is not None:
        pairs.append(("graph_subject_kind", graph_subject_kind))
    if graph_subject_id is not None:
        pairs.append(("graph_subject_id", graph_subject_id))
    if not pairs:
        return "/ui/events"
    return "/ui/events?" + urlencode(pairs)


def _latest_continuity_change(rows: list[dict[str, Any]]) -> dict[str, str] | None:
    """Return the latest lifecycle change visible in the current continuity scope."""
    latest_row: dict[str, Any] | None = None
    latest_sort_key: tuple[str, str, str, str] | None = None
    for row in rows:
        recorded_at = _display_recorded_at(row)
        sort_key = (
            recorded_at,
            str(row.get("subject_kind") or ""),
            str(row.get("subject_id") or ""),
            str(row.get("artifact_state") or ""),
        )
        if latest_sort_key is None or sort_key > latest_sort_key:
            latest_sort_key = sort_key
            latest_row = row
    if latest_row is None:
        return None
    return {
        "subject_kind": str(latest_row.get("subject_kind") or ""),
        "subject_id": str(latest_row.get("subject_id") or ""),
        "artifact_state": str(latest_row.get("artifact_state") or ""),
        "recorded_at": _display_recorded_at(latest_row),
    }


def _continuity_live_latest_recorded_at(rows: list[dict[str, Any]]) -> str:
    """Return the initial continuity live-summary timestamp label."""
    recent_change = _latest_continuity_change(rows)
    if recent_change is None:
        return "n/a"
    return recent_change["recorded_at"]


def _continuity_live_recent_change_label(rows: list[dict[str, Any]]) -> str:
    """Return the initial continuity live-summary recent change label."""
    recent_change = _latest_continuity_change(rows)
    return _recent_change_label(recent_change)


def _recent_change_label(recent_change: dict[str, str] | None) -> str:
    """Render one bounded recent-change summary label."""
    if recent_change is None:
        return "No recent continuity change in the current view."
    return (
        f'{recent_change["subject_kind"]}/{recent_change["subject_id"]} '
        f'[{recent_change["artifact_state"]}] at {recent_change["recorded_at"]}'
    )


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


def _normalized_query_display(value: str | None) -> str:
    """Return the displayed query value using the same normalization as matching."""
    return " ".join(_search_tokens(value))


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


def _continuity_table_html(rows: list[dict[str, Any]]) -> str:
    """Render the continuity list table for both page and live updates."""
    table_rows: list[list[str]] = []
    for capsule in rows:
        table_rows.append(
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
    return _html_table(
        headers=["Kind", "Subject", "Lifecycle", "Artifact", "Recorded", "Health", "Signals"],
        rows=table_rows,
        empty_message="No continuity capsules matched the current filter.",
    )


def _ui_detail_render_sections(
    *,
    detail: dict[str, Any],
    capsule: dict[str, Any],
    continuity: dict[str, Any],
    subject_kind: str,
    startup_summary: Any,
    trust_signals: Any,
    related_summary: list[list[str]],
) -> dict[str, str]:
    """Render bounded continuity detail fragments for page and SSE refreshes."""
    return {
        "related_artifact_rows": _html_table(
            headers=["Lifecycle", "Present", "Count", "Latest artifact", "Recorded", "Browse"],
            rows=related_summary,
            empty_message="No related lifecycle artifacts were found for this subject.",
        ),
        "recovery_warnings_html": _html_list([str(item) for item in detail.get("recovery_warnings", [])]),
        "startup_summary_html": _render_summary_document(startup_summary, empty_message="Startup summary unavailable."),
        "trust_signals_html": _render_summary_document(trust_signals, empty_message="Trust signals unavailable."),
        "top_priorities_html": _html_list(_coerce_str_list(continuity.get("top_priorities"))),
        "active_concerns_html": _html_list(_coerce_str_list(continuity.get("active_concerns"))),
        "active_constraints_html": _html_list(_coerce_str_list(continuity.get("active_constraints"))),
        "open_loops_html": _html_list(_coerce_str_list(continuity.get("open_loops"))),
        "session_trajectory_html": _html_list(_coerce_str_list(continuity.get("session_trajectory"))),
        "stance_summary_html": _paragraph(continuity.get("stance_summary")),
        "related_documents_html": _related_documents_table(continuity.get("related_documents")),
        "thread_descriptor_section": _thread_descriptor_section(capsule.get("thread_descriptor")),
        "stable_preferences_html": _stable_preferences_html(capsule=capsule, subject_kind=subject_kind),
        "negative_decisions_html": _structured_table(
            rows=list(continuity.get("negative_decisions") or []),
            columns=["decision", "rationale", "created_at", "updated_at", "last_confirmed_at"],
            empty_message="No negative decisions recorded.",
        ),
        "rationale_entries_html": _structured_table(
            rows=list(continuity.get("rationale_entries") or []),
            columns=["tag", "kind", "status", "summary", "reasoning", "updated_at"],
            empty_message="No rationale entries recorded.",
        ),
    }


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
    return f'<div class="table-wrap"><table class="table-hover"><thead><tr>{head}</tr></thead><tbody>{body_rows}</tbody></table></div>'


def _subject_link(subject_kind: str, subject_id: str) -> str:
    """Render the continuity detail link for one subject."""
    href = f"/ui/continuity/{quote(subject_kind, safe='')}/{quote(subject_id, safe='')}"
    return f'<a href="{href}">{html.escape(subject_id)}</a>'


def _continuity_graph_link_section(subject_kind: str, subject_id: str) -> str:
    """Render the continuity-detail graph link for graph-supported subjects only."""
    if subject_kind not in {"thread", "task"}:
        return ""
    href = f"/ui/graph/{quote(subject_kind, safe='')}/{quote(subject_id, safe='')}"
    return (
        '<section class="panel">'
        "<h2>Derived Graph</h2>"
        f'<p><a href="{href}">Open derived graph for {html.escape(subject_kind)}/{html.escape(subject_id)}</a></p>'
        "</section>"
    )


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


def _related_documents_table(value: Any) -> str:
    """Render read-only related document metadata for one continuity capsule."""
    rows = value if isinstance(value, list) else []
    return _structured_table(
        rows=rows,
        columns=["path", "kind", "label", "relevance"],
        empty_message="No related documents recorded.",
    )


def _thread_descriptor_section(value: Any) -> str:
    """Render a read-only thread descriptor section when descriptor metadata exists."""
    if not isinstance(value, dict) or not value:
        return ""
    descriptor = {
        "label": value.get("label"),
        "keywords": value.get("keywords"),
        "scope_anchors": value.get("scope_anchors"),
        "identity_anchors": value.get("identity_anchors"),
        "lifecycle": value.get("lifecycle"),
        "superseded_by": value.get("superseded_by"),
    }
    return (
        '<article class="panel">'
        "<h2>Thread Descriptor</h2>"
        f"{_render_summary_rows(descriptor)}"
        "</article>"
    )


def _stable_preferences_html(*, capsule: dict[str, Any], subject_kind: str) -> str:
    """Render stable preferences only for subject kinds where they are valid."""
    if subject_kind not in {"user", "peer"}:
        return '<p class="muted">Not applicable for this subject kind.</p>'
    return _structured_table(
        rows=list(capsule.get("stable_preferences") or []),
        columns=["tag", "content", "created_at", "updated_at", "last_confirmed_at"],
        empty_message="No stable preferences recorded.",
    )


def _startup_summary_for_ui(value: Any) -> Any:
    """Trim startup-summary data to fields that are not already rendered elsewhere."""
    if not isinstance(value, dict):
        return value
    filtered: dict[str, Any] = {}
    if "recovery" in value:
        filtered["recovery"] = value.get("recovery")
    if "updated_at" in value:
        filtered["updated_at"] = value.get("updated_at")
    return filtered


def _render_summary_document(value: Any, *, empty_message: str) -> str:
    """Render dense summary/trust data using flatter, full-width groups."""
    if value is None:
        return f'<p class="muted">{html.escape(empty_message)}</p>'
    if not isinstance(value, dict):
        return _render_value(value)
    if not value:
        return '<p class="muted">None</p>'
    parts = ['<div class="summary-document">']
    for key, item in value.items():
        parts.append(
            "<section class=\"summary-group\">"
            f"<h3>{html.escape(str(key))}</h3>"
            f"{_render_summary_content(item)}"
            "</section>"
        )
    parts.append("</div>")
    return "".join(parts)


def _render_summary_content(value: Any) -> str:
    """Render one summary-group value using flatter rows/cards."""
    if value is None:
        return '<p class="muted">None</p>'
    if isinstance(value, bool):
        return html.escape(_bool_label(value))
    if isinstance(value, (int, float)):
        return html.escape(str(value))
    if isinstance(value, str):
        return html.escape(value)
    if isinstance(value, dict):
        return _render_summary_rows(value)
    if isinstance(value, list):
        if not value:
            return '<p class="muted">None</p>'
        if all(isinstance(item, dict) for item in value):
            parts = ['<div class="summary-card-list">']
            for item in value:
                parts.append(f'<article class="summary-card">{_render_summary_rows(item)}</article>')
            parts.append("</div>")
            return "".join(parts)
        return '<ul class="summary-list">' + "".join(f"<li>{_render_summary_content(item)}</li>" for item in value) + "</ul>"
    return html.escape(str(value))


def _render_summary_rows(value: dict[str, Any]) -> str:
    """Render key/value rows for one summary-group or summary-card."""
    parts = ['<div class="summary-rows">']
    for key, item in value.items():
        parts.append(
            "<div class=\"summary-row-item\">"
            f"<div class=\"summary-key\">{html.escape(str(key))}</div>"
            f"<div class=\"summary-value\">{_render_summary_content(item)}</div>"
            "</div>"
        )
    parts.append("</div>")
    return "".join(parts)


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
