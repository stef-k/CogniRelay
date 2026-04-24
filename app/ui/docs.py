"""Read-only documentation browser helpers for the operator UI."""

from __future__ import annotations

import html
import posixpath
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urldefrag, urlsplit

import bleach
import markdown
from markdown.extensions.toc import TocExtension


@dataclass(frozen=True)
class UiDoc:
    """One fixed documentation entry renderable by the UI."""

    doc_id: str
    path: str
    title: str
    description: str


@dataclass(frozen=True)
class UiDocStatus:
    """Availability state for one allowlisted UI document."""

    doc: UiDoc
    available: bool
    warning: str | None = None


@dataclass(frozen=True)
class RenderedDoc:
    """Sanitized Markdown output and matching h2/h3 table of contents."""

    content_html: str
    toc_html: str


UI_DOCS: tuple[UiDoc, ...] = (
    UiDoc("readme", "README.md", "README", "Project overview, quick start, runtime shape, and canonical doc map."),
    UiDoc(
        "system-overview",
        "docs/system-overview.md",
        "System Overview",
        "Product shape, architecture, deployment topology, and agent usage model.",
    ),
    UiDoc(
        "payload-reference",
        "docs/payload-reference.md",
        "Payload Reference",
        "Continuity, retrieval, coordination, and schema payload reference.",
    ),
    UiDoc(
        "api-surface",
        "docs/api-surface.md",
        "API Surface",
        "Human-facing summary of implemented HTTP and runtime help surfaces.",
    ),
    UiDoc("mcp", "docs/mcp.md", "MCP Guide", "MCP bootstrap flow, tool model, mappings, and response behavior."),
    UiDoc(
        "cognirelay-client",
        "docs/cognirelay-client.md",
        "CogniRelay CLI Client",
        "Command-line client requirements, usage, and examples.",
    ),
    UiDoc(
        "agent-onboarding",
        "docs/agent-onboarding.md",
        "Agent Onboarding",
        "Practical startup, hook, workflow, retrieval, and anti-pattern guidance for agents.",
    ),
)
UI_DOCS_BY_ID: dict[str, UiDoc] = {doc.doc_id: doc for doc in UI_DOCS}
_UI_DOC_IDS_BY_PATH: dict[str, str] = {doc.path: doc.doc_id for doc in UI_DOCS}
_ALLOWED_TAGS = {
    "a",
    "p",
    "br",
    "strong",
    "em",
    "code",
    "pre",
    "blockquote",
    "ul",
    "ol",
    "li",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "table",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
    "hr",
}
_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}


def doc_statuses(repo_root: Path) -> list[UiDocStatus]:
    """Return fixed-order availability for all allowlisted docs."""
    return [_doc_status(repo_root, doc) for doc in UI_DOCS]


def read_doc_source(repo_root: Path, doc: UiDoc) -> tuple[str | None, str | None]:
    """Read one allowlisted doc, degrading to deterministic warning codes."""
    path = repo_root / doc.path
    if not path.is_file():
        return None, f"doc_missing:{doc.doc_id}"
    try:
        return path.read_text(encoding="utf-8"), None
    except OSError:
        return None, f"doc_unreadable:{doc.doc_id}"
    except UnicodeError:
        return None, f"doc_unreadable:{doc.doc_id}"


def render_doc_markdown(*, source: str, doc: UiDoc) -> RenderedDoc:
    """Render Markdown to sanitized UI HTML with allowlisted doc-link behavior."""
    md = markdown.Markdown(
        extensions=[
            "fenced_code",
            "tables",
            TocExtension(slugify=_slugify_heading, toc_depth="2-3"),
        ],
        output_format="html",
    )
    rendered = md.convert(source)
    rendered = _normalize_rendered_anchors(rendered, doc=doc)
    cleaned = bleach.Cleaner(
        tags=_ALLOWED_TAGS,
        attributes=_allowed_attribute,
        protocols={"http", "https"},
        strip=True,
        strip_comments=True,
    ).clean(rendered)
    return RenderedDoc(
        content_html=cleaned,
        toc_html=_toc_html(md.toc_tokens),
    )


def normalize_fragment(fragment: str) -> str:
    """Normalize a URL fragment according to the docs UI contract."""
    value = fragment[1:] if fragment.startswith("#") else fragment
    value = value.strip(" \t\r\n\f\v")
    if not value:
        return ""
    return quote(value, safe="-._~")


def _doc_status(repo_root: Path, doc: UiDoc) -> UiDocStatus:
    source, warning = read_doc_source(repo_root, doc)
    return UiDocStatus(doc=doc, available=source is not None, warning=warning)


def _slugify_heading(value: str, _separator: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "section"


def _allowed_attribute(tag: str, name: str, value: str) -> bool:
    if tag == "a" and name in {"title", "rel"}:
        return True
    if tag == "a" and name == "href":
        return _allowed_href(value)
    if tag in _HEADING_TAGS and name == "id":
        return True
    return False


def _allowed_href(value: str) -> bool:
    if value.startswith("http://") or value.startswith("https://"):
        return True
    if value.startswith("#"):
        fragment = value[1:]
        return _is_normalized_fragment(fragment)
    split = urlsplit(value)
    if split.scheme or split.netloc or split.query:
        return False
    if split.path not in {f"/ui/docs/{doc.doc_id}" for doc in UI_DOCS}:
        return False
    return not split.fragment or _is_normalized_fragment(split.fragment)


def _is_normalized_fragment(fragment: str) -> bool:
    return bool(fragment) and quote(unquote(fragment), safe="-._~") == fragment


def _toc_html(tokens: list[dict[str, Any]]) -> str:
    items: list[str] = []
    for token in _flatten_toc_tokens(tokens):
        level = int(token.get("level", 0))
        if level not in {2, 3}:
            continue
        anchor = str(token.get("id", ""))
        name = str(token.get("name", ""))
        if not anchor or not name:
            continue
        items.append(f'<li><a href="#{html.escape(anchor, quote=True)}">{html.escape(name)}</a></li>')
    if not items:
        return '<p class="muted">No section headings available.</p>'
    return "<ul>" + "".join(items) + "</ul>"


def _flatten_toc_tokens(tokens: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for token in tokens:
        flattened.append(token)
        children = token.get("children")
        if isinstance(children, list):
            flattened.extend(_flatten_toc_tokens(children))
    return flattened


def _normalize_rendered_anchors(content: str, *, doc: UiDoc) -> str:
    """Normalize rendered anchors before final sanitizer validation."""
    parser = _RenderedAnchorNormalizer(doc=doc)
    parser.feed(content)
    parser.close()
    return parser.content


class _RenderedAnchorNormalizer(HTMLParser):
    """Rewrite all rendered anchors through the UI docs link allowlist."""

    def __init__(self, *, doc: UiDoc) -> None:
        super().__init__(convert_charrefs=False)
        self.doc = doc
        self._parts: list[str] = []
        self._anchor: dict[str, Any] | None = None
        self._anchor_depth = 0

    @property
    def content(self) -> str:
        """Return normalized HTML collected so far."""
        return "".join(self._parts)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a" and self._anchor is None:
            self._anchor = {"attrs": attrs, "html": [], "text": []}
            self._anchor_depth = 1
            return
        if self._anchor is not None:
            self._anchor_depth += 1
            self._anchor["html"].append(self._format_starttag(tag, attrs))
            return
        self._parts.append(self._format_starttag(tag, attrs))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        target = self._anchor["html"] if self._anchor is not None else self._parts
        target.append(self._format_starttag(tag, attrs, startend=True))

    def handle_endtag(self, tag: str) -> None:
        if self._anchor is not None:
            self._anchor_depth -= 1
            if tag == "a" and self._anchor_depth == 0:
                self._parts.append(self._render_anchor())
                self._anchor = None
                return
            self._anchor["html"].append(f"</{tag}>")
            return
        self._parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self._anchor is not None:
            self._anchor["html"].append(html.escape(data, quote=False))
            self._anchor["text"].append(data)
            return
        self._parts.append(html.escape(data, quote=False))

    def handle_entityref(self, name: str) -> None:
        entity = f"&{name};"
        if self._anchor is not None:
            self._anchor["html"].append(entity)
            self._anchor["text"].append(html.unescape(entity))
            return
        self._parts.append(entity)

    def handle_charref(self, name: str) -> None:
        charref = f"&#{name};"
        if self._anchor is not None:
            self._anchor["html"].append(charref)
            self._anchor["text"].append(html.unescape(charref))
            return
        self._parts.append(charref)

    def handle_comment(self, data: str) -> None:
        target = self._anchor["html"] if self._anchor is not None else self._parts
        target.append(f"<!--{data}-->")

    def _render_anchor(self) -> str:
        attrs = list(self._anchor["attrs"])
        href = _attr_value(attrs, "href")
        replacement = _replacement_href(self.doc, href or "")
        if replacement is None:
            text = "".join(self._anchor["text"])
            return html.escape(text, quote=False) + " (not available in UI docs allowlist)"

        normalized_attrs = [(name, value) for name, value in attrs if name not in {"href", "rel"}]
        normalized_attrs.append(("href", replacement))
        if replacement.startswith("http://") or replacement.startswith("https://"):
            normalized_attrs.append(("rel", "noreferrer"))
        return f"<a{_format_attrs(normalized_attrs)}>{''.join(self._anchor['html'])}</a>"

    def _format_starttag(self, tag: str, attrs: list[tuple[str, str | None]], *, startend: bool = False) -> str:
        suffix = " /" if startend else ""
        return f"<{tag}{_format_attrs(attrs)}{suffix}>"


def _attr_value(attrs: list[tuple[str, str | None]], name: str) -> str | None:
    for attr_name, attr_value in attrs:
        if attr_name == name:
            return attr_value or ""
    return None


def _format_attrs(attrs: list[tuple[str, str | None]]) -> str:
    formatted = []
    for name, value in attrs:
        if value is None:
            formatted.append(f" {name}")
            continue
        formatted.append(f' {name}="{html.escape(value, quote=True)}"')
    return "".join(formatted)


def _replacement_href(doc: UiDoc, href: str) -> str | None:
    split = urlsplit(href)
    if split.scheme or split.netloc:
        if split.scheme in {"http", "https"}:
            return href
        return None
    target, fragment = urldefrag(href)
    normalized_fragment = normalize_fragment(fragment)
    if target == "":
        if not normalized_fragment:
            return None
        return f"#{normalized_fragment}"
    normalized_path = _normalize_doc_target(doc.path, target)
    target_doc_id = _UI_DOC_IDS_BY_PATH.get(normalized_path)
    if target_doc_id is None:
        return None
    final_href = f"/ui/docs/{target_doc_id}"
    if normalized_fragment:
        final_href = f"{final_href}#{normalized_fragment}"
    return final_href


def _normalize_doc_target(source_path: str, target: str) -> str:
    decoded = unquote(target)
    base = posixpath.dirname(source_path)
    joined = decoded if base == "" else posixpath.join(base, decoded)
    normalized = posixpath.normpath(joined)
    return "" if normalized == "." else normalized
