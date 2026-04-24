"""Read-only documentation browser helpers for the operator UI."""

from __future__ import annotations

import html
import posixpath
import re
import secrets
import xml.etree.ElementTree as etree
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urldefrag, urlsplit

import bleach
import markdown
from markdown.extensions import Extension
from markdown.extensions.toc import TocExtension
from markdown.treeprocessors import Treeprocessor


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
    token = secrets.token_urlsafe(16)
    link_extension = _DocsLinkExtension(doc=doc, token=token)
    md = markdown.Markdown(
        extensions=[
            "fenced_code",
            "tables",
            TocExtension(slugify=_slugify_heading, toc_depth="2-3"),
            link_extension,
        ],
        output_format="html",
    )
    rendered = md.convert(source)
    cleaned = bleach.Cleaner(
        tags=_ALLOWED_TAGS,
        attributes=_allowed_attribute,
        protocols={"http", "https"},
        strip=True,
        strip_comments=True,
    ).clean(rendered)
    content_html = _restore_generated_doc_links(cleaned, link_extension.generated_hrefs)
    return RenderedDoc(
        content_html=content_html,
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
    return False


def _restore_generated_doc_links(content: str, generated_hrefs: dict[str, str]) -> str:
    restored = content
    for placeholder, href in generated_hrefs.items():
        restored = restored.replace(html.escape(placeholder, quote=True), html.escape(href, quote=True))
        restored = restored.replace(placeholder, html.escape(href, quote=True))
    return restored


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


class _DocsLinkExtension(Extension):
    """Python-Markdown extension that rewrites links before final sanitization."""

    def __init__(self, *, doc: UiDoc, token: str) -> None:
        super().__init__()
        self.doc = doc
        self.token = token
        self.generated_hrefs: dict[str, str] = {}

    def extendMarkdown(self, md: markdown.Markdown) -> None:  # noqa: N802 - Markdown extension API
        md.treeprocessors.register(_DocsLinkTreeprocessor(md, extension=self), "cognirelay_docs_links", 5)


class _DocsLinkTreeprocessor(Treeprocessor):
    """Rewrite Markdown-generated anchors into the docs browser allowlist."""

    def __init__(self, md: markdown.Markdown, *, extension: _DocsLinkExtension) -> None:
        super().__init__(md)
        self.extension = extension

    def run(self, root: etree.Element) -> etree.Element:
        for parent in list(root.iter()):
            for index, child in enumerate(list(parent)):
                if child.tag != "a":
                    continue
                self._rewrite_link(parent, index, child)
        return root

    def _rewrite_link(self, parent: etree.Element, index: int, link: etree.Element) -> None:
        href = str(link.attrib.get("href", ""))
        replacement = self._replacement_href(href)
        if replacement is None:
            _degrade_link(parent, index, link)
            return
        link.attrib["href"] = replacement
        if replacement.startswith("http://") or replacement.startswith("https://"):
            if not replacement.startswith(self._placeholder_prefix):
                link.attrib["rel"] = "noreferrer"

    @property
    def _placeholder_prefix(self) -> str:
        return f"https://cognirelay.invalid/__ui_docs__/{self.extension.token}/"

    def _replacement_href(self, href: str) -> str | None:
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
            return self._placeholder(f"fragment/{normalized_fragment}", f"#{normalized_fragment}")
        normalized_path = _normalize_doc_target(self.extension.doc.path, target)
        target_doc_id = _UI_DOC_IDS_BY_PATH.get(normalized_path)
        if target_doc_id is None:
            return None
        final_href = f"/ui/docs/{target_doc_id}"
        placeholder_suffix = f"doc/{target_doc_id}"
        if normalized_fragment:
            final_href = f"{final_href}#{normalized_fragment}"
            placeholder_suffix = f"{placeholder_suffix}/{normalized_fragment}"
        return self._placeholder(placeholder_suffix, final_href)

    def _placeholder(self, suffix: str, final_href: str) -> str:
        placeholder = self._placeholder_prefix + suffix
        self.extension.generated_hrefs[placeholder] = final_href
        return placeholder


def _normalize_doc_target(source_path: str, target: str) -> str:
    decoded = unquote(target)
    base = posixpath.dirname(source_path)
    joined = decoded if base == "" else posixpath.join(base, decoded)
    normalized = posixpath.normpath(joined)
    return "" if normalized == "." else normalized


def _degrade_link(parent: etree.Element, index: int, link: etree.Element) -> None:
    span = etree.Element("span")
    span.text = "".join(link.itertext()) + " (not available in UI docs allowlist)"
    span.tail = link.tail
    parent[index] = span
