"""Tests for #253 read-only documentation browser UI."""

from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

from app.ui.docs import UI_DOCS


def _reload_ui_router():
    """Reload config and UI router so env-controlled settings are current."""
    import app.config as config_module
    import app.ui.docs as ui_docs_module
    import app.ui.router as ui_router_module

    importlib.reload(config_module)
    importlib.reload(ui_docs_module)
    return importlib.reload(ui_router_module)


def _reload_main_module():
    """Reload config, UI modules, and main so UI mounting follows env."""
    import app.config as config_module
    import app.main as main_module
    import app.ui.docs as ui_docs_module
    import app.ui.router as ui_router_module

    importlib.reload(config_module)
    importlib.reload(ui_docs_module)
    importlib.reload(ui_router_module)
    return importlib.reload(main_module)


def _request(path: str) -> Request:
    """Build a localhost UI request."""
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "http_version": "1.1",
        }
    )


def _ui_response(
    repo_root: Path,
    *,
    route_path: str,
    request_path: str,
    endpoint_kwargs: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """Render one UI docs route directly for deterministic assertions."""
    with patch.dict(
        os.environ,
        {
            "COGNIRELAY_REPO_ROOT": str(repo_root),
            "COGNIRELAY_AUTO_INIT_GIT": "true",
            "COGNIRELAY_AUDIT_LOG_ENABLED": "false",
        },
        clear=False,
    ):
        ui_router = _reload_ui_router()
        router = ui_router.build_ui_router(app_version="test-version")
        endpoint = next(route.endpoint for route in router.routes if route.path == route_path)
        response = endpoint(_request(request_path), **(endpoint_kwargs or {}))
        return SimpleNamespace(status_code=response.status_code, text=response.body.decode("utf-8"))


def _write_allowlisted_docs(repo_root: Path, *, readme: str | None = None) -> None:
    """Create every allowlisted doc with deterministic default content."""
    for doc in UI_DOCS:
        path = repo_root / doc.path
        path.parent.mkdir(parents=True, exist_ok=True)
        content = readme if doc.doc_id == "readme" and readme is not None else f"# {doc.title}\n\n## Section\n\nContent for {doc.doc_id}.\n"
        path.write_text(content, encoding="utf-8")


class UiDocsTests(unittest.TestCase):
    def test_docs_index_renders_allowlist_in_exact_order_and_runtime_help(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root)

            response = _ui_response(repo_root, route_path="/ui/docs", request_path="/ui/docs")

        self.assertEqual(response.status_code, 200)
        positions = [response.text.index(f"/ui/docs/{doc.doc_id}") for doc in UI_DOCS]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('href="/v1/help">Runtime help index</a>', response.text)
        self.assertIn('href="/v1/help/onboarding">Runtime onboarding index</a>', response.text)
        self.assertIn('href="/v1/help/limits">Validation limits index</a>', response.text)
        self.assertIn('href="/ui/docs">Docs</a>', response.text)

    def test_each_allowlisted_doc_id_renders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root)

            for doc in UI_DOCS:
                with self.subTest(doc_id=doc.doc_id):
                    response = _ui_response(
                        repo_root,
                        route_path="/ui/docs/{doc_id}",
                        request_path=f"/ui/docs/{doc.doc_id}",
                        endpoint_kwargs={"doc_id": doc.doc_id},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertIn(doc.title, response.text)
                    self.assertIn(doc.path, response.text)
                    self.assertIn("Available", response.text)

    def test_unknown_and_path_like_doc_ids_do_not_render_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root)
            unknown = _ui_response(
                repo_root,
                route_path="/ui/docs/{doc_id}",
                request_path="/ui/docs/not-allowed",
                endpoint_kwargs={"doc_id": "not-allowed"},
            )
            traversal = _ui_response(
                repo_root,
                route_path="/ui/docs/{doc_id}",
                request_path="/ui/docs/..%2FREADME.md",
                endpoint_kwargs={"doc_id": "../README.md"},
            )

            with patch.dict(
                os.environ,
                {
                    "COGNIRELAY_REPO_ROOT": str(repo_root),
                    "COGNIRELAY_AUTO_INIT_GIT": "true",
                    "COGNIRELAY_AUDIT_LOG_ENABLED": "false",
                    "COGNIRELAY_UI_ENABLED": "true",
                },
                clear=False,
            ):
                app_module = _reload_main_module()
                slash_response = TestClient(app_module.app).get("/ui/docs/foo/bar")

        self.assertEqual(unknown.status_code, 404)
        self.assertIn("Documentation Not Found", unknown.text)
        self.assertNotIn("README content", unknown.text)
        self.assertEqual(traversal.status_code, 404)
        self.assertIn("Documentation Not Found", traversal.text)
        self.assertEqual(slash_response.status_code, 404)

    def test_missing_allowlisted_file_degrades_on_index_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root)
            (repo_root / "docs" / "mcp.md").unlink()

            index = _ui_response(repo_root, route_path="/ui/docs", request_path="/ui/docs")
            detail = _ui_response(
                repo_root,
                route_path="/ui/docs/{doc_id}",
                request_path="/ui/docs/mcp",
                endpoint_kwargs={"doc_id": "mcp"},
            )

        self.assertEqual(index.status_code, 200)
        self.assertIn("doc_missing:mcp", index.text)
        self.assertIn("Unavailable", index.text)
        self.assertNotIn('href="/ui/docs/mcp"', index.text)
        self.assertEqual(detail.status_code, 200)
        self.assertIn("MCP Guide", detail.text)
        self.assertIn("doc_missing:mcp", detail.text)
        self.assertIn("Document content is unavailable.", detail.text)

    def test_markdown_sanitization_code_tables_and_links(self) -> None:
        markdown_source = """# README

## Repeat!
### Repeat?
## Repeat!

[System](docs/system-overview.md# Agent Usage )
[Payload](docs/payload-reference.md#field/value)
[Same](# Repeat! )
[EmptySame](#)
[EmptyCross](docs/mcp.md#   )
[Unsupported](docs/reviewer-guide.md)
[RootInject](/ui/docs/agent-onboarding)
[External](https://example.com/a?b=1)
[BadJs](javascript:alert(1))

<script>alert("x")</script>
<a href="https://example.com/raw">raw</a>
<a href="/ui/docs/agent-onboarding">raw internal</a>
<a href="file:///tmp/secret">file</a>
<span style="color:red" onclick="alert(1)">styled</span>
![Alt](docs/image.png)

```html
<script>safe text</script>
```

| A | B |
|---|---|
| 1 | 2 |
"""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root, readme=markdown_source)
            response = _ui_response(
                repo_root,
                route_path="/ui/docs/{doc_id}",
                request_path="/ui/docs/readme",
                endpoint_kwargs={"doc_id": "readme"},
            )

        self.assertEqual(response.status_code, 200)
        content = response.text[response.text.index('<section class="panel docs-content">') :]
        self.assertIn('href="/ui/docs/system-overview#Agent%20Usage"', response.text)
        self.assertIn('href="/ui/docs/payload-reference#field%2Fvalue"', response.text)
        self.assertIn('href="#Repeat%21"', response.text)
        self.assertIn('href="/ui/docs/mcp">EmptyCross</a>', response.text)
        self.assertIn("Unsupported (not available in UI docs allowlist)", response.text)
        self.assertIn("RootInject (not available in UI docs allowlist)", response.text)
        self.assertIn('href="https://example.com/a?b=1" rel="noreferrer"', response.text)
        self.assertIn('href="https://example.com/raw" rel="noreferrer">raw</a>', response.text)
        self.assertIn("raw internal (not available in UI docs allowlist)", response.text)
        self.assertNotIn("javascript:alert", content)
        self.assertNotIn("<script>", content)
        self.assertNotIn("onclick", content)
        self.assertNotIn("style=", content)
        self.assertNotIn("<img", content)
        self.assertNotIn('href="/ui/docs/agent-onboarding">raw internal</a>', content)
        self.assertNotIn('href="/ui/docs/agent-onboarding"', content)
        self.assertNotIn("file:///tmp/secret", content)
        self.assertIn("<pre><code>", response.text)
        self.assertIn("&lt;script&gt;safe text&lt;/script&gt;", response.text)
        self.assertIn("<table>", response.text)
        self.assertIn("<thead>", response.text)
        self.assertIn("<tbody>", response.text)

    def test_heading_anchors_toc_and_read_only_page_shape(self) -> None:
        markdown_source = """# Top

## Same Heading
### Same Heading
#### Not In Toc
## Same Heading
## !!!
"""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _write_allowlisted_docs(repo_root, readme=markdown_source)
            response = _ui_response(
                repo_root,
                route_path="/ui/docs/{doc_id}",
                request_path="/ui/docs/readme",
                endpoint_kwargs={"doc_id": "readme"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn('<h2 id="same-heading">Same Heading</h2>', response.text)
        self.assertIn('<h3 id="same-heading_1">Same Heading</h3>', response.text)
        self.assertIn('<h2 id="same-heading_2">Same Heading</h2>', response.text)
        self.assertIn('<h2 id="section">!!!</h2>', response.text)
        toc_start = response.text.index("<h2>Table of Contents</h2>")
        content_start = response.text.index('<section class="panel docs-content">')
        toc = response.text[toc_start:content_start]
        self.assertIn('href="#same-heading">Same Heading</a>', toc)
        self.assertIn('href="#same-heading_1">Same Heading</a>', toc)
        self.assertIn('href="#same-heading_2">Same Heading</a>', toc)
        self.assertIn('href="#section">!!!</a>', toc)
        self.assertNotIn("not-in-toc", toc)
        self.assertNotIn("<form", response.text)
        self.assertNotIn("<button", response.text)
        self.assertNotIn("data-live-page", response.text)


if __name__ == "__main__":
    unittest.main()
