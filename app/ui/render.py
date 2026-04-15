"""Minimal file-based HTML template helpers for the operator UI."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from string import Template

_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


@lru_cache(maxsize=None)
def _load_template(name: str) -> Template:
    """Load and cache one HTML template file by name."""
    return Template((_TEMPLATE_DIR / name).read_text(encoding="utf-8"))


def render_template(name: str, **context: str) -> str:
    """Render one cached HTML template with the provided string context."""
    normalized = {key: str(value) for key, value in context.items()}
    return _load_template(name).safe_substitute(normalized)
