"""Setuptools command customizations for CogniRelay packaging."""

from __future__ import annotations

from pathlib import PurePosixPath

from setuptools import setup
from setuptools.command.sdist import sdist as _sdist


def _is_egg_info_path(path: str) -> bool:
    """Return whether a source distribution path points inside egg-info metadata."""
    return any(part.endswith(".egg-info") for part in PurePosixPath(path.replace("\\", "/")).parts)


class sdist(_sdist):
    """Build an sdist without setuptools' generated egg-info directory."""

    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        super().make_release_tree(base_dir, [path for path in files if not _is_egg_info_path(path)])


setup(cmdclass={"sdist": sdist})
