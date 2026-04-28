"""Setuptools command customizations for CogniRelay packaging."""

from __future__ import annotations

import shutil
from pathlib import PurePosixPath

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

AGENT_ASSET_FILES = (
    "README.md",
    "hooks/README.md",
    "hooks/cognirelay_retrieval_hook.py",
    "hooks/cognirelay_continuity_save_hook.py",
    "skills/cognirelay-continuity-authoring/SKILL.md",
)


def _is_egg_info_path(path: str) -> bool:
    """Return whether a source distribution path points inside egg-info metadata."""
    return any(part.endswith(".egg-info") for part in PurePosixPath(path.replace("\\", "/")).parts)


class build_py(_build_py):
    """Copy the allowlisted agent assets into wheel build output only."""

    def run(self) -> None:
        super().run()
        source_root = PurePosixPath("agent-assets")
        target_root = PurePosixPath("cognirelay") / "agent_assets"
        for relative in AGENT_ASSET_FILES:
            source = source_root / relative
            target = PurePosixPath(self.build_lib) / target_root / relative
            self.mkpath(str(target.parent))
            shutil.copy2(str(source), str(target))


class sdist(_sdist):
    """Build an sdist without setuptools' generated egg-info directory."""

    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        super().make_release_tree(base_dir, [path for path in files if not _is_egg_info_path(path)])


setup(cmdclass={"build_py": build_py, "sdist": sdist})
