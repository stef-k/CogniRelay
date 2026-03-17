"""Small git wrapper used for repository-backed persistence."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional


class GitManager:
    """Manage git operations for the configured repository root."""
    def __init__(self, repo_root: Path, author_name: str, author_email: str) -> None:
        """Store repository and author information for later git calls."""
        self.repo_root = repo_root
        self.author_name = author_name
        self.author_email = author_email

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Run a git command inside the repository root."""
        return subprocess.run(
            ["git", *args],
            cwd=self.repo_root,
            check=check,
            text=True,
            capture_output=True,
        )

    def is_repo(self) -> bool:
        """Return whether the repository root already contains a git repository."""
        return (self.repo_root / ".git").exists()

    def init_repo(self) -> None:
        """Initialize the repository and configure the commit author if needed."""
        self.repo_root.mkdir(parents=True, exist_ok=True)
        if not self.is_repo():
            self._run("init")
            self._run("config", "user.name", self.author_name)
            self._run("config", "user.email", self.author_email)

    def ensure_repo(self, auto_init: bool) -> None:
        """Ensure a git repository exists, optionally initializing it."""
        self.repo_root.mkdir(parents=True, exist_ok=True)
        if self.is_repo():
            return
        if auto_init:
            self.init_repo()
        else:
            raise RuntimeError(f"Git repo not initialized at {self.repo_root}")

    def commit_paths(self, paths: list[Path], message: str) -> bool:
        """Commit one or more repository-relative paths if any have staged changes."""
        rels = [str(path.relative_to(self.repo_root)) for path in paths]
        if not rels:
            return False
        self._run("add", *rels)

        status = self._run("status", "--porcelain", "--", *rels)
        if not status.stdout.strip():
            return False

        env = {
            "GIT_AUTHOR_NAME": self.author_name,
            "GIT_AUTHOR_EMAIL": self.author_email,
            "GIT_COMMITTER_NAME": self.author_name,
            "GIT_COMMITTER_EMAIL": self.author_email,
        }
        subprocess.run(
            ["git", "commit", "-m", message, "--", *rels],
            cwd=self.repo_root,
            check=True,
            text=True,
            capture_output=True,
            env={**env, **os.environ},
        )
        return True

    def commit_file(self, path: Path, message: str) -> bool:
        """Commit a single file if it has staged changes."""
        return self.commit_paths([path], message)

    def latest_commit(self) -> Optional[str]:
        """Return the current HEAD commit SHA if the repo is initialized."""
        if not self.is_repo():
            return None
        cp = self._run("rev-parse", "HEAD", check=False)
        if cp.returncode != 0:
            return None
        return cp.stdout.strip() or None
