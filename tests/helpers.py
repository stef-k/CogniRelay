"""Shared stubs used across the test suite."""

from __future__ import annotations

from pathlib import Path


class AllowAllAuthStub:
    """Auth stub that permits all requested scopes and path checks."""

    def __init__(self, *, peer_id: str = "peer-test", client_ip: str | None = None) -> None:
        """Store the caller identity exposed to route and service code."""
        self.peer_id = peer_id
        self.client_ip = client_ip

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None

    def require_write_path(self, _path: str) -> None:
        """Accept any requested write path for test purposes."""
        return None


class SimpleGitManagerStub:
    """Git manager stub that reports successful commits and a stable SHA."""

    repo_root = Path("/tmp/stub-repo")

    def __init__(self, repo_root: Path | None = None) -> None:
        """Store an optional repo_root for git_safety unstage compatibility."""
        self.repo_root = repo_root or Path("/tmp/stub-repo")

    def commit_paths(self, _paths: list[Path], _message: str) -> bool:
        """Report a successful multi-path commit without touching git."""
        return True

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful single-file commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"
