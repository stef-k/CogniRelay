"""Authentication and request authorization helpers."""

from __future__ import annotations

import ipaddress
import logging
import posixpath
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Set

from fastapi import Depends, Header, HTTPException, Request, status

from .config import get_settings, sha256_token
from .timestamps import parse_iso as _parse_iso

_log = logging.getLogger(__name__)


@dataclass
class AuthContext:
    """Authenticated caller context used by route and service authorization checks."""
    token: str
    peer_id: str
    scopes: Set[str]
    read_namespaces: Set[str]
    write_namespaces: Set[str]
    client_ip: str | None = None
    bypass_events: list[dict[str, str]] = field(default_factory=list, repr=False)

    def require(self, scope: str) -> None:
        """Require a scope or raise an HTTP 403 error."""
        if scope not in self.scopes and "admin:peers" not in self.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing scope: {scope}",
            )
        if scope not in self.scopes and "admin:peers" in self.scopes:
            self.bypass_events.append({"kind": "scope", "required": scope})

    def _require_path_mode(self, relative_path: str, mode: str) -> None:
        """Require read or write access for a repository-relative path.

        Access is granted when the normalized path exactly matches an allowed
        namespace or falls under one as a sub-directory (prefix + '/' boundary).
        Paths are normalized via posixpath.normpath to collapse '..' and '//'
        before comparison, preventing traversal-based namespace escapes.
        """
        normalized = posixpath.normpath(relative_path) if relative_path else ""
        if not normalized or normalized == "." or normalized.startswith("/") or normalized.startswith(".."):
            _log.warning("Namespace %s denied (invalid path): peer=%s path=%r", mode, self.peer_id, relative_path)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Invalid path for namespace check: {relative_path}",
            )
        allowed = self.write_namespaces if mode == "write" else self.read_namespaces
        if "*" in allowed or "admin:peers" in self.scopes:
            if "admin:peers" in self.scopes and "*" not in allowed:
                self.bypass_events.append({"kind": "namespace", "mode": mode, "path": normalized})
            return
        for ns in allowed:
            if normalized == ns or normalized.startswith(ns + "/"):
                return
        _log.warning("Namespace %s denied: peer=%s path=%s allowed=%s", mode, self.peer_id, normalized, allowed)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"{mode.title()} path namespace not allowed: {normalized}",
        )

    def require_path(self, relative_path: str) -> None:
        """Require write access for a repository-relative path."""
        self._require_path_mode(relative_path, "write")

    def require_read_path(self, relative_path: str) -> None:
        """Require read access for a repository-relative path."""
        self._require_path_mode(relative_path, "read")

    def require_write_path(self, relative_path: str) -> None:
        """Require write access for a repository-relative path."""
        self._require_path_mode(relative_path, "write")


def _extract_bearer_token(authorization: str | None) -> str:
    """Extract the bearer token string from an Authorization header."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    return parts[1].strip()


def _normalize_ip(value: str | None) -> str | None:
    """Normalize a candidate IP or hostname string for comparison."""
    if not value:
        return None
    out = str(value).strip()
    if not out:
        return None
    if out.startswith("[") and out.endswith("]"):
        out = out[1:-1]
    return out


def _is_loopback_host(value: str | None) -> bool:
    """Return whether the provided host value resolves to loopback."""
    ip = _normalize_ip(value)
    if not ip:
        return False
    if ip.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(ip).is_loopback
    except ValueError:
        return False


def _extract_client_ip(x_forwarded_for: str | None, x_real_ip: str | None, request_client_host: str | None) -> str | None:
    """Choose the safest client IP from transport and proxy headers."""
    forwarded = None
    if x_forwarded_for:
        forwarded = _normalize_ip(str(x_forwarded_for).split(",", 1)[0])
    real = _normalize_ip(x_real_ip)
    request_ip = _normalize_ip(request_client_host)

    # Trust the transport source first. If the transport source is loopback,
    # prefer non-loopback forwarded value to support local reverse-proxy setups.
    if request_ip:
        if _is_loopback_host(request_ip):
            for candidate in (forwarded, real):
                if candidate and not _is_loopback_host(candidate):
                    return candidate
        return request_ip

    for candidate in (forwarded, real):
        if candidate:
            return candidate
    return None


def require_auth(
    authorization: str | None = Header(default=None),
    x_forwarded_for: str | None = Header(default=None, alias="X-Forwarded-For"),
    x_real_ip: str | None = Header(default=None, alias="X-Real-IP"),
    request: Request = None,  # type: ignore[assignment]
) -> AuthContext:
    """Resolve and validate the current caller from the configured token store."""
    # Force reload so token issue/revoke endpoints take effect immediately.
    settings = get_settings(force_reload=True)
    if authorization is not None and not isinstance(authorization, str):
        authorization = None
    if x_forwarded_for is not None and not isinstance(x_forwarded_for, str):
        x_forwarded_for = None
    if x_real_ip is not None and not isinstance(x_real_ip, str):
        x_real_ip = None
    token = _extract_bearer_token(authorization)

    peer = settings.tokens.get(token)
    if peer is None:
        peer = settings.tokens.get(f"sha256:{sha256_token(token)}")
    if peer is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    if str(peer.status or "active") != "active":
        raise HTTPException(status_code=401, detail="Token revoked")

    expires_at = _parse_iso(peer.expires_at)
    if expires_at is not None and datetime.now(timezone.utc) > expires_at:
        raise HTTPException(status_code=401, detail="Token expired")

    return AuthContext(
        token=token,
        peer_id=peer.peer_id,
        scopes=peer.scopes,
        read_namespaces=peer.read_namespaces,
        write_namespaces=peer.write_namespaces,
        client_ip=_extract_client_ip(
            x_forwarded_for,
            x_real_ip,
            request.client.host if request is not None and request.client is not None else None,
        ),
    )


AuthDep = Depends(require_auth)
