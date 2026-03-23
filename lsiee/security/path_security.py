"""Filesystem security helpers for LSIEE."""

from __future__ import annotations

import os
import stat
import tempfile
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional


class PathSecurityError(ValueError):
    """Raised when a filesystem path fails security validation."""


_BLOCKED_ROOTS = tuple(
    Path(path)
    for path in (
        "/etc",
        "/sys",
        "/proc",
        "/dev",
        "/root",
        "/boot",
        "/System",
        "/Library",
        "/private",
    )
)


def _default_allowed_roots() -> list[Path]:
    roots = {Path.home().resolve(), Path(tempfile.gettempdir()).resolve(), Path.cwd().resolve()}
    return sorted(roots)


def _normalize_path_text(path: Path | str) -> Path:
    raw = str(path)
    normalized = unicodedata.normalize("NFKC", raw)
    if "\x00" in normalized:
        raise PathSecurityError("Path contains null bytes")
    if len(normalized) > 4096:
        raise PathSecurityError("Path exceeds the supported length")
    if any(ord(char) < 32 for char in normalized):
        raise PathSecurityError("Path contains control characters")
    return Path(normalized)


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validate_root_membership(resolved: Path, allowed_roots: Optional[list[Path]]) -> None:
    roots = [root.resolve() for root in (allowed_roots or _default_allowed_roots())]
    if not any(_is_relative_to(resolved, root) for root in roots):
        raise PathSecurityError("Path is outside the allowed roots")

    for blocked_root in _BLOCKED_ROOTS:
        if blocked_root.exists() and (
            resolved == blocked_root.resolve() or _is_relative_to(resolved, blocked_root.resolve())
        ):
            raise PathSecurityError("Path is inside a blocked system directory")


def _validate_depth(resolved: Path) -> None:
    parts = [part for part in resolved.parts if part not in (resolved.anchor, "")]
    if len(parts) > 20:
        raise PathSecurityError("Path depth exceeds the supported limit")


def display_path(path: Path | str) -> str:
    """Return a user-facing, redacted path."""
    normalized = _normalize_path_text(path)
    try:
        resolved = normalized.resolve(strict=False)
    except OSError:
        resolved = normalized

    home = Path.home().resolve()
    cwd = Path.cwd().resolve()

    if _is_relative_to(resolved, cwd):
        return str(resolved.relative_to(cwd))
    if _is_relative_to(resolved, home):
        return f"~/{resolved.relative_to(home)}"
    return resolved.name or str(resolved)


def ensure_safe_directory(
    path: Path | str,
    *,
    allowed_roots: Optional[list[Path]] = None,
    must_exist: bool = True,
) -> Path:
    """Validate a directory path before traversal."""
    candidate = _normalize_path_text(path)

    try:
        resolved = candidate.resolve(strict=must_exist)
    except OSError as exc:
        raise PathSecurityError("Directory access denied") from exc

    _validate_root_membership(resolved, allowed_roots)
    _validate_depth(resolved)

    if must_exist:
        try:
            st = candidate.lstat()
        except OSError as exc:
            raise PathSecurityError("Directory access denied") from exc
        if stat.S_ISLNK(st.st_mode):
            raise PathSecurityError("Directory symlinks are not allowed")
        if not stat.S_ISDIR(st.st_mode):
            raise PathSecurityError("Path is not a directory")
        if not os.access(resolved, os.R_OK | os.X_OK):
            raise PathSecurityError("Directory access denied")

    return resolved


def ensure_safe_file(
    path: Path | str,
    *,
    allowed_roots: Optional[list[Path]] = None,
    max_size_bytes: Optional[int] = None,
) -> Path:
    """Validate a regular file path before reading."""
    candidate = _normalize_path_text(path)

    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise PathSecurityError("File access denied") from exc

    _validate_root_membership(resolved, allowed_roots)
    _validate_depth(resolved)

    try:
        st = candidate.lstat()
    except OSError as exc:
        raise PathSecurityError("File access denied") from exc

    if stat.S_ISLNK(st.st_mode):
        raise PathSecurityError("Symlinks are not allowed")
    if not stat.S_ISREG(st.st_mode):
        raise PathSecurityError("Only regular files are supported")
    if max_size_bytes is not None and st.st_size > max_size_bytes:
        raise PathSecurityError("File exceeds the configured size limit")
    if not os.access(resolved, os.R_OK):
        raise PathSecurityError("File access denied")

    return resolved


def ensure_safe_output_path(
    path: Path | str,
    *,
    allowed_roots: Optional[list[Path]] = None,
) -> Path:
    """Validate a destination path before writing."""
    candidate = _normalize_path_text(path)
    parent = candidate.parent if candidate.parent != Path("") else Path(".")
    parent_resolved = ensure_safe_directory(
        parent,
        allowed_roots=allowed_roots,
        must_exist=parent.exists(),
    )
    resolved = parent_resolved / candidate.name

    if candidate.exists():
        try:
            st = candidate.lstat()
        except OSError as exc:
            raise PathSecurityError("Output path is not writable") from exc
        if stat.S_ISLNK(st.st_mode):
            raise PathSecurityError("Refusing to write through a symlink")
        if not stat.S_ISREG(st.st_mode):
            raise PathSecurityError("Output path must be a regular file")

    return resolved


@contextmanager
def _secure_fd(path: Path, *, mode: str) -> Iterator[object]:
    flags = os.O_RDONLY if "r" in mode else os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    fd = None
    try:
        fd = os.open(path, flags)
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise PathSecurityError("Only regular files are supported")

        open_kwargs = {}
        if "b" not in mode:
            open_kwargs["encoding"] = "utf-8"
            open_kwargs["errors"] = "ignore"

        with os.fdopen(fd, mode, **open_kwargs) as handle:
            fd = None
            yield handle
    except OSError as exc:
        raise PathSecurityError("File access denied") from exc
    finally:
        if fd is not None:
            os.close(fd)


def read_secure_text(
    path: Path | str,
    *,
    max_bytes: int,
    allowed_roots: Optional[list[Path]] = None,
) -> str:
    """Read text from a validated file without following symlinks."""
    safe_path = ensure_safe_file(path, allowed_roots=allowed_roots, max_size_bytes=max_bytes)
    with _secure_fd(safe_path, mode="r") as handle:
        return handle.read(max_bytes)


def read_secure_bytes(
    path: Path | str,
    *,
    max_bytes: int,
    allowed_roots: Optional[list[Path]] = None,
) -> bytes:
    """Read bytes from a validated file without following symlinks."""
    safe_path = ensure_safe_file(path, allowed_roots=allowed_roots, max_size_bytes=max_bytes)
    with _secure_fd(safe_path, mode="rb") as handle:
        return handle.read(max_bytes)


def atomic_write_text(path: Path | str, content: str, *, encoding: str = "utf-8") -> Path:
    """Write a text file atomically with restrictive permissions."""
    safe_path = ensure_safe_output_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{safe_path.name}.", suffix=".tmp", dir=safe_path.parent
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, safe_path)
        os.chmod(safe_path, 0o600)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
    return safe_path


def atomic_write_bytes(path: Path | str, content: bytes) -> Path:
    """Write a binary file atomically with restrictive permissions."""
    safe_path = ensure_safe_output_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{safe_path.name}.", suffix=".tmp", dir=safe_path.parent
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, safe_path)
        os.chmod(safe_path, 0o600)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
    return safe_path
