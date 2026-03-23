"""Extract metadata from files."""

import hashlib
import logging
import mimetypes
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Optional

from lsiee.config import config
from lsiee.security import PathSecurityError, ensure_safe_file

logger = logging.getLogger(__name__)


class FileMetadata:
    """File metadata container."""

    def __init__(
        self,
        path: Path,
        filename: str,
        extension: str,
        mime_type: Optional[str],
        size_bytes: int,
        created_at: datetime,
        modified_at: datetime,
        accessed_at: datetime,
        content_hash: Optional[str] = None,
    ):
        self.path = str(path.absolute())
        self.filename = filename
        self.extension = extension
        self.mime_type = mime_type
        self.size_bytes = size_bytes
        self.created_at = created_at
        self.modified_at = modified_at
        self.accessed_at = accessed_at
        self.content_hash = content_hash

    def __repr__(self):
        return f"FileMetadata('{self.filename}', {self.size_bytes}B)"


def extract_metadata(filepath: Path, calculate_hash: bool = False) -> Optional[FileMetadata]:
    """Extract metadata from a file.

    Args:
        filepath: Path to file
        calculate_hash: Calculate SHA256 hash (slow for large files)

    Returns:
        FileMetadata or None if inaccessible
    """
    try:
        safe_path = ensure_safe_file(filepath)
        stats = safe_path.lstat()
        mime_type, _ = mimetypes.guess_type(str(safe_path))

        # Calculate hash only for small files
        content_hash = None
        max_hash_bytes = int(config.get("security.max_index_file_size_mb", 50) * 1024 * 1024)
        if calculate_hash and stats.st_size <= max_hash_bytes:
            content_hash = calculate_file_hash(safe_path)

        return FileMetadata(
            path=safe_path,
            filename=safe_path.name,
            extension=safe_path.suffix.lstrip(".").lower() if safe_path.suffix else "",
            mime_type=mime_type,
            size_bytes=stats.st_size,
            created_at=datetime.fromtimestamp(stats.st_ctime),
            modified_at=datetime.fromtimestamp(stats.st_mtime),
            accessed_at=datetime.fromtimestamp(stats.st_atime),
            content_hash=content_hash,
        )

    except (PathSecurityError, PermissionError, FileNotFoundError, OSError) as exc:
        logger.warning("Could not access file metadata for %s: %s", filepath.name, exc)
        return None


def calculate_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash."""
    sha256_hash = hashlib.sha256()
    safe_path = ensure_safe_file(filepath)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    fd = os.open(safe_path, flags)
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise PathSecurityError("Only regular files are supported")

        with os.fdopen(fd, "rb") as handle:
            fd = None
            for byte_block in iter(lambda: handle.read(4096), b""):
                sha256_hash.update(byte_block)
    finally:
        if fd is not None:
            os.close(fd)

    return sha256_hash.hexdigest()
