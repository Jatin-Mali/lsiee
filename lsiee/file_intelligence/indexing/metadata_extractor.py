"""Extract metadata from files."""

import os
import mimetypes
from pathlib import Path
from datetime import datetime
from typing import Optional
import hashlib


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
        content_hash: Optional[str] = None
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
        stats = filepath.stat()
        mime_type, _ = mimetypes.guess_type(str(filepath))
        
        # Calculate hash only for small files
        content_hash = None
        if calculate_hash and stats.st_size < 50 * 1024 * 1024:  # < 50MB
            content_hash = calculate_file_hash(filepath)
        
        return FileMetadata(
            path=filepath,
            filename=filepath.name,
            extension=filepath.suffix.lstrip('.').lower() if filepath.suffix else '',
            mime_type=mime_type,
            size_bytes=stats.st_size,
            created_at=datetime.fromtimestamp(stats.st_ctime),
            modified_at=datetime.fromtimestamp(stats.st_mtime),
            accessed_at=datetime.fromtimestamp(stats.st_atime),
            content_hash=content_hash
        )
    
    except (PermissionError, FileNotFoundError, OSError) as e:
        print(f"Warning: Could not access {filepath}: {e}")
        return None


def calculate_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash."""
    sha256_hash = hashlib.sha256()
    
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    
    return sha256_hash.hexdigest()