"""File utilities for LSIEE."""

import hashlib
from pathlib import Path


def calculate_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def get_file_size_mb(filepath: Path) -> float:
    """Get file size in megabytes."""
    return filepath.stat().st_size / (1024 * 1024)


def is_text_file(filepath: Path) -> bool:
    """Check if file is a text file."""
    text_extensions = {".txt", ".md", ".log", ".py", ".js", ".json", ".xml", ".html", ".css"}
    return filepath.suffix.lower() in text_extensions
