"""Directory scanner for file discovery."""

import fnmatch
import logging
import os
from pathlib import Path
from typing import Iterator, List, Optional

from lsiee.file_intelligence.indexing.metadata_extractor import FileMetadata, extract_metadata

logger = logging.getLogger(__name__)


class DirectoryScanner:
    """Scans directories and discovers files."""

    def __init__(
        self,
        excluded_patterns: Optional[List[str]] = None,
        max_file_size_mb: int = 50,
        follow_symlinks: bool = False,
    ):
        """Initialize scanner.

        Args:
            excluded_patterns: Patterns to exclude
            max_file_size_mb: Max file size in MB
            follow_symlinks: Follow symbolic links
        """
        self.excluded_patterns = excluded_patterns or [
            "node_modules",
            ".git",
            "__pycache__",
            "*.tmp",
            "*.cache",
            ".DS_Store",
            "venv",
            "env",
        ]
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.follow_symlinks = follow_symlinks

        # Statistics
        self.files_found = 0
        self.files_skipped = 0
        self.errors = 0

    def scan(self, directory: Path) -> Iterator[FileMetadata]:
        """Scan directory and yield file metadata.

        Args:
            directory: Directory to scan

        Yields:
            FileMetadata objects
        """
        if not directory.exists():
            raise ValueError(f"Directory does not exist: {directory}")

        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")

        logger.info(f"Scanning: {directory}")

        for root, dirs, files in os.walk(directory, followlinks=self.follow_symlinks):
            root_path = Path(root)

            # Filter excluded directories IN PLACE
            original_dir_count = len(dirs)
            dirs[:] = [d for d in dirs if not self._should_exclude(d)]
            self.files_skipped += original_dir_count - len(dirs)
            for filename in files:
                filepath = root_path / filename

                if self._should_exclude(filename):
                    self.files_skipped += 1
                    continue

                if not filepath.is_file():
                    continue

                try:
                    size = filepath.stat().st_size
                    if size > self.max_file_size_bytes:
                        self.files_skipped += 1
                        logger.debug(f"Skipped (too large): {filepath}")
                        continue
                except (PermissionError, OSError) as e:
                    self.errors += 1
                    logger.warning(f"Could not stat {filepath}: {e}")
                    continue

                metadata = extract_metadata(filepath, calculate_hash=False)

                if metadata:
                    self.files_found += 1
                    yield metadata
                else:
                    self.errors += 1

        logger.info(f"Scan complete. Found: {self.files_found}, Skipped: {self.files_skipped}")

    def _should_exclude(self, name: str) -> bool:
        """Check if file/directory should be excluded."""
        for pattern in self.excluded_patterns:
            if fnmatch.fnmatch(name, pattern):
                return True
        return False

    def get_stats(self) -> dict:
        """Get scanning statistics."""
        return {
            "files_found": self.files_found,
            "files_skipped": self.files_skipped,
            "errors": self.errors,
        }

    def reset_stats(self):
        """Reset statistics."""
        self.files_found = 0
        self.files_skipped = 0
        self.errors = 0
