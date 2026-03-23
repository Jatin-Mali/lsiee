"""Directory scanner for file discovery."""

import fnmatch
import logging
import os
import stat
from pathlib import Path
from typing import Iterator, List, Optional

from lsiee.file_intelligence.indexing.metadata_extractor import FileMetadata, extract_metadata
from lsiee.security import PathSecurityError, ensure_safe_directory, ensure_safe_file

logger = logging.getLogger(__name__)


class DirectoryScanner:
    """Scans directories and discovers files."""

    def __init__(
        self,
        excluded_patterns: Optional[List[str]] = None,
        excluded_directories: Optional[List[str]] = None,
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
        self.excluded_directories = [
            Path(path).expanduser().resolve() for path in (excluded_directories or [])
        ]
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.follow_symlinks = follow_symlinks

        # Statistics
        self.files_found = 0
        self.files_skipped = 0
        self.errors = 0
        self.permission_denied = 0
        self.too_large = 0
        self.unsafe_paths = 0

    def scan(self, directory: Path) -> Iterator[FileMetadata]:
        """Scan directory and yield file metadata.

        Args:
            directory: Directory to scan

        Yields:
            FileMetadata objects
        """
        try:
            safe_directory = ensure_safe_directory(directory)
        except PathSecurityError as exc:
            raise ValueError("Directory access denied") from exc

        logger.info("Scanning: %s", safe_directory)

        for root, dirs, files in os.walk(safe_directory, followlinks=self.follow_symlinks):
            root_path = Path(root)

            # Filter excluded directories IN PLACE
            filtered_dirs = []
            for dirname in dirs:
                dirpath = root_path / dirname
                if self._should_exclude(dirname):
                    self.files_skipped += 1
                    continue
                try:
                    dir_stat = dirpath.lstat()
                    if stat.S_ISLNK(dir_stat.st_mode):
                        self.files_skipped += 1
                        logger.warning("Skipped symlinked directory: %s", dirpath.name)
                        continue
                    ensure_safe_directory(dirpath)
                    if self._is_excluded_directory(dirpath):
                        self.files_skipped += 1
                        continue
                except (OSError, PathSecurityError) as exc:
                    self.files_skipped += 1
                    logger.warning("Skipped directory %s: %s", dirpath.name, exc)
                    continue
                filtered_dirs.append(dirname)
            dirs[:] = filtered_dirs

            for filename in files:
                filepath = root_path / filename

                if self._should_exclude(filename):
                    self.files_skipped += 1
                    continue

                try:
                    if self._is_excluded_directory(filepath):
                        self.files_skipped += 1
                        continue
                    file_stat = filepath.lstat()
                    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
                        self.files_skipped += 1
                        logger.warning("Skipped non-regular file: %s", filepath.name)
                        continue

                    size = file_stat.st_size
                    if size > self.max_file_size_bytes:
                        self.files_skipped += 1
                        self.too_large += 1
                        logger.debug("Skipped (too large): %s", filepath.name)
                        continue
                    safe_file = ensure_safe_file(
                        filepath,
                        max_size_bytes=self.max_file_size_bytes,
                    )
                except PermissionError as exc:
                    self.files_skipped += 1
                    self.permission_denied += 1
                    logger.warning("Permission denied for %s: %s", filepath.name, exc)
                    continue
                except PathSecurityError as exc:
                    self.files_skipped += 1
                    self.unsafe_paths += 1
                    logger.warning("Skipped unsafe file %s: %s", filepath.name, exc)
                    continue
                except OSError as exc:
                    self.errors += 1
                    logger.warning("Could not stat %s: %s", filepath.name, exc)
                    continue

                metadata = extract_metadata(safe_file, calculate_hash=False)

                if metadata:
                    self.files_found += 1
                    yield metadata
                else:
                    self.files_skipped += 1

        logger.info(
            "Scan complete. Found: %s, Skipped: %s",
            self.files_found,
            self.files_skipped,
        )

    def _should_exclude(self, name: str) -> bool:
        """Check if file/directory should be excluded."""
        for pattern in self.excluded_patterns:
            if fnmatch.fnmatch(name, pattern):
                return True
        return False

    def _is_excluded_directory(self, path: Path) -> bool:
        """Check if a path is inside an excluded directory root."""
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            resolved = path
        for excluded in self.excluded_directories:
            try:
                resolved.relative_to(excluded)
                return True
            except ValueError:
                continue
        return False

    def get_stats(self) -> dict:
        """Get scanning statistics."""
        return {
            "files_found": self.files_found,
            "files_skipped": self.files_skipped,
            "errors": self.errors,
            "permission_denied": self.permission_denied,
            "too_large": self.too_large,
            "unsafe_paths": self.unsafe_paths,
        }

    def reset_stats(self):
        """Reset statistics."""
        self.files_found = 0
        self.files_skipped = 0
        self.errors = 0
        self.permission_denied = 0
        self.too_large = 0
        self.unsafe_paths = 0
