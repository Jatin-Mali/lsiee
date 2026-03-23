"""Tests for file scanner."""

import os
import shutil
import stat
import tempfile
from pathlib import Path

import pytest

from lsiee.file_intelligence.indexing.metadata_extractor import extract_metadata
from lsiee.file_intelligence.indexing.scanner import DirectoryScanner


@pytest.fixture
def temp_test_dir():
    """Create temporary test directory with files."""
    test_dir = Path(tempfile.mkdtemp())

    # Create test files
    (test_dir / "file1.txt").write_text("Test content 1")
    (test_dir / "file2.py").write_text('print("test")')

    # Create subdirectory
    subdir = test_dir / "subdir"
    subdir.mkdir()
    (subdir / "file3.md").write_text("# Test")

    # Create excluded directory
    gitdir = test_dir / ".git"
    gitdir.mkdir()
    (gitdir / "config").write_text("git config")

    yield test_dir

    # Cleanup
    shutil.rmtree(test_dir)


def test_scanner_finds_files(temp_test_dir):
    """Test scanner finds all non-excluded files."""
    scanner = DirectoryScanner()
    files = list(scanner.scan(temp_test_dir))

    assert len(files) == 3

    filenames = {f.filename for f in files}
    assert "file1.txt" in filenames
    assert "file2.py" in filenames
    assert "file3.md" in filenames


def test_scanner_respects_exclusions(temp_test_dir):
    """Test scanner respects exclusion patterns."""
    scanner = DirectoryScanner(excluded_patterns=["*.py"])
    files = list(scanner.scan(temp_test_dir))

    filenames = {f.filename for f in files}
    assert "file2.py" not in filenames
    assert "file1.txt" in filenames


def test_scanner_statistics(temp_test_dir):
    """Test scanner tracking statistics."""
    scanner = DirectoryScanner()
    list(scanner.scan(temp_test_dir))

    stats = scanner.get_stats()
    assert stats["files_found"] == 3
    assert stats["files_skipped"] >= 1  # .git directory


def test_scanner_tracks_large_files(temp_test_dir):
    """Oversized files should be skipped and counted separately."""
    large_file = temp_test_dir / "large.txt"
    large_file.write_text("x" * (2 * 1024 * 1024), encoding="utf-8")

    scanner = DirectoryScanner(max_file_size_mb=1)
    files = list(scanner.scan(temp_test_dir))
    stats = scanner.get_stats()

    assert "large.txt" not in {file.filename for file in files}
    assert stats["too_large"] >= 1


def test_scanner_tracks_permission_denied_files(temp_test_dir, monkeypatch):
    """Permission-denied files should be skipped without aborting the scan."""
    from lsiee.file_intelligence.indexing import scanner as scanner_module

    original_ensure_safe_file = scanner_module.ensure_safe_file

    def fake_ensure_safe_file(path, *args, **kwargs):
        if Path(path).name == "file1.txt":
            raise PermissionError("permission denied")
        return original_ensure_safe_file(path, *args, **kwargs)

    monkeypatch.setattr(scanner_module, "ensure_safe_file", fake_ensure_safe_file)

    scanner = DirectoryScanner()
    files = list(scanner.scan(temp_test_dir))
    stats = scanner.get_stats()

    assert "file1.txt" not in {file.filename for file in files}
    assert stats["permission_denied"] == 1


def test_metadata_extraction(temp_test_dir):
    """Test metadata extraction."""
    test_file = temp_test_dir / "file1.txt"
    metadata = extract_metadata(test_file)

    assert metadata is not None
    assert metadata.filename == "file1.txt"
    assert metadata.extension == "txt"
    assert metadata.size_bytes > 0


def test_metadata_with_hash(temp_test_dir):
    """Test metadata extraction with hash calculation."""
    test_file = temp_test_dir / "file1.txt"
    metadata = extract_metadata(test_file, calculate_hash=True)

    assert metadata is not None
    assert metadata.content_hash is not None
    assert len(metadata.content_hash) == 64  # SHA256 hex digest


def test_scanner_skips_symlinked_files(temp_test_dir):
    """Symlinked files should be rejected during scanning."""
    target = temp_test_dir / "file1.txt"
    link = temp_test_dir / "linked.txt"
    link.symlink_to(target)

    scanner = DirectoryScanner()
    files = list(scanner.scan(temp_test_dir))

    filenames = {f.filename for f in files}
    assert "linked.txt" not in filenames


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO not supported on this platform")
def test_scanner_skips_special_files(temp_test_dir):
    """Special files such as FIFOs should not be indexed."""
    fifo_path = temp_test_dir / "queue.pipe"
    os.mkfifo(fifo_path)

    scanner = DirectoryScanner()
    files = list(scanner.scan(temp_test_dir))

    filenames = {f.filename for f in files}
    assert "queue.pipe" not in filenames
    assert stat.S_ISFIFO(fifo_path.lstat().st_mode)
