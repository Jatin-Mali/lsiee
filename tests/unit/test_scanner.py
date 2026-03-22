"""Tests for file scanner."""

import shutil
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
