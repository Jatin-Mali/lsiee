"""Tests for database operations."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from lsiee.storage.metadata_db import FileRecord, MetadataDB
from lsiee.storage.schemas import initialize_database


@pytest.fixture
def temp_db():
    """Create temporary database."""
    db_path = Path(tempfile.mktemp(suffix=".db"))
    initialize_database(db_path)
    yield db_path
    if db_path.exists():
        db_path.unlink()


def test_insert_and_retrieve(temp_db):
    """Test inserting and retrieving a file record."""
    with MetadataDB(temp_db) as db:
        record = FileRecord(
            id=None,
            path="/test/file.txt",
            filename="file.txt",
            extension="txt",
            size_bytes=1024,
            modified_at=datetime.now(),
        )

        file_id = db.insert_file(record)
        assert file_id > 0

        retrieved = db.get_file_by_path("/test/file.txt")
        assert retrieved is not None
        assert retrieved.filename == "file.txt"
        assert retrieved.size_bytes == 1024


def test_get_all_files(temp_db):
    """Test getting all files."""
    with MetadataDB(temp_db) as db:
        # Insert multiple files
        for i in range(3):
            record = FileRecord(
                id=None,
                path=f"/test/file{i}.txt",
                filename=f"file{i}.txt",
                extension="txt",
                size_bytes=1024,
                modified_at=datetime.now(),
            )
            db.insert_file(record)

        files = db.get_all_files()
        assert len(files) == 3


def test_update_status(temp_db):
    """Test updating file status."""
    with MetadataDB(temp_db) as db:
        record = FileRecord(
            id=None,
            path="/test/file.txt",
            filename="file.txt",
            extension="txt",
            size_bytes=1024,
            modified_at=datetime.now(),
            index_status="pending",
        )

        file_id = db.insert_file(record)
        db.update_file_status(file_id, "indexed")

        retrieved = db.get_file_by_path("/test/file.txt")
        assert retrieved.index_status == "indexed"


def test_get_stats(temp_db):
    """Test getting database statistics."""
    with MetadataDB(temp_db) as db:
        for i in range(5):
            record = FileRecord(
                id=None,
                path=f"/test/file{i}.txt",
                filename=f"file{i}.txt",
                extension="txt",
                size_bytes=1000 + i,
                modified_at=datetime.now(),
            )
            db.insert_file(record)

        stats = db.get_stats()
        assert stats["total_files"] == 5
        assert stats["total_size_bytes"] > 0


def test_files_table_uses_index_status_column(temp_db):
    """The files table should expose the audited index_status column name only."""
    with MetadataDB(temp_db) as db:
        columns = db.get_columns("files")

    assert "index_status" in columns
    assert "status" not in columns
