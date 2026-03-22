"""Tests for indexing refresh behavior."""

from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.storage.metadata_db import MetadataDB
from lsiee.storage.schemas import initialize_database


def test_indexer_skips_unchanged_files_and_refreshes_modified_files(tmp_path):
    """Re-indexing should skip unchanged files and refresh changed ones."""
    db_path = tmp_path / "lsiee.db"
    initialize_database(db_path).disconnect()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    sample_file = corpus_dir / "notes.txt"
    sample_file.write_text("initial content", encoding="utf-8")

    indexer = Indexer(db_path=db_path)

    first_run = indexer.index_directory(corpus_dir, show_progress=False)
    assert first_run["files_indexed"] == 1
    assert first_run["files_updated"] == 0
    assert first_run["files_unchanged"] == 0

    second_run = indexer.index_directory(corpus_dir, show_progress=False)
    assert second_run["files_indexed"] == 0
    assert second_run["files_updated"] == 0
    assert second_run["files_unchanged"] == 1

    sample_file.write_text("updated content that changes file size", encoding="utf-8")
    third_run = indexer.index_directory(corpus_dir, show_progress=False)
    assert third_run["files_indexed"] == 0
    assert third_run["files_updated"] == 1
    assert third_run["files_unchanged"] == 0

    with MetadataDB(db_path) as db:
        record = db.get_file_by_path(str(sample_file))

    assert record is not None
    assert record.index_status == "pending"
    assert record.size_bytes == sample_file.stat().st_size
    assert abs(record.modified_at.timestamp() - sample_file.stat().st_mtime) < 1.0


def test_indexer_force_marks_existing_files_for_reindex(tmp_path):
    """Force mode should re-queue existing files even when unchanged."""
    db_path = tmp_path / "lsiee.db"
    initialize_database(db_path).disconnect()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    sample_file = corpus_dir / "script.py"
    sample_file.write_text("print('hello')\n", encoding="utf-8")

    indexer = Indexer(db_path=db_path)
    indexer.index_directory(corpus_dir, show_progress=False)
    forced = indexer.index_directory(corpus_dir, show_progress=False, force=True)

    assert forced["files_updated"] == 1
    assert forced["files_unchanged"] == 0
