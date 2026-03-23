"""Tests for runtime verification checks."""

import os
import sqlite3
from datetime import datetime

from lsiee.security.verification import verify_lsiee_runtime
from lsiee.storage.metadata_db import FileRecord, MetadataDB
from lsiee.storage.schemas import initialize_database
from lsiee.storage.vector_db import VectorDB


def test_verify_runtime_passes_for_consistent_local_state(tmp_path):
    """Verification should pass when the DB and search index agree."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    config_file = tmp_path / "config.yaml"
    log_dir = tmp_path / "logs"

    initialize_database(db_path).disconnect()
    config_file.write_text("search:\n  max_results: 10\n", encoding="utf-8")
    os.chmod(config_file, 0o600)
    log_dir.mkdir()
    os.chmod(log_dir, 0o700)

    sample_file = tmp_path / "notes.txt"
    sample_file.write_text("python bug fix notes", encoding="utf-8")

    with MetadataDB(db_path) as db:
        file_id = db.insert_file(
            FileRecord(
                id=None,
                path=str(sample_file),
                filename=sample_file.name,
                extension="txt",
                size_bytes=sample_file.stat().st_size,
                modified_at=datetime.fromtimestamp(sample_file.stat().st_mtime),
                index_status="indexed",
            )
        )
        assert file_id > 0

    VectorDB(vector_db_path).add_embeddings(
        ids=[str(sample_file)],
        embeddings=[[]],
        documents=["notes.txt python bug fix notes"],
        metadatas=[{"filename": sample_file.name, "extension": ".txt"}],
    )

    report = verify_lsiee_runtime(
        db_path=db_path,
        vector_db_path=vector_db_path,
        config_file=config_file,
        log_dir=log_dir,
    )

    assert report["ok"] is True


def test_verify_runtime_detects_indexed_file_without_vector(tmp_path):
    """Verification should fail when indexed files and search vectors drift apart."""
    db_path = tmp_path / "lsiee.db"
    initialize_database(db_path).disconnect()

    sample_file = tmp_path / "notes.txt"
    sample_file.write_text("python bug fix notes", encoding="utf-8")

    with MetadataDB(db_path) as db:
        db.insert_file(
            FileRecord(
                id=None,
                path=str(sample_file),
                filename=sample_file.name,
                extension="txt",
                size_bytes=sample_file.stat().st_size,
                modified_at=datetime.fromtimestamp(sample_file.stat().st_mtime),
                index_status="indexed",
            )
        )

    report = verify_lsiee_runtime(
        db_path=db_path,
        vector_db_path=tmp_path / "vectors",
        config_file=tmp_path / "config.yaml",
        log_dir=tmp_path / "logs",
    )

    assert report["ok"] is False
    assert any(
        check["name"] == "Indexed files match search vectors" and not check["ok"]
        for check in report["checks"]
    )


def test_verify_runtime_detects_tampered_events(tmp_path):
    """Verification should fail when stored event checksums do not match."""
    db_path = tmp_path / "lsiee.db"
    initialize_database(db_path).disconnect()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO events
            (timestamp, event_type, source, data, severity, tags, created_at, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1.0, "tampered", "tests", "{}", "INFO", "[]", 1.0, "bad-checksum"),
        )
        conn.commit()

    report = verify_lsiee_runtime(
        db_path=db_path,
        vector_db_path=tmp_path / "vectors",
        config_file=tmp_path / "config.yaml",
        log_dir=tmp_path / "logs",
    )

    assert report["ok"] is False
    assert any(
        check["name"] == "Event integrity valid" and not check["ok"] for check in report["checks"]
    )
