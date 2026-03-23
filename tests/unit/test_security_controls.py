"""Tests for Part 3 security and privacy controls."""

from __future__ import annotations

import json
import sqlite3
import time

from lsiee.config import config
from lsiee.security import (
    cleanup_lsiee_data,
    export_lsiee_data,
    sanitize_terminal_text,
)
from lsiee.storage.schemas import initialize_database


def test_sanitize_terminal_text_strips_escape_sequences():
    """Terminal output should not preserve control sequences."""
    text = sanitize_terminal_text("\x1b]0;HACKED\x07\x1b[2Jreport.txt")

    assert "\x1b" not in text
    assert "report.txt" in text


def test_cleanup_lsiee_data_deletes_old_rows(tmp_path):
    """Cleanup helpers should remove old events and snapshots without touching newer rows."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    now = time.time()
    old_timestamp = now - (120 * 86400)
    recent_timestamp = now - 60

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO process_snapshots
            (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
             memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
             create_time, parent_pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                old_timestamp,
                101,
                "old-process",
                None,
                None,
                10.0,
                50.0,
                1.0,
                1,
                1,
                "running",
                2,
                old_timestamp - 10,
                1,
            ),
        )
        conn.execute(
            """
            INSERT INTO process_snapshots
            (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
             memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
             create_time, parent_pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recent_timestamp,
                202,
                "new-process",
                None,
                None,
                20.0,
                60.0,
                1.5,
                1,
                1,
                "running",
                3,
                recent_timestamp - 10,
                1,
            ),
        )
        conn.execute(
            """
            INSERT INTO events
            (timestamp, event_type, source, data, severity, tags, created_at, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                old_timestamp,
                "old_event",
                "tests",
                "{}",
                "INFO",
                "[]",
                old_timestamp,
                "checksum",
            ),
        )
        conn.execute(
            """
            INSERT INTO events
            (timestamp, event_type, source, data, severity, tags, created_at, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recent_timestamp,
                "recent_event",
                "tests",
                "{}",
                "INFO",
                "[]",
                recent_timestamp,
                "checksum",
            ),
        )
        conn.commit()

    preview = cleanup_lsiee_data(
        db_path=db_path,
        data_type="all",
        older_than_days=30,
        dry_run=True,
    )
    assert sum(plan["matched_rows"] for plan in preview["plans"]) == 2

    result = cleanup_lsiee_data(
        db_path=db_path,
        data_type="all",
        older_than_days=30,
        dry_run=False,
    )

    assert result["deleted_rows"] == 2

    with sqlite3.connect(db_path) as conn:
        remaining_snapshots = conn.execute("SELECT COUNT(*) FROM process_snapshots").fetchone()[0]
        remaining_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert remaining_snapshots == 1
    assert remaining_events == 1


def test_export_lsiee_data_writes_json_payload(tmp_path):
    """Full local-data export should emit a structured JSON document."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config._config = config._default_config()
    config_file = config_dir / "config.yaml"
    config_file.write_text("search:\n  max_results: 10\n", encoding="utf-8")

    schema = initialize_database(db_path)
    schema.disconnect()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO files
            (path, filename, extension, size_bytes, modified_at, content_hash, index_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("/tmp/example.txt", "example.txt", "txt", 12, time.time(), "hash", "indexed"),
        )
        conn.commit()

    output_path = tmp_path / "lsiee-export.json"
    summary = export_lsiee_data(
        db_path=db_path,
        vector_db_path=vector_db_path,
        config_file=config_file,
        output_path=output_path,
        format="json",
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert summary["counts"]["files"] == 1
    assert payload["counts"]["files"] == 1
    assert payload["tables"]["files"][0]["filename"] == "example.txt"
