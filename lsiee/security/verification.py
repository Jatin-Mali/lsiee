"""Runtime verification helpers for LSIEE local state."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from lsiee.storage.schemas import configure_connection
from lsiee.storage.vector_db import VectorDB
from lsiee.temporal_intelligence.events.event_logger import EventLogger

REQUIRED_TABLES = {
    "files",
    "schemas",
    "process_snapshots",
    "events",
    "correlations",
}
REQUIRED_FILES_COLUMNS = {
    "id",
    "path",
    "filename",
    "modified_at",
    "content_hash",
    "index_status",
}
REQUIRED_FILE_INDEXES = {
    "idx_files_path",
    "idx_files_modified",
    "idx_files_extension",
}


def verify_lsiee_runtime(
    *,
    db_path: Path,
    vector_db_path: Path,
    config_file: Path,
    log_dir: Path,
) -> Dict[str, Any]:
    """Verify local LSIEE state for consistency and secure defaults."""
    checks: List[Dict[str, Any]] = []
    indexed_paths: set[str] = set()

    db_path = Path(db_path)
    vector_db_path = Path(vector_db_path)
    config_file = Path(config_file)
    log_dir = Path(log_dir)

    checks.append(
        _permission_check(
            "Config file permissions",
            config_file,
            expected_mode=0o600,
            missing_ok=True,
        )
    )
    checks.append(
        _permission_check(
            "Log directory permissions",
            log_dir,
            expected_mode=0o700,
            missing_ok=True,
            directory=True,
        )
    )

    if not db_path.exists():
        checks.append(
            {
                "name": "Database present",
                "ok": False,
                "details": "Local metadata database is missing",
            }
        )
        return _finalize_checks(checks)

    checks.append(_permission_check("Database permissions", db_path, expected_mode=0o600))

    with sqlite3.connect(db_path) as conn:
        configure_connection(conn)

        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        checks.append(
            {
                "name": "SQLite integrity check",
                "ok": integrity == "ok",
                "details": str(integrity),
            }
        )

        table_names = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        missing_tables = sorted(REQUIRED_TABLES - table_names)
        checks.append(
            {
                "name": "Required tables present",
                "ok": not missing_tables,
                "details": (
                    ", ".join(missing_tables) if missing_tables else "All required tables exist"
                ),
            }
        )

        if "files" in table_names:
            file_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(files)").fetchall()
            }
            missing_columns = sorted(REQUIRED_FILES_COLUMNS - file_columns)
            legacy_status_present = "status" in file_columns
            checks.append(
                {
                    "name": "Files schema uses index_status",
                    "ok": not missing_columns and not legacy_status_present,
                    "details": (
                        "Legacy status column detected"
                        if legacy_status_present
                        else (
                            ", ".join(missing_columns)
                            if missing_columns
                            else "Files table schema looks correct"
                        )
                    ),
                }
            )

            file_indexes = {
                row["name"] for row in conn.execute("PRAGMA index_list(files)").fetchall()
            }
            missing_indexes = sorted(REQUIRED_FILE_INDEXES - file_indexes)
            checks.append(
                {
                    "name": "Files table indexes present",
                    "ok": not missing_indexes,
                    "details": (
                        ", ".join(missing_indexes)
                        if missing_indexes
                        else "Files table indexes are present"
                    ),
                }
            )

            indexed_paths = {
                row["path"]
                for row in conn.execute(
                    "SELECT path FROM files WHERE index_status = 'indexed'"
                ).fetchall()
            }

        pragma_results = {
            "foreign_keys": int(conn.execute("PRAGMA foreign_keys").fetchone()[0]),
            "busy_timeout": int(conn.execute("PRAGMA busy_timeout").fetchone()[0]),
            "journal_mode": str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
        }
        checks.append(
            {
                "name": "SQLite pragmas enforced",
                "ok": (
                    pragma_results["foreign_keys"] == 1
                    and pragma_results["busy_timeout"] >= 30000
                    and pragma_results["journal_mode"] == "wal"
                ),
                "details": (
                    "foreign_keys={foreign_keys}, busy_timeout={busy_timeout}, "
                    "journal_mode={journal_mode}"
                ).format(**pragma_results),
            }
        )

        if "events" in table_names:
            rows = conn.execute("""
                SELECT id, timestamp, event_type, source, data, related_process_id,
                       related_file_id, severity, tags, created_at, checksum
                FROM events
                """).fetchall()
            invalid_events = 0
            for row in rows:
                parsed = {
                    **dict(row),
                    "data": row["data"],
                    "tags": row["tags"],
                }
                deserialized = EventLogger._deserialize_row(parsed)
                if not deserialized["integrity_valid"]:
                    invalid_events += 1
            checks.append(
                {
                    "name": "Event integrity valid",
                    "ok": invalid_events == 0,
                    "details": f"{invalid_events} invalid event row(s)",
                }
            )

    vector_check = _verify_vector_store(vector_db_path=vector_db_path, indexed_paths=indexed_paths)
    checks.extend(vector_check)
    return _finalize_checks(checks)


def _verify_vector_store(*, vector_db_path: Path, indexed_paths: set[str]) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    if not vector_db_path.exists() and not indexed_paths:
        checks.append(
            {
                "name": "Vector store consistency",
                "ok": True,
                "details": "No vector store present and no indexed search documents expected",
            }
        )
        return checks

    vector_db = VectorDB(vector_db_path)
    diagnostics = vector_db.get_diagnostics()
    vector_ids = set(vector_db.ids)
    missing_vectors = sorted(indexed_paths - vector_ids)
    orphaned_vectors = sorted(vector_ids - indexed_paths)

    checks.append(
        {
            "name": "Vector store structure",
            "ok": diagnostics["is_consistent"],
            "details": (
                f"vectors={diagnostics['vector_count']}, "
                f"documents={diagnostics['document_count']}, "
                f"metadata={diagnostics['metadata_count']}"
            ),
        }
    )
    checks.append(
        {
            "name": "Indexed files match search vectors",
            "ok": not missing_vectors and not orphaned_vectors,
            "details": (
                f"missing_vectors={len(missing_vectors)}, orphaned_vectors={len(orphaned_vectors)}"
            ),
        }
    )
    return checks


def _permission_check(
    name: str,
    path: Path,
    *,
    expected_mode: int,
    missing_ok: bool = False,
    directory: bool = False,
) -> Dict[str, Any]:
    if not path.exists():
        return {
            "name": name,
            "ok": missing_ok,
            "details": "Not present",
        }

    actual_mode = os.stat(path).st_mode & 0o777
    expected_type = path.is_dir() if directory else path.is_file()
    return {
        "name": name,
        "ok": expected_type and actual_mode == expected_mode,
        "details": f"mode={oct(actual_mode)}",
    }


def _finalize_checks(checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    failed = [check for check in checks if not check["ok"]]
    return {
        "ok": not failed,
        "checks": checks,
        "failed_count": len(failed),
        "passed_count": len(checks) - len(failed),
    }
