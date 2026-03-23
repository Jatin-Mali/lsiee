"""Privacy, export, and cleanup helpers for LSIEE."""

from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from lsiee.security.path_security import (
    PathSecurityError,
    atomic_write_bytes,
    atomic_write_text,
    ensure_safe_directory,
    ensure_safe_file,
    ensure_safe_output_path,
    read_secure_text,
)
from lsiee.storage.schemas import configure_connection

DATA_INVENTORY = [
    {
        "category": "file_metadata",
        "sensitivity": "internal",
        "description": "Indexed file paths, names, extensions, sizes, and timestamps",
        "retention": "Until file deletion or explicit cleanup",
    },
    {
        "category": "search_index",
        "sensitivity": "confidential",
        "description": "Locally stored searchable text snippets and vector metadata",
        "retention": "Until file deletion or explicit cleanup",
    },
    {
        "category": "process_snapshots",
        "sensitivity": "internal",
        "description": "Local process metrics such as names, CPU, memory, and status",
        "retention": "30 days by default",
    },
    {
        "category": "events",
        "sensitivity": "internal",
        "description": "Local event timeline used for alerts, correlation, and explanations",
        "retention": "90 days by default",
    },
]


def export_lsiee_data(
    *,
    db_path: Path,
    vector_db_path: Path,
    config_file: Path,
    output_path: Path,
    format: str = "json",
) -> Dict[str, Any]:
    """Export LSIEE data in JSON or CSV-bundle form."""
    safe_output = ensure_safe_output_path(output_path)
    payload = build_export_payload(
        db_path=db_path,
        vector_db_path=vector_db_path,
        config_file=config_file,
    )

    if format == "json":
        atomic_write_text(safe_output, json.dumps(payload, indent=2, sort_keys=True, default=str))
    elif format == "csv":
        atomic_write_bytes(safe_output, _build_csv_bundle(payload))
    else:
        raise ValueError("Unsupported export format")

    return {
        "output_path": str(safe_output),
        "format": format,
        "counts": payload["counts"],
    }


def build_export_payload(
    *,
    db_path: Path,
    vector_db_path: Path,
    config_file: Path,
) -> Dict[str, Any]:
    """Build the full local-data export payload."""
    tables = {
        "files": _read_table(db_path, "files"),
        "process_snapshots": _read_table(db_path, "process_snapshots"),
        "events": _read_table(db_path, "events"),
        "correlations": _read_table(db_path, "correlations"),
    }
    vector_payload = _read_vector_store(vector_db_path)
    config_payload = _read_config_payload(config_file)

    counts = {
        "files": len(tables["files"]),
        "process_snapshots": len(tables["process_snapshots"]),
        "events": len(tables["events"]),
        "correlations": len(tables["correlations"]),
        "search_documents": len(vector_payload.get("ids", [])),
    }

    return {
        "generated_at": time.time(),
        "data_location": {
            "database": str(db_path),
            "vector_store": str(vector_db_path),
            "config_file": str(config_file),
        },
        "data_inventory": DATA_INVENTORY,
        "counts": counts,
        "config": config_payload,
        "tables": tables,
        "search_index": vector_payload,
    }


def cleanup_lsiee_data(
    *,
    db_path: Path,
    data_type: str = "all",
    older_than_days: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Delete aged LSIEE data for supported retention-managed tables."""
    cfg = _config()
    cutoff_process = time.time() - (
        int(older_than_days or cfg.get("retention.process_snapshots_days", 30)) * 86400
    )
    cutoff_events = time.time() - (
        int(older_than_days or cfg.get("retention.events_days", 90)) * 86400
    )

    plans = []
    if data_type in {"process-snapshots", "all"}:
        plans.append(
            _plan_cleanup(
                db_path=db_path,
                table="process_snapshots",
                timestamp_column="timestamp",
                cutoff=cutoff_process,
                dry_run=dry_run,
            )
        )
    if data_type in {"events", "all"}:
        plans.append(
            _plan_cleanup(
                db_path=db_path,
                table="events",
                timestamp_column="timestamp",
                cutoff=cutoff_events,
                dry_run=dry_run,
            )
        )

    return {
        "dry_run": dry_run,
        "data_type": data_type,
        "plans": plans,
        "deleted_rows": sum(plan["deleted_rows"] for plan in plans),
    }


def purge_lsiee_data(
    *,
    db_path: Path,
    vector_db_path: Path,
    config_file: Path,
    log_dir: Path,
) -> Dict[str, Any]:
    """Delete LSIEE-managed local data files and directories."""
    removed: List[str] = []

    for suffix in ("", "-wal", "-shm"):
        removed.extend(_remove_path(db_path.with_name(f"{db_path.name}{suffix}")))

    removed.extend(_remove_path(db_path.parent / "monitor.pid"))
    removed.extend(_remove_path(vector_db_path))
    removed.extend(_remove_path(config_file))
    removed.extend(_remove_path(log_dir))

    return {"removed": removed}


def apply_event_retention(db_path: Path) -> int:
    """Delete events older than the configured retention period."""
    cutoff = time.time() - (int(_config().get("retention.events_days", 90)) * 86400)
    summary = _plan_cleanup(
        db_path=db_path,
        table="events",
        timestamp_column="timestamp",
        cutoff=cutoff,
        dry_run=False,
    )
    return summary["deleted_rows"]


def _plan_cleanup(
    *,
    db_path: Path,
    table: str,
    timestamp_column: str,
    cutoff: float,
    dry_run: bool,
) -> Dict[str, Any]:
    if not db_path.exists():
        return {
            "table": table,
            "cutoff": cutoff,
            "matched_rows": 0,
            "deleted_rows": 0,
            "oldest_timestamp": None,
            "newest_timestamp": None,
        }

    with sqlite3.connect(db_path) as conn:
        configure_connection(conn)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS count,
                   MIN({timestamp_column}) AS oldest_timestamp,
                   MAX({timestamp_column}) AS newest_timestamp
            FROM {table}
            WHERE {timestamp_column} < ?
            """,
            (cutoff,),
        ).fetchone()

        matched_rows = int(row["count"] or 0)
        deleted_rows = 0
        if matched_rows and not dry_run:
            cursor = conn.execute(f"DELETE FROM {table} WHERE {timestamp_column} < ?", (cutoff,))
            conn.commit()
            deleted_rows = cursor.rowcount if cursor.rowcount != -1 else matched_rows

    return {
        "table": table,
        "cutoff": cutoff,
        "matched_rows": matched_rows,
        "deleted_rows": deleted_rows,
        "oldest_timestamp": row["oldest_timestamp"],
        "newest_timestamp": row["newest_timestamp"],
    }


def _read_table(db_path: Path, table: str) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []

    with sqlite3.connect(db_path) as conn:
        configure_connection(conn)
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return [dict(row) for row in rows]


def _read_vector_store(vector_db_path: Path) -> Dict[str, Any]:
    try:
        from lsiee.storage.vector_db import VectorDB

        vector_db = VectorDB(vector_db_path)
    except Exception:
        return {"ids": [], "documents": [], "metadatas": [], "embeddings": []}

    return {
        "ids": vector_db.ids,
        "documents": vector_db.documents,
        "metadatas": vector_db.metadatas,
        "embeddings": vector_db.embeddings,
        "diagnostics": vector_db.get_diagnostics(),
    }


def _read_config_payload(config_file: Path) -> Dict[str, Any]:
    if not config_file.exists():
        return {}
    try:
        ensure_safe_file(config_file, max_size_bytes=1024 * 1024)
        return yaml.safe_load(read_secure_text(config_file, max_bytes=1024 * 1024)) or {}
    except Exception:
        return {}


def _build_csv_bundle(payload: Dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(payload["data_inventory"], indent=2))
        archive.writestr("counts.json", json.dumps(payload["counts"], indent=2))
        archive.writestr("config.yaml", yaml.safe_dump(payload["config"], sort_keys=True))
        archive.writestr(
            "search_index.csv",
            _rows_to_csv(
                [
                    {
                        "id": id_,
                        "document": document,
                        "metadata": json.dumps(metadata, sort_keys=True),
                    }
                    for id_, document, metadata in zip(
                        payload["search_index"].get("ids", []),
                        payload["search_index"].get("documents", []),
                        payload["search_index"].get("metadatas", []),
                    )
                ]
            ),
        )
        for table_name, rows in payload["tables"].items():
            archive.writestr(f"{table_name}.csv", _rows_to_csv(rows))
    return buffer.getvalue()


def _rows_to_csv(rows: Iterable[Dict[str, Any]]) -> str:
    row_list = list(rows)
    if not row_list:
        return ""

    fieldnames: List[str] = []
    for row in row_list:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in row_list:
        writer.writerow(
            {
                key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            }
        )
    return buffer.getvalue()


def _remove_path(path: Path) -> List[str]:
    candidate = Path(path)
    if not candidate.exists():
        return []

    removed: List[str] = []
    if candidate.is_symlink():
        raise PathSecurityError(f"Refusing to delete symlinked path: {candidate}")

    if candidate.is_dir():
        ensure_safe_directory(candidate)
        for child in list(candidate.iterdir()):
            removed.extend(_remove_path(child))
        candidate.rmdir()
        removed.append(str(candidate))
        return removed

    ensure_safe_file(candidate, max_size_bytes=max(candidate.stat().st_size, 1))
    os.unlink(candidate)
    removed.append(str(candidate))
    return removed


def _config():
    """Load config lazily to avoid module import cycles."""
    from lsiee.config import config

    return config
