"""Database schemas for LSIEE."""

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional

SQLITE_RETRY_ATTEMPTS = 5
SQLITE_INITIAL_RETRY_DELAY = 0.05


def configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Apply the SQLite settings used throughout LSIEE."""
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA auto_vacuum = FULL")
    return conn


def tighten_database_permissions(db_path: Path) -> None:
    """Apply restrictive permissions to the SQLite database and sidecars."""
    for candidate in (
        Path(db_path),
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
    ):
        if not candidate.exists():
            continue
        try:
            os.chmod(candidate, 0o600)
        except OSError:
            continue


def _is_transient_sqlite_error(exc: sqlite3.Error) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database schema is locked" in message


def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params=(),
    *,
    many: bool = False,
    commit: bool = False,
    db_path: Optional[Path] = None,
    retry_attempts: int = SQLITE_RETRY_ATTEMPTS,
    initial_delay: float = SQLITE_INITIAL_RETRY_DELAY,
):
    """Execute SQL with bounded retry/backoff for transient SQLite lock errors."""
    delay = initial_delay
    last_exc: Optional[sqlite3.Error] = None

    for attempt in range(retry_attempts):
        try:
            cursor = conn.executemany(sql, params) if many else conn.execute(sql, params)
            if commit:
                conn.commit()
                if db_path is not None:
                    tighten_database_permissions(db_path)
            return cursor
        except sqlite3.Error as exc:
            last_exc = exc
            if not _is_transient_sqlite_error(exc) or attempt == retry_attempts - 1:
                raise
            time.sleep(delay)
            delay *= 2

    if last_exc is not None:  # pragma: no cover - defensive fallback
        raise last_exc
    raise sqlite3.OperationalError("SQLite operation failed without an exception")


class DatabaseSchema:
    """Manages database schema creation and migrations."""

    def __init__(self, db_path: Path):
        """Initialize schema manager.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """Connect to database."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = configure_connection(sqlite3.connect(self.db_path, timeout=30.0))
        tighten_database_permissions(self.db_path)

    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_all_tables(self):
        """Create all database tables."""
        if not self.conn:
            self.connect()

        self._create_files_table()
        self._create_schemas_table()
        self._create_process_snapshots_table()
        self._create_events_table()
        self._create_correlations_table()

        self.conn.commit()

    def _create_files_table(self):
        """Create files metadata table."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL,
                filename TEXT NOT NULL,
                extension TEXT,
                mime_type TEXT,
                size_bytes INTEGER,
                created_at TIMESTAMP,
                modified_at TIMESTAMP,
                accessed_at TIMESTAMP,
                indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT,
                is_structured BOOLEAN DEFAULT FALSE,
                row_count INTEGER,
                column_count INTEGER,
                index_status TEXT DEFAULT 'pending',
                index_error TEXT,
                CHECK (index_status IN ('pending', 'indexed', 'failed', 'skipped'))
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_modified ON files(modified_at)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension)")

    def _create_schemas_table(self):
        """Create table for structured file schemas."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS schemas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                sheet_name TEXT,
                column_name TEXT NOT NULL,
                column_type TEXT,
                column_index INTEGER,
                sample_values TEXT,
                min_value REAL,
                max_value REAL,
                unique_count INTEGER,
                null_count INTEGER,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_schemas_file ON schemas(file_id)")

    def _create_process_snapshots_table(self):
        """Create table for process monitoring snapshots."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS process_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                pid INTEGER NOT NULL,
                name TEXT NOT NULL,
                exe_path TEXT,
                cmdline TEXT,
                cpu_percent REAL,
                memory_mb REAL,
                memory_percent REAL,
                io_read_bytes INTEGER,
                io_write_bytes INTEGER,
                status TEXT,
                num_threads INTEGER,
                create_time REAL,
                parent_pid INTEGER
            )
        """)

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON process_snapshots(timestamp)"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_pid ON process_snapshots(pid)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_name ON process_snapshots(name)"
        )

    def _create_events_table(self):
        """Create table for temporal event logging."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                data TEXT NOT NULL,
                related_process_id INTEGER,
                related_file_id INTEGER,
                severity TEXT DEFAULT 'INFO',
                tags TEXT,
                created_at REAL DEFAULT (unixepoch()),
                checksum TEXT,
                CHECK (severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source)")
        self._ensure_events_integrity_columns()

    def _create_correlations_table(self):
        """Create table for discovered correlations."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS correlations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type_a TEXT NOT NULL,
                event_type_b TEXT NOT NULL,
                support REAL,
                confidence REAL,
                lift REAL,
                occurrences INTEGER,
                first_seen REAL,
                last_seen REAL,
                avg_delay_seconds REAL
            )
        """)
        self.conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_correlations_pair
            ON correlations(event_type_a, event_type_b)
            """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_correlations_lift ON correlations(lift)")

    def _ensure_events_integrity_columns(self):
        """Backfill integrity columns for event logs in older databases."""
        columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(events)").fetchall()}

        if "created_at" not in columns:
            self.conn.execute("ALTER TABLE events ADD COLUMN created_at REAL")
        if "checksum" not in columns:
            self.conn.execute("ALTER TABLE events ADD COLUMN checksum TEXT")

        stale_rows = self.conn.execute("""
            SELECT id, timestamp, event_type, source, data, related_process_id,
                   related_file_id, severity, tags, created_at, checksum
            FROM events
            WHERE created_at IS NULL OR checksum IS NULL
            """).fetchall()

        for row in stale_rows:
            event = dict(row)
            created_at = float(event.get("created_at") or event["timestamp"] or time.time())
            checksum = self._calculate_event_checksum(event)
            self.conn.execute(
                "UPDATE events SET created_at = ?, checksum = ? WHERE id = ?",
                (created_at, checksum, event["id"]),
            )

    @staticmethod
    def _canonical_json(value) -> str:
        """Serialize event payloads consistently for integrity hashing."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _calculate_event_checksum(cls, row: dict) -> str:
        """Calculate a stable checksum for an event row."""
        payload = {
            "timestamp": float(row["timestamp"]),
            "event_type": str(row["event_type"]),
            "source": str(row["source"]),
            "data": cls._canonical_json(row.get("data") or {}),
            "related_process_id": row.get("related_process_id"),
            "related_file_id": row.get("related_file_id"),
            "severity": str(row.get("severity") or "INFO").upper(),
            "tags": cls._canonical_json(row.get("tags") or []),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def initialize_database(db_path: Path) -> DatabaseSchema:
    """Initialize database with all tables.

    Args:
        db_path: Path to database file

    Returns:
        DatabaseSchema instance
    """
    schema = DatabaseSchema(db_path)
    schema.connect()
    schema.create_all_tables()
    return schema
