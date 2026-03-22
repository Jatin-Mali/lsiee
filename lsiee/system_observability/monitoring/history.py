"""Historical process monitoring queries."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lsiee.storage.schemas import configure_connection


class ProcessHistory:
    """Query historical process snapshot data."""

    def __init__(self, db_path: Path):
        """Initialize the history helper."""
        self.db_path = Path(db_path)

    def get_process_history(
        self,
        pid: int,
        start_time: float,
        end_time: float,
    ) -> List[Dict[str, Any]]:
        """Return historical rows for a process in the provided time range."""
        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT * FROM process_snapshots
                WHERE pid = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
                """,
                (pid, start_time, end_time),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_cpu_timeline(self, process_name: str, hours: int = 24) -> List[Tuple[float, float]]:
        """Return a CPU timeline for matching process names."""
        end_time = datetime.now().timestamp()
        start_time = (datetime.now() - timedelta(hours=hours)).timestamp()

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT timestamp, cpu_percent
                FROM process_snapshots
                WHERE name LIKE ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
                """,
                (f"%{process_name}%", start_time, end_time),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_recent_history(self, hours: int = 1, limit: int = 20) -> List[Dict[str, Any]]:
        """Return recent snapshots for quick inspection."""
        start_time = (datetime.now() - timedelta(hours=hours)).timestamp()

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT *
                FROM process_snapshots
                WHERE timestamp >= ?
                ORDER BY timestamp DESC, cpu_percent DESC
                LIMIT ?
                """,
                (start_time, limit),
            )
            return [dict(row) for row in cursor.fetchall()]
