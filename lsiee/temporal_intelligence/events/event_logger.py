"""Centralized event logging for LSIEE components."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from lsiee.config import get_db_path
from lsiee.storage.schemas import configure_connection, initialize_database

logger = logging.getLogger(__name__)


class EventLogger:
    """Centralized event logging backed by the LSIEE SQLite database."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize the event logger."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        schema = initialize_database(self.db_path)
        schema.disconnect()

    def log_event(
        self,
        event_type: str,
        source: str,
        data: Dict[str, Any],
        severity: str = "INFO",
        tags: Optional[List[str]] = None,
        related_process_id: Optional[int] = None,
        related_file_id: Optional[int] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Persist a single event."""
        payload = (
            float(time.time() if timestamp is None else timestamp),
            event_type,
            source,
            json.dumps(data),
            related_process_id,
            related_file_id,
            severity.upper(),
            json.dumps(tags or []),
        )

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            conn.execute(
                """
                INSERT INTO events
                (timestamp, event_type, source, data, related_process_id,
                 related_file_id, severity, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()

        logger.debug("Logged event %s from %s", event_type, source)

    def log_events(self, events: Iterable[Dict[str, Any]]) -> int:
        """Persist multiple events in a single transaction."""
        rows = []
        for event in events:
            rows.append(
                (
                    float(event.get("timestamp", time.time())),
                    event["event_type"],
                    event["source"],
                    json.dumps(event.get("data", {})),
                    event.get("related_process_id"),
                    event.get("related_file_id"),
                    str(event.get("severity", "INFO")).upper(),
                    json.dumps(event.get("tags", [])),
                )
            )

        if not rows:
            return 0

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            conn.executemany(
                """
                INSERT INTO events
                (timestamp, event_type, source, data, related_process_id,
                 related_file_id, severity, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

        logger.debug("Logged %s events", len(rows))
        return len(rows)

    def get_events(
        self,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        severity: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query and deserialize matching events."""
        query = ["""
            SELECT timestamp, event_type, source, data, related_process_id,
                   related_file_id, severity, tags
            FROM events
            WHERE 1=1
            """]
        params: List[Any] = []

        if event_type:
            query.append("AND event_type = ?")
            params.append(event_type)
        if source:
            query.append("AND source = ?")
            params.append(source)
        if start_time is not None:
            query.append("AND timestamp >= ?")
            params.append(float(start_time))
        if end_time is not None:
            query.append("AND timestamp <= ?")
            params.append(float(end_time))
        if severity:
            query.append("AND severity = ?")
            params.append(severity.upper())

        query.append("ORDER BY timestamp DESC LIMIT ?")
        params.append(int(limit))

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(" ".join(query), params)
            rows = [self._deserialize_row(dict(row)) for row in cursor.fetchall()]

        if tags:
            required_tags = set(tags)
            rows = [row for row in rows if required_tags.issubset(set(row["tags"]))]

        return rows

    @staticmethod
    def _deserialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert serialized event columns back into Python values."""
        return {
            **row,
            "data": json.loads(row["data"]) if row.get("data") else {},
            "tags": json.loads(row["tags"]) if row.get("tags") else [],
        }
