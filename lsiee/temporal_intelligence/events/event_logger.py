"""Centralized event logging for LSIEE components."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from lsiee.config import get_db_path
from lsiee.security import display_path
from lsiee.storage.schemas import configure_connection, execute_with_retry, initialize_database

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
        sanitized_data = self._sanitize_event_data(data)
        sanitized_tags = self._sanitize_tags(tags or [])
        event_timestamp = float(time.time() if timestamp is None else timestamp)
        created_at = time.time()
        normalized_severity = self._sanitize_severity(severity)
        payload = (
            event_timestamp,
            self._sanitize_identifier(event_type, default="event"),
            self._sanitize_identifier(source, default="component"),
            self._canonical_json(sanitized_data),
            related_process_id,
            related_file_id,
            normalized_severity,
            self._canonical_json(sanitized_tags),
            created_at,
            self._calculate_event_checksum(
                {
                    "timestamp": event_timestamp,
                    "event_type": event_type,
                    "source": source,
                    "data": sanitized_data,
                    "related_process_id": related_process_id,
                    "related_file_id": related_file_id,
                    "severity": normalized_severity,
                    "tags": sanitized_tags,
                }
            ),
        )

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            execute_with_retry(
                conn,
                """
                INSERT INTO events
                (timestamp, event_type, source, data, related_process_id,
                 related_file_id, severity, tags, created_at, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
                commit=True,
                db_path=self.db_path,
            )

        logger.debug("Logged event %s from %s", event_type, source)

    def log_events(self, events: Iterable[Dict[str, Any]]) -> int:
        """Persist multiple events in a single transaction."""
        rows = []
        for event in events:
            sanitized_data = self._sanitize_event_data(event.get("data", {}))
            sanitized_tags = self._sanitize_tags(event.get("tags", []))
            event_timestamp = float(event.get("timestamp", time.time()))
            normalized_severity = self._sanitize_severity(event.get("severity", "INFO"))
            rows.append(
                (
                    event_timestamp,
                    self._sanitize_identifier(event["event_type"], default="event"),
                    self._sanitize_identifier(event["source"], default="component"),
                    self._canonical_json(sanitized_data),
                    event.get("related_process_id"),
                    event.get("related_file_id"),
                    normalized_severity,
                    self._canonical_json(sanitized_tags),
                    time.time(),
                    self._calculate_event_checksum(
                        {
                            "timestamp": event_timestamp,
                            "event_type": event["event_type"],
                            "source": event["source"],
                            "data": sanitized_data,
                            "related_process_id": event.get("related_process_id"),
                            "related_file_id": event.get("related_file_id"),
                            "severity": normalized_severity,
                            "tags": sanitized_tags,
                        }
                    ),
                )
            )

        if not rows:
            return 0

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            execute_with_retry(
                conn,
                """
                INSERT INTO events
                (timestamp, event_type, source, data, related_process_id,
                 related_file_id, severity, tags, created_at, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
                many=True,
                commit=True,
                db_path=self.db_path,
            )

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
        verify_integrity: bool = True,
    ) -> List[Dict[str, Any]]:
        """Query and deserialize matching events."""
        query = ["""
            SELECT id, timestamp, event_type, source, data, related_process_id,
                   related_file_id, severity, tags, created_at, checksum
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

        if verify_integrity:
            rows = [row for row in rows if row["integrity_valid"]]

        if tags:
            required_tags = set(tags)
            rows = [row for row in rows if required_tags.issubset(set(row["tags"]))]

        return rows

    @classmethod
    def _deserialize_row(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert serialized event columns back into Python values."""
        parsed = {
            **row,
            "data": json.loads(row["data"]) if row.get("data") else {},
            "tags": json.loads(row["tags"]) if row.get("tags") else [],
        }
        parsed["integrity_valid"] = cls.verify_row_integrity(parsed)
        return parsed

    @classmethod
    def verify_row_integrity(cls, row: Dict[str, Any]) -> bool:
        """Verify an event row has not been modified after creation."""
        checksum = row.get("checksum")
        if not checksum:
            return False
        return checksum == cls._calculate_event_checksum(row)

    @staticmethod
    def _sanitize_identifier(value: Any, *, default: str) -> str:
        normalized = "".join(
            ch if str(ch).isalnum() or ch in {"_", "-", "."} else "_"
            for ch in str(value or default)
        )
        normalized = normalized.strip("_")[:64]
        return normalized or default

    @staticmethod
    def _sanitize_severity(value: Any) -> str:
        normalized = str(value or "INFO").upper()
        return (
            normalized
            if normalized in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            else "INFO"
        )

    @classmethod
    def _sanitize_event_data(cls, value: Any, *, key: Optional[str] = None) -> Any:
        sensitive_keys = {
            "password",
            "token",
            "secret",
            "api_key",
            "authorization",
            "cookie",
            "session",
            "cmdline",
            "command",
            "argv",
        }
        if key and key.lower() in sensitive_keys:
            return "[REDACTED]"
        if isinstance(value, dict):
            return {
                cls._sanitize_identifier(dict_key, default="field"): cls._sanitize_event_data(
                    dict_value,
                    key=str(dict_key),
                )
                for dict_key, dict_value in value.items()
            }
        if isinstance(value, list):
            return [cls._sanitize_event_data(item, key=key) for item in value[:32]]
        if isinstance(value, (int, float, bool)) or value is None:
            return value

        text = " ".join(str(value).split())
        if key and any(token in key.lower() for token in {"path", "file", "directory"}):
            try:
                text = display_path(text)
            except Exception:
                pass
        elif text.startswith("/") or text.startswith("~/"):
            try:
                text = display_path(text)
            except Exception:
                pass
        return text[:512]

    @staticmethod
    def _sanitize_tags(tags: List[str]) -> List[str]:
        cleaned = []
        for tag in tags[:16]:
            normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(tag))
            normalized = normalized.strip("_")[:64]
            if normalized:
                cleaned.append(normalized)
        return cleaned

    @staticmethod
    def _canonical_json(value: Any) -> str:
        """Serialize JSON consistently for event storage and checksums."""
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _calculate_event_checksum(cls, row: Dict[str, Any]) -> str:
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
