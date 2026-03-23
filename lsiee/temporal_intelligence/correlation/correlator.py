"""Event correlation discovery for temporal intelligence."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lsiee.config import get_db_path
from lsiee.storage.schemas import configure_connection, execute_with_retry, initialize_database
from lsiee.temporal_intelligence.events import EventLogger

logger = logging.getLogger(__name__)


class EventCorrelator:
    """Discover and persist relationships between event types."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize the correlator."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        schema = initialize_database(self.db_path)
        schema.disconnect()
        self.event_logger = EventLogger(self.db_path)

    def find_correlations(
        self,
        time_window: float = 60.0,
        min_support: float = 0.01,
        min_occurrences: int = 2,
    ) -> List[Dict[str, Any]]:
        """Find co-occurring event-type pairs within the provided time window."""
        events = self._get_events()
        if len(events) < 2:
            return []

        cooccurrences: Dict[Tuple[str, str], int] = defaultdict(int)
        event_counts: Dict[str, int] = defaultdict(int)
        delays: Dict[Tuple[str, str], List[float]] = defaultdict(list)

        for index, event_a in enumerate(events):
            event_counts[event_a["event_type"]] += 1

            for event_b in events[index + 1 :]:
                time_diff = float(event_b["timestamp"]) - float(event_a["timestamp"])
                if time_diff > time_window:
                    break

                if event_a["event_type"] == event_b["event_type"]:
                    continue

                pair = tuple(sorted((event_a["event_type"], event_b["event_type"])))
                cooccurrences[pair] += 1
                delays[pair].append(time_diff)

        total_events = len(events)
        correlations = []

        for (type_a, type_b), count in cooccurrences.items():
            if count < min_occurrences:
                continue

            support = count / total_events
            if support < min_support:
                continue

            confidence = count / max(1, min(event_counts[type_a], event_counts[type_b]))
            prob_a = event_counts[type_a] / total_events
            prob_b = event_counts[type_b] / total_events
            expected = prob_a * prob_b * total_events
            lift = count / expected if expected > 0 else 0.0

            correlations.append(
                {
                    "event_type_a": type_a,
                    "event_type_b": type_b,
                    "support": support,
                    "confidence": confidence,
                    "lift": lift,
                    "occurrences": count,
                    "avg_delay_seconds": sum(delays[(type_a, type_b)])
                    / len(delays[(type_a, type_b)]),
                }
            )

        correlations.sort(key=lambda item: (item["lift"], item["occurrences"]), reverse=True)
        logger.info("Found %s event correlations", len(correlations))
        return correlations

    def store_correlations(self, correlations: List[Dict[str, Any]]) -> int:
        """Store or refresh discovered correlations in SQLite."""
        if not correlations:
            return 0

        now = time.time()
        rows = [
            (
                correlation["event_type_a"],
                correlation["event_type_b"],
                correlation["support"],
                correlation["confidence"],
                correlation["lift"],
                correlation["occurrences"],
                now,
                now,
                correlation.get("avg_delay_seconds", 0.0),
            )
            for correlation in correlations
        ]

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            execute_with_retry(
                conn,
                """
                INSERT INTO correlations
                (event_type_a, event_type_b, support, confidence, lift,
                 occurrences, first_seen, last_seen, avg_delay_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_type_a, event_type_b) DO UPDATE SET
                    support = excluded.support,
                    confidence = excluded.confidence,
                    lift = excluded.lift,
                    occurrences = excluded.occurrences,
                    last_seen = excluded.last_seen,
                    avg_delay_seconds = excluded.avg_delay_seconds
                """,
                rows,
                many=True,
                commit=True,
                db_path=self.db_path,
            )

        return len(rows)

    def get_stored_correlations(
        self, min_lift: float = 1.0, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Return persisted correlations ordered by strength."""
        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT event_type_a, event_type_b, support, confidence, lift,
                       occurrences, first_seen, last_seen, avg_delay_seconds
                FROM correlations
                WHERE lift >= ?
                ORDER BY lift DESC, occurrences DESC
                LIMIT ?
                """,
                (min_lift, limit),
            )
            return [dict(row) for row in cursor.fetchall()]

    def _get_events(self) -> List[Dict[str, Any]]:
        """Load all events ordered by time for correlation analysis."""
        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute("""
                SELECT id, timestamp, event_type, source, data, related_process_id,
                       related_file_id, severity, tags, created_at, checksum
                FROM events
                ORDER BY timestamp, id
                """)
            events = []
            for row in cursor.fetchall():
                parsed = self.event_logger._deserialize_row(dict(row))
                if parsed["integrity_valid"]:
                    events.append(parsed)
            return events
