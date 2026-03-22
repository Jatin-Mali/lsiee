"""Alerting helpers for anomaly detection."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from lsiee.config import config, get_db_path
from lsiee.storage.schemas import configure_connection, initialize_database
from lsiee.temporal_intelligence.events import EventLogger


class AlertManager:
    """Manage anomaly and threshold-based alerts."""

    def __init__(
        self, db_path: Optional[Path] = None, thresholds: Optional[Dict[str, float]] = None
    ):
        """Initialize alert manager state."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        configured_thresholds = {
            "cpu": float(config.get("anomaly_detection.cpu_threshold", 80.0)),
            "memory": float(config.get("anomaly_detection.memory_threshold", 80.0)),
            "anomaly_score": float(config.get("anomaly_detection.anomaly_score_threshold", -0.5)),
        }
        if thresholds:
            configured_thresholds.update(thresholds)
        self.thresholds = configured_thresholds
        self.alert_history: List[Dict[str, Any]] = []
        schema = initialize_database(self.db_path)
        schema.disconnect()
        self.event_logger = EventLogger(self.db_path)

    def check_thresholds(
        self,
        metrics: Dict[str, Any],
        prediction: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Build alert records for resource thresholds and anomaly predictions."""
        alerts: List[Dict[str, Any]] = []
        process_name = metrics.get("name") or prediction.get("process_name") if prediction else None
        pid = metrics.get("pid") or prediction.get("pid") if prediction else metrics.get("pid")

        cpu_percent = float(metrics.get("cpu_percent", 0.0) or 0.0)
        if cpu_percent > self.thresholds["cpu"]:
            alerts.append(
                {
                    "type": "cpu_high",
                    "source": "anomaly_detector",
                    "severity": "WARNING",
                    "message": f"CPU usage {cpu_percent:.1f}% exceeds threshold",
                    "pid": pid,
                    "process_name": process_name,
                    "cpu_percent": cpu_percent,
                }
            )

        memory_percent = float(metrics.get("memory_percent", 0.0) or 0.0)
        if memory_percent > self.thresholds["memory"]:
            alerts.append(
                {
                    "type": "memory_high",
                    "source": "anomaly_detector",
                    "severity": "WARNING",
                    "message": f"Memory usage {memory_percent:.1f}% exceeds threshold",
                    "pid": pid,
                    "process_name": process_name,
                    "memory_percent": memory_percent,
                }
            )

        if prediction and prediction.get("is_anomaly"):
            anomaly_score = float(prediction.get("anomaly_score", 0.0))
            severity = "ERROR" if anomaly_score <= self.thresholds["anomaly_score"] else "WARNING"
            alerts.append(
                {
                    "type": "anomaly_detected",
                    "source": "anomaly_detector",
                    "severity": severity,
                    "message": self._format_anomaly_message(prediction, anomaly_score),
                    "pid": prediction.get("pid"),
                    "process_name": prediction.get("process_name"),
                    "anomaly_score": anomaly_score,
                }
            )

        self.alert_history.extend(alerts)
        return alerts

    @staticmethod
    def _format_anomaly_message(prediction: Dict[str, Any], anomaly_score: float) -> str:
        """Build a readable anomaly message."""
        process_name = prediction.get("process_name", "<unknown>")
        pid = prediction.get("pid")
        return (
            f"Anomalous behavior detected for {process_name} "
            f"(PID {pid}, score {anomaly_score:.4f})"
        )

    def log_alert(self, alert: Dict[str, Any]):
        """Persist a single alert into the events table."""
        payload = dict(alert)
        event_type = payload.pop("type", "anomaly_alert")
        source = payload.pop("source", "anomaly_detector")
        severity = str(payload.pop("severity", "INFO")).upper()
        timestamp = float(payload.pop("timestamp", time.time()))
        self.event_logger.log_event(
            event_type=event_type,
            source=source,
            data=payload,
            severity=severity,
            tags=["system_observability", "anomaly_detection"],
            related_process_id=payload.get("pid"),
            timestamp=timestamp,
        )

    def log_alerts(self, alerts: List[Dict[str, Any]]):
        """Persist multiple alerts into the events table."""
        self.event_logger.log_events(
            [
                {
                    "timestamp": float(alert.get("timestamp", time.time())),
                    "event_type": alert.get("type", "anomaly_alert"),
                    "source": alert.get("source", "anomaly_detector"),
                    "severity": str(alert.get("severity", "INFO")).upper(),
                    "data": {
                        key: value
                        for key, value in alert.items()
                        if key not in {"timestamp", "type", "source", "severity"}
                    },
                    "tags": ["system_observability", "anomaly_detection"],
                    "related_process_id": alert.get("pid"),
                }
                for alert in alerts
            ]
        )

    def get_recent_alerts(self, hours: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """Return recently logged anomaly alerts."""
        start_time = time.time() - (hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT timestamp, event_type, source, data, severity
                FROM events
                WHERE source = ? AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                ("anomaly_detector", start_time, limit),
            )
            rows = []
            for row in cursor.fetchall():
                payload = json.loads(row["data"])
                rows.append(
                    {
                        "timestamp": row["timestamp"],
                        "event_type": row["event_type"],
                        "source": row["source"],
                        "severity": row["severity"],
                        **payload,
                    }
                )
            return rows
