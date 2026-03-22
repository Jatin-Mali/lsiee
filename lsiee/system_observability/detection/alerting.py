"""Alerting helpers for anomaly detection."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional

from lsiee.config import config, get_db_path
from lsiee.storage.schemas import initialize_database


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
                    "message": (
                        f"Anomalous behavior detected for {prediction.get('process_name', '<unknown>')} "
                        f"(PID {prediction.get('pid')}, score {anomaly_score:.4f})"
                    ),
                    "pid": prediction.get("pid"),
                    "process_name": prediction.get("process_name"),
                    "anomaly_score": anomaly_score,
                }
            )

        self.alert_history.extend(alerts)
        return alerts

    def log_alert(self, alert: Dict[str, Any]):
        """Persist a single alert into the events table."""
        payload = dict(alert)
        timestamp = float(payload.pop("timestamp", time.time()))
        source = payload.pop("source", "anomaly_detector")
        severity = str(payload.pop("severity", "INFO")).upper()
        event_type = payload.pop("type", "anomaly_alert")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (timestamp, event_type, source, data, severity)
                VALUES (?, ?, ?, ?, ?)
                """,
                (timestamp, event_type, source, json.dumps(payload), severity),
            )
            conn.commit()

    def log_alerts(self, alerts: List[Dict[str, Any]]):
        """Persist multiple alerts into the events table."""
        for alert in alerts:
            self.log_alert(alert)

    def get_recent_alerts(self, hours: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """Return recently logged anomaly alerts."""
        start_time = time.time() - (hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
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
