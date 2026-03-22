"""Tests for anomaly detection and alerting."""

from __future__ import annotations

import time

from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.detection import (
    AlertManager,
    AnomalyDetector,
    FeatureEngineer,
    RealtimeAnomalyDetector,
)


def test_anomaly_detector_identifies_outlier():
    """A strong outlier should be marked as anomalous after training."""
    normal_data = [
        {
            "timestamp": float(index),
            "pid": 1000 + index,
            "name": "python",
            "cpu_percent": 8 + (index % 4),
            "memory_mb": 120 + (index % 5) * 3,
            "memory_percent": 2 + (index % 3) * 0.3,
            "num_threads": 4 + (index % 2),
            "io_read_bytes": 1000 + index * 10,
            "io_write_bytes": 900 + index * 10,
        }
        for index in range(40)
    ]

    detector = AnomalyDetector(min_samples=20)
    detector.fit(normal_data)

    anomaly = {
        "timestamp": 999.0,
        "pid": 4242,
        "name": "runaway",
        "cpu_percent": 98.0,
        "memory_mb": 4096.0,
        "memory_percent": 92.0,
        "num_threads": 150,
        "io_read_bytes": 5_000_000,
        "io_write_bytes": 8_000_000,
    }

    result = detector.predict(anomaly)

    assert result["is_anomaly"] is True
    assert result["pid"] == 4242


def test_feature_engineer_computes_temporal_features():
    """Temporal features should summarize process history."""
    history = [
        {"timestamp": 1.0, "cpu_percent": 10.0, "memory_mb": 100.0},
        {"timestamp": 2.0, "cpu_percent": 15.0, "memory_mb": 120.0},
        {"timestamp": 3.0, "cpu_percent": 25.0, "memory_mb": 150.0},
    ]

    features = FeatureEngineer().compute_temporal_features(history)

    assert features["cpu_max"] == 25.0
    assert features["cpu_mean"] > 0
    assert features["cpu_trend"] > 0
    assert features["mem_growth_rate"] > 0
    assert features["duration"] == 2.0


def test_realtime_detector_flags_anomaly_after_training():
    """Realtime detector should flag an anomalous live snapshot once trained."""
    detector = RealtimeAnomalyDetector(
        history_window=50,
        retrain_interval=10,
        min_training_samples=10,
    )

    for index in range(15):
        detector.update(
            [
                {
                    "timestamp": float(index),
                    "pid": 111,
                    "name": "python",
                    "cpu_percent": 8.0 + (index % 3),
                    "memory_mb": 100.0 + index,
                    "memory_percent": 2.0 + (index * 0.05),
                    "num_threads": 4,
                    "io_read_bytes": 1000 + index * 5,
                    "io_write_bytes": 1000 + index * 4,
                }
            ]
        )

    anomalies = detector.check_anomalies(
        [
            {
                "timestamp": 20.0,
                "pid": 111,
                "name": "python",
                "cpu_percent": 95.0,
                "memory_mb": 4000.0,
                "memory_percent": 85.0,
                "num_threads": 120,
                "io_read_bytes": 9_000_000,
                "io_write_bytes": 7_000_000,
            }
        ]
    )

    assert anomalies
    assert anomalies[0]["is_anomaly"] is True


def test_alert_manager_logs_and_reads_alerts(tmp_path):
    """Alerts should persist into the events table and be queryable."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    manager = AlertManager(db_path=db_path)
    alerts = manager.check_thresholds(
        {
            "pid": 77,
            "name": "worker",
            "cpu_percent": 91.0,
            "memory_percent": 88.0,
        },
        prediction={
            "is_anomaly": True,
            "anomaly_score": -0.82,
            "process_name": "worker",
            "pid": 77,
        },
    )
    manager.log_alerts(alerts)

    rows = manager.get_recent_alerts(hours=1, limit=10)

    assert rows
    event_types = {row["event_type"] for row in rows}
    assert "anomaly_detected" in event_types
    assert "cpu_high" in event_types
    assert any(row["process_name"] == "worker" for row in rows)
