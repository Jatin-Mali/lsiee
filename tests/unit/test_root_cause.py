"""Tests for root-cause analysis and recommendations."""

from __future__ import annotations

import sqlite3

from lsiee.storage.schemas import initialize_database
from lsiee.temporal_intelligence.correlation import EventCorrelator
from lsiee.temporal_intelligence.events import EventLogger
from lsiee.temporal_intelligence.explanation import RecommendationEngine, RootCauseAnalyzer


def test_root_cause_analyzer_explains_slowdown(tmp_path):
    """A synthetic slowdown scenario should produce evidence and recommendations."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    incident_time = 1_700_000_000.0

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO process_snapshots
            (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
             memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
             create_time, parent_pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    incident_time - 30,
                    4242,
                    "backup.exe",
                    "/usr/bin/backup",
                    "backup --run",
                    96.0,
                    2048.0,
                    82.0,
                    10_000,
                    15_000,
                    "running",
                    24,
                    incident_time - 600,
                    1,
                ),
                (
                    incident_time - 10,
                    4242,
                    "backup.exe",
                    "/usr/bin/backup",
                    "backup --run",
                    92.0,
                    1980.0,
                    80.0,
                    12_000,
                    16_000,
                    "running",
                    24,
                    incident_time - 600,
                    1,
                ),
                (
                    incident_time - 3600,
                    3131,
                    "backup.exe",
                    "/usr/bin/backup",
                    "backup --run",
                    88.0,
                    1500.0,
                    70.0,
                    8_000,
                    9_000,
                    "running",
                    20,
                    incident_time - 4200,
                    1,
                ),
            ],
        )
        conn.commit()

    logger = EventLogger(db_path)
    logger.log_events(
        [
            {
                "timestamp": incident_time - 40,
                "event_type": "index_started",
                "source": "file_indexer",
                "data": {"directory": "/data"},
                "severity": "INFO",
            },
            {
                "timestamp": incident_time - 20,
                "event_type": "anomaly_detected",
                "source": "anomaly_detector",
                "data": {"process_name": "backup.exe", "pid": 4242},
                "severity": "WARNING",
            },
        ]
    )

    correlator = EventCorrelator(db_path)
    correlations = correlator.find_correlations(
        time_window=60.0, min_support=0.1, min_occurrences=1
    )
    correlator.store_correlations(correlations)

    explanation = RootCauseAnalyzer(db_path=db_path).explain_slowdown(incident_time)

    assert explanation["issue"] == "system_slowdown"
    assert explanation["root_causes"]
    assert any("backup.exe" in cause for cause in explanation["root_causes"])
    assert explanation["recommendations"]
    assert "not proven causation" in explanation["disclaimer"].lower()
    evidence_types = {item["type"] for item in explanation["evidence"]}
    assert {"process_metrics", "events", "historical"}.issubset(evidence_types)


def test_recommendation_engine_adds_context_specific_guidance():
    """Recommendations should incorporate process-specific context."""
    recommendations = RecommendationEngine().recommend(
        "cpu_high",
        {
            "process_name": "trainer.py",
            "max_cpu": 95.0,
            "correlated_event_types": ["anomaly_detected", "cpu_high"],
        },
    )

    assert any("trainer.py" in recommendation for recommendation in recommendations)
    assert any("correlated events" in recommendation.lower() for recommendation in recommendations)
