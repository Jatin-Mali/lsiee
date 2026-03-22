"""Tests for temporal event logging and correlation analysis."""

from __future__ import annotations

from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.storage.schemas import initialize_database
from lsiee.temporal_intelligence.correlation import EventCorrelator, PatternDetector
from lsiee.temporal_intelligence.events import EventLogger


def test_event_logger_logs_and_filters_events(tmp_path):
    """Events should persist and be filterable by type, source, and tags."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    logger = EventLogger(db_path)
    logger.log_event(
        event_type="index_started",
        source="file_indexer",
        data={"directory": "/tmp/corpus"},
        tags=["file_intelligence", "indexing"],
        timestamp=100.0,
    )
    logger.log_event(
        event_type="anomaly_detected",
        source="anomaly_detector",
        data={"process_name": "stress-ng"},
        severity="WARNING",
        tags=["system_observability", "anomaly_detection"],
        timestamp=120.0,
    )

    rows = logger.get_events(source="file_indexer", tags=["indexing"], limit=10)

    assert len(rows) == 1
    assert rows[0]["event_type"] == "index_started"
    assert rows[0]["data"]["directory"] == "/tmp/corpus"


def test_indexer_emits_index_lifecycle_events(tmp_path):
    """Indexing should log start and completion events into the temporal store."""
    db_path = tmp_path / "lsiee.db"
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    (corpus_dir / "alpha.txt").write_text("alpha", encoding="utf-8")

    schema = initialize_database(db_path)
    schema.disconnect()

    stats = Indexer(db_path=db_path).index_directory(corpus_dir, show_progress=False)
    rows = EventLogger(db_path).get_events(source="file_indexer", limit=10)

    assert stats["files_indexed"] == 1
    event_types = [row["event_type"] for row in rows]
    assert "index_started" in event_types
    assert "index_completed" in event_types
    completed = next(row for row in rows if row["event_type"] == "index_completed")
    assert completed["data"]["stats"]["files_indexed"] == 1


def test_correlator_discovers_and_stores_event_pairs(tmp_path):
    """Repeated neighboring events should appear as a stored correlation."""
    db_path = tmp_path / "lsiee.db"
    schema = initialize_database(db_path)
    schema.disconnect()

    logger = EventLogger(db_path)
    events = []
    for index in range(6):
        base = float(index * 100)
        events.extend(
            [
                {
                    "timestamp": base,
                    "event_type": "index_started",
                    "source": "file_indexer",
                    "data": {"run": index},
                },
                {
                    "timestamp": base + 5,
                    "event_type": "index_completed",
                    "source": "file_indexer",
                    "data": {"run": index},
                },
            ]
        )
    logger.log_events(events)

    correlator = EventCorrelator(db_path)
    correlations = correlator.find_correlations(
        time_window=10.0, min_support=0.1, min_occurrences=3
    )
    stored_count = correlator.store_correlations(correlations)
    stored = correlator.get_stored_correlations(min_lift=1.0, limit=10)

    assert correlations
    assert stored_count == len(correlations)
    assert any(
        {row["event_type_a"], row["event_type_b"]} == {"index_started", "index_completed"}
        for row in stored
    )


def test_pattern_detector_finds_sequences_periods_bursts_and_cascades():
    """Pattern detection should surface the common event motifs required by Phase 8."""
    detector = PatternDetector()
    events = [
        {"timestamp": 0.0, "event_type": "index_started"},
        {"timestamp": 2.0, "event_type": "index_completed"},
        {"timestamp": 4.0, "event_type": "search_executed"},
        {"timestamp": 20.0, "event_type": "index_started"},
        {"timestamp": 22.0, "event_type": "index_completed"},
        {"timestamp": 24.0, "event_type": "search_executed"},
        {"timestamp": 60.0, "event_type": "heartbeat"},
        {"timestamp": 120.0, "event_type": "heartbeat"},
        {"timestamp": 180.0, "event_type": "heartbeat"},
        {"timestamp": 240.0, "event_type": "heartbeat"},
        {"timestamp": 300.0, "event_type": "burst"},
        {"timestamp": 301.0, "event_type": "burst"},
        {"timestamp": 302.0, "event_type": "burst"},
        {"timestamp": 303.0, "event_type": "burst"},
        {"timestamp": 304.0, "event_type": "burst"},
        {"timestamp": 400.0, "event_type": "trigger"},
        {"timestamp": 405.0, "event_type": "effect_a"},
        {"timestamp": 410.0, "event_type": "effect_b"},
        {"timestamp": 500.0, "event_type": "trigger"},
        {"timestamp": 505.0, "event_type": "effect_a"},
        {"timestamp": 510.0, "event_type": "effect_b"},
    ]

    patterns = detector.detect_patterns(events)

    assert any(
        item["sequence"] == ["index_started", "index_completed", "search_executed"]
        for item in patterns["sequences"]
    )
    assert any(item["event_type"] == "heartbeat" for item in patterns["periodic_events"])
    assert any(item["event_count"] >= 5 for item in patterns["bursts"])
    assert any(item["source_event"] == "trigger" for item in patterns["cascades"])
