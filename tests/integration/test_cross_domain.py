"""Cross-domain integration tests for LSIEE."""

from __future__ import annotations

import sqlite3
import time

from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.detection import AlertManager, AnomalyDetector
from lsiee.system_observability.monitoring import MonitoringDaemon, ProcessHistory


class StubMonitor:
    """Return lightweight deterministic process snapshots for integration tests."""

    def __init__(self):
        self._counter = 0

    def capture_snapshot(self):
        self._counter += 1
        now = time.time()
        return [
            {
                "timestamp": now,
                "pid": 5000 + self._counter,
                "name": "python",
                "exe_path": "/usr/bin/python",
                "cmdline": "python worker.py",
                "cpu_percent": 10.0 + self._counter,
                "memory_mb": 128.0 + self._counter,
                "memory_percent": 3.0 + (self._counter * 0.1),
                "io_read_bytes": 1000 + self._counter * 10,
                "io_write_bytes": 900 + self._counter * 10,
                "status": "running",
                "num_threads": 4,
                "create_time": now - 60,
                "parent_pid": 1,
            }
        ]


class TrainingMonitor:
    """Return a larger synthetic history for anomaly training."""

    def capture_snapshot(self):
        now = time.time()
        return [
            {
                "timestamp": now - (30 - index),
                "pid": 7000 + index,
                "name": "indexer",
                "exe_path": "/usr/bin/python",
                "cmdline": "python indexer.py",
                "cpu_percent": 12.0 + (index % 4),
                "memory_mb": 200.0 + index,
                "memory_percent": 5.0 + (index * 0.1),
                "io_read_bytes": 2000 + index * 20,
                "io_write_bytes": 1500 + index * 18,
                "status": "running",
                "num_threads": 4 + (index % 2),
                "create_time": now - 3600,
                "parent_pid": 1,
            }
            for index in range(30)
        ]


def test_index_and_monitor_work_together(tmp_path):
    """Monitoring should continue storing snapshots while indexing runs."""
    db_path = tmp_path / "lsiee.db"
    initialize_database(db_path).disconnect()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    for index in range(5):
        (corpus_dir / f"file{index}.txt").write_text(f"document {index}", encoding="utf-8")

    daemon = MonitoringDaemon(db_path=db_path, interval=0.01, monitor=StubMonitor())
    daemon.start()
    try:
        stats = Indexer(db_path=db_path).index_directory(corpus_dir, show_progress=False)
        time.sleep(0.05)
    finally:
        daemon.stop()

    with sqlite3.connect(db_path) as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM process_snapshots").fetchone()[0]

    assert stats["files_indexed"] == 5
    assert snapshot_count > 0


def test_search_while_monitoring_works(tmp_path):
    """Semantic search should work while monitoring writes to the same database."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    initialize_database(db_path).disconnect()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    (corpus_dir / "budget.txt").write_text(
        "quarterly budget forecast and revenue planning",
        encoding="utf-8",
    )
    (corpus_dir / "notes.txt").write_text("gardening checklist and soil notes", encoding="utf-8")

    Indexer(db_path=db_path).index_directory(corpus_dir, show_progress=False)
    assert EmbeddingIndexer(db_path=db_path, vector_db_path=vector_db_path).index_all_pending() == 2

    daemon = MonitoringDaemon(db_path=db_path, interval=0.01, monitor=StubMonitor())
    daemon.start()
    try:
        results = SemanticSearch(db_path=db_path, vector_db_path=vector_db_path).search(
            "budget revenue",
            max_results=3,
        )
        time.sleep(0.05)
    finally:
        daemon.stop()

    with sqlite3.connect(db_path) as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM process_snapshots").fetchone()[0]

    assert results
    assert results[0]["metadata"]["filename"] == "budget.txt"
    assert snapshot_count > 0


def test_end_to_end_workflow_across_domains(tmp_path):
    """Indexing, search, monitoring, anomaly detection, and alerting should interoperate."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    initialize_database(db_path).disconnect()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    (corpus_dir / "release_notes.txt").write_text(
        "release checklist bug fixes and deployment verification",
        encoding="utf-8",
    )
    (corpus_dir / "sales.csv").write_text("region,amount\nwest,200\neast,100\n", encoding="utf-8")

    index_stats = Indexer(db_path=db_path).index_directory(corpus_dir, show_progress=False)
    indexed = EmbeddingIndexer(db_path=db_path, vector_db_path=vector_db_path).index_all_pending()

    MonitoringDaemon(
        db_path=db_path,
        interval=0.01,
        monitor=TrainingMonitor(),
    ).start(blocking=True, iterations=1)

    search_results = SemanticSearch(db_path=db_path, vector_db_path=vector_db_path).search(
        "deployment bug fixes",
        max_results=5,
    )

    history_rows = ProcessHistory(db_path).get_recent_history(hours=1, limit=50)
    detector = AnomalyDetector(min_samples=10)
    detector.fit(history_rows)
    prediction = detector.predict(
        {
            "timestamp": time.time(),
            "pid": 9999,
            "name": "stress-ng",
            "cpu_percent": 99.0,
            "memory_mb": 4096.0,
            "memory_percent": 91.0,
            "num_threads": 128,
            "io_read_bytes": 8_000_000,
            "io_write_bytes": 9_000_000,
        }
    )

    alert_manager = AlertManager(db_path=db_path)
    alerts = alert_manager.check_thresholds(
        {
            "pid": 9999,
            "name": "stress-ng",
            "cpu_percent": 99.0,
            "memory_percent": 91.0,
        },
        prediction=prediction,
    )
    alert_manager.log_alerts(alerts)
    recent_alerts = alert_manager.get_recent_alerts(hours=1, limit=10)

    assert index_stats["files_indexed"] == 2
    assert indexed == 2
    assert search_results
    assert search_results[0]["metadata"]["filename"] == "release_notes.txt"
    assert prediction["is_anomaly"] is True
    assert any(alert["event_type"] == "anomaly_detected" for alert in recent_alerts)
