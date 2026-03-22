"""End-to-end workflow verification for the complete LSIEE stack."""

from __future__ import annotations

import sqlite3
import time

from lsiee.config import config
from lsiee.file_intelligence.data_extraction.parsers import StructuredDataParser
from lsiee.file_intelligence.data_extraction.query_executor import QueryExecutor
from lsiee.file_intelligence.indexing.embedding_indexer import EmbeddingIndexer
from lsiee.file_intelligence.indexing.indexer import Indexer
from lsiee.file_intelligence.search.semantic_search import SemanticSearch
from lsiee.storage.schemas import initialize_database
from lsiee.system_observability.detection import AlertManager, AnomalyDetector
from lsiee.system_observability.monitoring import ProcessMonitor, SystemMetrics
from lsiee.temporal_intelligence.correlation import EventCorrelator
from lsiee.temporal_intelligence.events import EventLogger
from lsiee.temporal_intelligence.explanation import RootCauseAnalyzer


def test_complete_workflow(tmp_path, monkeypatch):
    """Exercise the full file, observability, and temporal workflow."""
    db_path = tmp_path / "lsiee.db"
    vector_db_path = tmp_path / "vectors"
    config_dir = tmp_path / "config"

    monkeypatch.setenv("LSIEE_DB_PATH", str(db_path))
    monkeypatch.setenv("LSIEE_VECTOR_DB_PATH", str(vector_db_path))
    monkeypatch.setenv("LSIEE_CONFIG_DIR", str(config_dir))
    config._config = config._default_config()

    initialize_database(db_path).disconnect()

    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    text_file = corpus_dir / "release_notes.txt"
    csv_file = corpus_dir / "sales.csv"

    text_file.write_text(
        "release checklist bug fixes deployment verification and rollout notes",
        encoding="utf-8",
    )
    csv_file.write_text(
        "region,revenue\nwest,200\neast,100\nnorth,150\n",
        encoding="utf-8",
    )

    indexer = Indexer(db_path=db_path)
    stats = indexer.index_directory(corpus_dir, show_progress=False)
    assert stats["files_indexed"] == 2

    indexed = EmbeddingIndexer(db_path=db_path, vector_db_path=vector_db_path).index_all_pending()
    assert indexed == 2

    searcher = SemanticSearch(db_path=db_path, vector_db_path=vector_db_path)
    results = searcher.search("bug fixes", max_results=5)
    assert results
    assert results[0]["metadata"]["filename"] == "release_notes.txt"

    parser = StructuredDataParser()
    data = parser.parse_csv(csv_file)
    assert data["row_count"] == 3

    executor = QueryExecutor()
    result = executor.execute_query(csv_file, "sum of revenue")
    assert result["result"] == 450.0

    snapshot = ProcessMonitor().capture_snapshot()
    assert snapshot

    system_metrics = SystemMetrics().get_all_metrics()
    assert "cpu" in system_metrics
    assert "memory" in system_metrics

    detector = AnomalyDetector(min_samples=20)
    training_rows = [
        {
            "timestamp": float(index),
            "pid": 5000 + index,
            "name": "python",
            "cpu_percent": 10.0 + (index % 4),
            "memory_mb": 150.0 + index,
            "memory_percent": 5.0 + (index % 3) * 0.4,
            "num_threads": 4 + (index % 2),
            "io_read_bytes": 1_000 + index * 50,
            "io_write_bytes": 900 + index * 40,
        }
        for index in range(30)
    ]
    detector.fit(training_rows)
    prediction = detector.predict(
        {
            "timestamp": time.time(),
            "pid": 9999,
            "name": "stress-ng",
            "cpu_percent": 99.0,
            "memory_mb": 4096.0,
            "memory_percent": 92.0,
            "num_threads": 128,
            "io_read_bytes": 8_000_000,
            "io_write_bytes": 9_000_000,
        }
    )
    assert "is_anomaly" in prediction

    alert_manager = AlertManager(db_path=db_path)
    alerts = alert_manager.check_thresholds(
        {
            "pid": 9999,
            "name": "stress-ng",
            "cpu_percent": 99.0,
            "memory_percent": 92.0,
        },
        prediction=prediction,
    )
    alert_manager.log_alerts(alerts)
    recent_alerts = alert_manager.get_recent_alerts(hours=1, limit=10)
    assert recent_alerts

    issue_time = time.time()
    event_logger = EventLogger(db_path=db_path)
    event_logger.log_events(
        [
            {
                "timestamp": issue_time - 10,
                "event_type": "index_completed",
                "source": "indexer",
                "severity": "INFO",
                "data": {"files_indexed": 2},
                "tags": ["file_intelligence"],
            },
            {
                "timestamp": issue_time - 8,
                "event_type": "cpu_high",
                "source": "monitor",
                "severity": "WARNING",
                "data": {"process": "backup.exe"},
                "tags": ["system_observability"],
            },
            {
                "timestamp": issue_time - 4,
                "event_type": "index_completed",
                "source": "indexer",
                "severity": "INFO",
                "data": {"files_indexed": 1},
                "tags": ["file_intelligence"],
            },
            {
                "timestamp": issue_time - 2,
                "event_type": "cpu_high",
                "source": "monitor",
                "severity": "WARNING",
                "data": {"process": "backup.exe"},
                "tags": ["system_observability"],
            },
            {
                "timestamp": issue_time - 1,
                "event_type": "manual_check",
                "source": "operator",
                "severity": "INFO",
                "data": {"status": "started"},
                "tags": ["temporal_intelligence"],
            },
        ]
    )

    correlator = EventCorrelator(db_path=db_path)
    correlations = correlator.find_correlations(time_window=15.0, min_occurrences=2)
    assert isinstance(correlations, list)
    assert correlations
    assert correlator.store_correlations(correlations) >= 1

    rows = [
        (
            issue_time - 120,
            8800,
            "backup.exe",
            "/usr/bin/backup.exe",
            "backup.exe --sync",
            75.0,
            1500.0,
            70.0,
            1000,
            900,
            "running",
            12,
            issue_time - 3600,
            1,
        ),
        (
            issue_time - 5,
            8800,
            "backup.exe",
            "/usr/bin/backup.exe",
            "backup.exe --sync",
            91.0,
            1900.0,
            86.0,
            1100,
            950,
            "running",
            16,
            issue_time - 3600,
            1,
        ),
        (
            issue_time - 1,
            8800,
            "backup.exe",
            "/usr/bin/backup.exe",
            "backup.exe --sync",
            96.0,
            2100.0,
            89.0,
            1200,
            1000,
            "running",
            18,
            issue_time - 3600,
            1,
        ),
    ]
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO process_snapshots
            (timestamp, pid, name, exe_path, cmdline, cpu_percent, memory_mb,
             memory_percent, io_read_bytes, io_write_bytes, status, num_threads,
             create_time, parent_pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()

    analyzer = RootCauseAnalyzer(db_path=db_path)
    explanation = analyzer.explain_slowdown(issue_time)
    assert explanation["root_causes"]
    assert explanation["recommendations"]
